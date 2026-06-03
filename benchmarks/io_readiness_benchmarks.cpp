#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

#include <benchmark/benchmark.h>

#include "af/async_flow.hpp"

#if AF_DETAIL_HAS_EPOLL || AF_DETAIL_HAS_KQUEUE
#include <cerrno>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

constexpr std::size_t readiness_queue_capacity = 65536;

template <typename RuntimeT, typename IoThreadTag>
class ReadinessReadLoopTask final : public RuntimeT::Task {
public:
    using TaskBase = typename RuntimeT::Task;
    using Thread = typename RuntimeT::Thread;

private:
    static constexpr Thread io_thread =
        RuntimeT::template thread_group<IoThreadTag>().template at<0>();

public:
    explicit ReadinessReadLoopTask(typename TaskBase::FactoryToken token) : TaskBase(token) {}

    bool do_it(int fd, int expected_reads, std::atomic<int> *completed, std::atomic<int> *reads,
               std::atomic<int> *failures) {
        fd_ = fd;
        expected_reads_ = expected_reads;
        completed_ = completed;
        reads_ = reads;
        failures_ = failures;
        return this->schedule(io_thread);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            return arm_read();

        case State::Read:
            return finish_read();
        }
        return complete_failure();
    }

    af::TaskResult arm_read() {
        state_ = State::Read;
        const af::IoStatus status =
            af::io_read_some(*this, io_thread, fd_, &value_, sizeof(value_), read_);
        return handle_status(status);
    }

    af::TaskResult finish_read() {
        const af::IoStatus status =
            af::io_read_some(*this, io_thread, fd_, &value_, sizeof(value_), read_);
        return handle_status(status);
    }

    af::TaskResult handle_status(af::IoStatus status) {
        if (status.pending()) {
            return this->pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return complete_failure();
        }

        ++read_count_;
        reads_->store(read_count_, std::memory_order_release);
        if (read_count_ == expected_reads_) {
            return complete_success();
        }

        state_ = State::Arm;
        return arm_read();
    }

    af::TaskResult complete_success() {
        completed_->store(1, std::memory_order_release);
        completed_->notify_one();
        return this->done();
    }

    af::TaskResult complete_failure() {
        failures_->fetch_add(1, std::memory_order_relaxed);
        completed_->store(1, std::memory_order_release);
        completed_->notify_one();
        return this->done();
    }

    State state_{State::Arm};
    int fd_{-1};
    int expected_reads_{0};
    int read_count_{0};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *reads_{nullptr};
    std::atomic<int> *failures_{nullptr};
};

struct SocketPair {
    af::UniqueFd reader{};
    af::UniqueFd writer{};

    [[nodiscard]] bool create() noexcept {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
            return false;
        }
        if (!set_nonblocking_cloexec(fds[0]) || !set_nonblocking_cloexec(fds[1])) {
            if (fds[0] >= 0) {
                ::close(fds[0]);
            }
            if (fds[1] >= 0) {
                ::close(fds[1]);
            }
            return false;
        }
        reader.reset(fds[0]);
        writer.reset(fds[1]);
        return true;
    }

private:
    [[nodiscard]] static bool set_nonblocking_cloexec(int fd) noexcept {
        const int flags = ::fcntl(fd, F_GETFL, 0);
        if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
            return false;
        }
        const int fd_flags = ::fcntl(fd, F_GETFD, 0);
        return fd_flags >= 0 && ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) == 0;
    }
};

[[nodiscard]] bool write_bytes_until(int fd, int byte_count) {
    int written_total = 0;
    char buffer[256]{};
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (written_total < byte_count) {
        const int remaining = byte_count - written_total;
        const std::size_t chunk = static_cast<std::size_t>(
            remaining < static_cast<int>(sizeof(buffer)) ? remaining
                                                         : static_cast<int>(sizeof(buffer)));
        const ssize_t written = ::write(fd, buffer, chunk);
        if (written > 0) {
            written_total += static_cast<int>(written);
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) &&
            std::chrono::steady_clock::now() <= deadline) {
            std::this_thread::yield();
            continue;
        }
        return false;
    }
    return true;
}

[[nodiscard]] bool wait_completed(std::atomic<int> &completed) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (completed.load(std::memory_order_acquire) == 0) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

template <typename RuntimeT, typename IoThreadTag, typename TaskT>
void run_readiness_rearm_benchmark(benchmark::State &state, const char *backend_name) {
    const int read_count = static_cast<int>(state.range(0));
    SocketPair sockets{};
    if (!sockets.create()) {
        state.SkipWithError("socketpair failed");
        return;
    }

    RuntimeT::init();
    constexpr auto io_thread = RuntimeT::template thread_group<IoThreadTag>().template at<0>();
    if (!RuntimeT::io_backend_available(io_thread)) {
        RuntimeT::shutdown();
        state.SkipWithError(backend_name);
        return;
    }

    bool failed = false;
    for (auto _ : state) {
        std::atomic<int> completed{0};
        std::atomic<int> reads{0};
        std::atomic<int> failures{0};
        if (!RuntimeT::template start_task<TaskT>(sockets.reader.get(), read_count, &completed,
                                                  &reads, &failures)) {
            state.SkipWithError("readiness read task did not start");
            failed = true;
            break;
        }
        if (!write_bytes_until(sockets.writer.get(), read_count) || !wait_completed(completed) ||
            failures.load(std::memory_order_acquire) != 0 ||
            reads.load(std::memory_order_acquire) != read_count) {
            state.SkipWithError("readiness loop failed");
            failed = true;
            break;
        }
    }

    RuntimeT::shutdown();
    if (!failed) {
        state.SetItemsProcessed(state.iterations() * read_count);
    }
}

#if AF_DETAIL_HAS_EPOLL

struct EpollReadinessIoThreadTag;

struct EpollReadinessRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<EpollReadinessIoThreadTag, 1, af::ThreadKind::Epoll, "readiness">());
    static constexpr std::size_t spsc_queue_capacity = readiness_queue_capacity;
    static constexpr std::size_t external_queue_capacity = readiness_queue_capacity;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using EpollReadinessRuntime = af::AsyncRuntime<EpollReadinessRuntimeTraits>;
using EpollReadinessReadLoopTask =
    ReadinessReadLoopTask<EpollReadinessRuntime, EpollReadinessIoThreadTag>;

void BM_LiveEpollReadinessRearm(benchmark::State &state) {
    run_readiness_rearm_benchmark<EpollReadinessRuntime, EpollReadinessIoThreadTag,
                                  EpollReadinessReadLoopTask>(state, "epoll backend unavailable");
}

BENCHMARK(BM_LiveEpollReadinessRearm)
    ->Arg(64)
    ->Arg(1024)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond);

#endif

#if AF_DETAIL_HAS_KQUEUE

struct KqueueReadinessIoThreadTag;

struct KqueueReadinessRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<KqueueReadinessIoThreadTag, 1, af::ThreadKind::Kqueue, "readiness">());
    static constexpr std::size_t spsc_queue_capacity = readiness_queue_capacity;
    static constexpr std::size_t external_queue_capacity = readiness_queue_capacity;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using KqueueReadinessRuntime = af::AsyncRuntime<KqueueReadinessRuntimeTraits>;
using KqueueReadinessReadLoopTask =
    ReadinessReadLoopTask<KqueueReadinessRuntime, KqueueReadinessIoThreadTag>;

void BM_LiveKqueueReadinessRearm(benchmark::State &state) {
    run_readiness_rearm_benchmark<KqueueReadinessRuntime, KqueueReadinessIoThreadTag,
                                  KqueueReadinessReadLoopTask>(state, "kqueue backend unavailable");
}

BENCHMARK(BM_LiveKqueueReadinessRearm)
    ->Arg(64)
    ->Arg(1024)
    ->UseRealTime()
    ->Unit(benchmark::kMicrosecond);

#endif

} // namespace
#endif
