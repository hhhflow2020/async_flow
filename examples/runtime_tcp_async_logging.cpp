#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include "af/async_flow.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

struct RuntimeTcpLogicThreadTag;
struct RuntimeTcpIoThreadTag;

inline constexpr std::uint32_t runtime_tcp_records_per_task = 32;

struct RuntimeTcpTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<RuntimeTcpLogicThreadTag, 2, af::ThreadKind::Worker, "tcp-log-cpu">(),
        af::thread_group<RuntimeTcpIoThreadTag, 1, af::preferred_io_thread_kind, "tcp-log-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using runtime_tcp_async = af::AsyncRuntime<RuntimeTcpTraits>;
using RuntimeTcpTaskBase = runtime_tcp_async::Task;

struct RuntimeTcpThreads {
    static constexpr auto logic = runtime_tcp_async::thread_group<RuntimeTcpLogicThreadTag>();
    static constexpr auto IO_0 =
        runtime_tcp_async::thread_group<RuntimeTcpIoThreadTag>().template at<0>();
};

class RuntimeTcpLogTask final : public RuntimeTcpTaskBase {
public:
    explicit RuntimeTcpLogTask(RuntimeTcpTaskBase::FactoryToken token)
        : RuntimeTcpTaskBase(token) {}

    bool do_it(std::uint32_t task_id, std::atomic<int> *completed) {
        task_id_ = task_id;
        completed_ = completed;
        return schedule(RuntimeTcpThreads::logic.shard(task_id));
    }

private:
    af::TaskResult run() override {
        for (std::uint32_t i = 0; i < runtime_tcp_records_per_task; ++i) {
            LOG(INFO) << "runtime tcp log task=" << task_id_ << " seq=" << i
                      << " thread=" << runtime_tcp_async::current_thread_index();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::uint32_t task_id_{0};
    std::atomic<int> *completed_{nullptr};
};

bool wait_for_completion(std::atomic<int> &completed, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (completed.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

#if !defined(_WIN32)
void close_fd(int &fd) noexcept {
    if (fd >= 0) {
        static_cast<void>(::close(fd));
        fd = -1;
    }
}

void set_fd_nonblocking(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        static_cast<void>(::fcntl(fd, F_SETFL, flags | O_NONBLOCK));
    }
}

bool transient_socket_error() noexcept {
    return errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK;
}

int make_loopback_tcp_listener(std::uint16_t &port) noexcept {
    int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        return -1;
    }

    const int reuse = 1;
    static_cast<void>(::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
        ::listen(listener, 16) != 0) {
        close_fd(listener);
        return -1;
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(listener, reinterpret_cast<sockaddr *>(&address), &address_size) != 0) {
        close_fd(listener);
        return -1;
    }
    port = ntohs(address.sin_port);
    set_fd_nonblocking(listener);
    return listener;
}

int accept_until(int listener, std::chrono::steady_clock::time_point deadline) noexcept {
    while (std::chrono::steady_clock::now() < deadline) {
        int accepted = ::accept(listener, nullptr, nullptr);
        if (accepted >= 0) {
            set_fd_nonblocking(accepted);
            return accepted;
        }
        if (!transient_socket_error()) {
            return -1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return -1;
}

void receive_stream_lines(int listener, std::atomic<int> &received, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    int accepted = accept_until(listener, deadline);
    if (accepted < 0) {
        return;
    }

    std::array<char, 4096> buffer{};
    while (received.load(std::memory_order_acquire) < expected &&
           std::chrono::steady_clock::now() < deadline) {
        const auto n = ::recv(accepted, buffer.data(), buffer.size(), 0);
        if (n > 0) {
            for (std::ptrdiff_t i = 0; i < n; ++i) {
                if (buffer[static_cast<std::size_t>(i)] == '\n') {
                    received.fetch_add(1, std::memory_order_release);
                }
            }
            continue;
        }
        if (n == 0) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    close_fd(accepted);
}
#endif

} // namespace

int main() {
#if defined(_WIN32)
    std::cout << "runtime tcp async logging example is POSIX-only\n";
    return 0;
#else
    using namespace std::chrono_literals;

    std::uint16_t tcp_port = 0;
    int listener = make_loopback_tcp_listener(tcp_port);
    if (listener < 0) {
        std::cerr << "failed to create tcp listener: " << std::strerror(errno) << '\n';
        return 1;
    }

    constexpr int task_count = 4;
    constexpr int expected_records =
        task_count * static_cast<int>(runtime_tcp_records_per_task) + 1;
    std::atomic<int> received{0};
    std::thread receiver_thread(
        [&] { receive_stream_lines(listener, received, expected_records); });

    runtime_tcp_async::init();

    af::AsyncLogConfig config;
    config.queue_capacity = 1U << 15U;
    config.runtime_queue_capacity = 1U << 15U;
    config.max_batch_size = 512;
    config.overflow_policy = af::LogOverflowPolicy::DropNewest;
    config.flush_poll_interval = 1ms;
    config.backends.push_back(af::make_runtime_tcp_log_backend<runtime_tcp_async>({
        .thread = RuntimeTcpThreads::IO_0,
        .host = "127.0.0.1",
        .port = tcp_port,
        .reconnect_interval = 1ms,
        .batch_queue_capacity = 128,
        .max_batch_records = 128,
        .max_batches_per_run = 64,
    }));

    auto logging = af::start_async_logging_for_runtime<runtime_tcp_async>(std::move(config),
                                                                          RuntimeTcpThreads::IO_0);

    std::atomic<int> completed{0};
    bool started = true;
    for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(task_count); ++i) {
        started = runtime_tcp_async::start_task<RuntimeTcpLogTask>(i, &completed) && started;
    }

    const bool completed_all = wait_for_completion(completed, task_count);
    LOG(INFO) << "external tcp log after runtime tasks";

    const bool flushed = logging->flush(5s);
    const af::AsyncLogStats stats = logging->stats();
    logging->stop();
    runtime_tcp_async::shutdown();

    receiver_thread.join();
    close_fd(listener);

    std::cout << "started=" << started << " completed=" << completed.load(std::memory_order_acquire)
              << " accepted=" << stats.accepted << " dropped=" << stats.dropped
              << " flushed=" << flushed
              << " tcp_received=" << received.load(std::memory_order_acquire) << '\n';
    return started && completed_all && flushed && stats.dropped == 0U &&
                   received.load(std::memory_order_acquire) == expected_records
               ? 0
               : 1;
#endif
}
