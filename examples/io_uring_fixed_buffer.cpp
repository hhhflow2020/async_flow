#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace {

enum class FixedBufferThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct FixedBufferRuntimeTraits {
    using Thread = FixedBufferThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(FixedBufferThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(FixedBufferThread thread) noexcept {
        return thread == FixedBufferThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using fixed_async = af::AsyncRuntime<FixedBufferRuntimeTraits>;
using FixedBufferTask = fixed_async::Task;

bool wait_until(std::atomic<int>& value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

#if defined(__linux__)
class FixedBufferRoundTripTask final : public FixedBufferTask {
public:
    explicit FixedBufferRoundTripTask(FixedBufferTask::FactoryToken token)
        : FixedBufferTask(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<char>* byte_read) {
        file_.reset(FixedBufferThread::IO_0, fd);
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(FixedBufferThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Write,
        Fsync,
        Read,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_buffer();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::Unregister:
            return unregister_buffer();
        }
        return failed();
    }

    af::TaskResult register_buffer() {
        iovec iov{buffer_, sizeof(buffer_)};
        int error = 0;
        if (!fixed_async::io_register_buffers(FixedBufferThread::IO_0, &iov, 1, &error)) {
            std::cout << "io_uring register buffers failed error=" << error << '\n';
            return failed();
        }
        buffer_[0] = value_;
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U) {
            return failed();
        }
        buffer_[0] = 0;
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U) {
            return failed();
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_buffer() {
        int error = 0;
        if (!fixed_async::io_unregister_buffers(FixedBufferThread::IO_0, &error)) {
            std::cout << "io_uring unregister buffers failed error=" << error << '\n';
            return failed();
        }
        byte_read_->store(buffer_[0], std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    af::IoFile<FixedBufferThread> file_{};
    alignas(64) char buffer_[64]{};
    char value_{'R'};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    fixed_async::init();
    if (!fixed_async::io_uring_backend_available(FixedBufferThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        fixed_async::shutdown();
        return 0;
    }

    char path[] = "/tmp/asyncflow-fixed-buffer-XXXXXX";
    const int fd = ::mkstemp(path);
    if (fd < 0) {
        std::cout << "mkstemp failed\n";
        fixed_async::shutdown();
        return 1;
    }
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    const bool started = fixed_async::start_task<FixedBufferRoundTripTask>(
        file.get(),
        &completed,
        &byte_read);
    AF_ASSERT(started);

    if (!started || !wait_until(completed, 1)) {
        std::cout << "io_uring fixed buffer task timed out\n";
        file.reset();
        static_cast<void>(::unlink(path));
        fixed_async::shutdown();
        return 1;
    }

    std::cout << "io_uring fixed buffer byte="
              << byte_read.load(std::memory_order_acquire) << '\n';
    file.reset();
    static_cast<void>(::unlink(path));
    fixed_async::shutdown();
    return 0;
#else
    std::cout << "io_uring fixed buffer example is Linux-only\n";
    return 0;
#endif
}
