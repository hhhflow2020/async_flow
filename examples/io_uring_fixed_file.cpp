#include <atomic>
#include <cerrno>
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

enum class FixedFileThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct FixedFileRuntimeTraits {
    using Thread = FixedFileThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(FixedFileThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(FixedFileThread thread) noexcept {
        return thread == FixedFileThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using fixed_file_async = af::AsyncRuntime<FixedFileRuntimeTraits>;
using FixedFileTask = fixed_file_async::Task;

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
class FixedFileRoundTripTask final : public FixedFileTask {
public:
    explicit FixedFileRoundTripTask(FixedFileTask::FactoryToken token) : FixedFileTask(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* completed,
        std::atomic<int>* error,
        std::atomic<char>* byte_read) {
        fd_ = fd;
        completed_ = completed;
        error_ = error;
        byte_read_ = byte_read;
        file_.reset(FixedFileThread::IO_0, 0);
        return schedule(FixedFileThread::IO_0);
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
            return register_file();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::Unregister:
            return unregister_file();
        }
        return complete(EIO);
    }

    af::TaskResult register_file() {
        int error = 0;
        if (!fixed_file_async::io_register_files(FixedFileThread::IO_0, &fd_, 1, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        iovec iov{buffer_, sizeof(buffer_)};
        if (!fixed_file_async::io_register_buffers(FixedFileThread::IO_0, &iov, 1, &error)) {
            return complete(error == 0 ? EIO : error);
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
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
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
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_fixed_at(
            *this,
            af::IoFixedBuffer{buffer_, 1, 0},
            0,
            read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_file() {
        int error = 0;
        if (!fixed_file_async::io_unregister_buffers(FixedFileThread::IO_0, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        if (!fixed_file_async::io_unregister_files(FixedFileThread::IO_0, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        byte_read_->store(buffer_[0], std::memory_order_release);
        return complete(0);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    int fd_{-1};
    af::IoFixedFile<FixedFileThread> file_{};
    alignas(64) char buffer_[64]{};
    char value_{'F'};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    fixed_file_async::init();
    if (!fixed_file_async::io_uring_backend_available(FixedFileThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        fixed_file_async::shutdown();
        return 0;
    }

    char path[] = "/tmp/asyncflow-fixed-file-XXXXXX";
    const int fd = ::mkstemp(path);
    if (fd < 0) {
        std::cout << "mkstemp failed\n";
        fixed_file_async::shutdown();
        return 1;
    }
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<int> error{0};
    std::atomic<char> byte_read{0};
    const bool started = fixed_file_async::start_task<FixedFileRoundTripTask>(
        file.get(),
        &completed,
        &error,
        &byte_read);
    AF_ASSERT(started);

    if (!started || !wait_until(completed, 1)) {
        std::cout << "io_uring fixed file task timed out\n";
        file.reset();
        static_cast<void>(::unlink(path));
        fixed_file_async::shutdown();
        return 1;
    }

    if (error.load(std::memory_order_acquire) != 0) {
        std::cout << "io_uring fixed file failed error="
                  << error.load(std::memory_order_acquire) << '\n';
        file.reset();
        static_cast<void>(::unlink(path));
        fixed_file_async::shutdown();
        return 1;
    }

    std::cout << "io_uring fixed file byte="
              << byte_read.load(std::memory_order_acquire) << '\n';
    file.reset();
    static_cast<void>(::unlink(path));
    fixed_file_async::shutdown();
    return 0;
#else
    std::cout << "io_uring fixed file example is Linux-only\n";
    return 0;
#endif
}
