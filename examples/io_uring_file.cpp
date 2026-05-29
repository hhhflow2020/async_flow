#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <unistd.h>
#endif

namespace {

enum class FileThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct FileRuntimeTraits {
    using Thread = FileThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(FileThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(FileThread thread) noexcept {
        return thread == FileThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using file_async = af::AsyncRuntime<FileRuntimeTraits>;
using FileTask = file_async::Task;

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
class FileRoundTripTask final : public FileTask {
public:
    explicit FileRoundTripTask(FileTask::FactoryToken token) : FileTask(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<char>* byte_read) {
        file_.reset(FileThread::IO_0, fd);
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(FileThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Write,
        Fsync,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
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
        const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_)) {
            return failed();
        }
        byte_read_->store(read_, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Write};
    af::IoFile<FileThread> file_{};
    char value_{'I'};
    char read_{0};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<char>* byte_read_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    file_async::init();
    if (!file_async::io_uring_backend_available(FileThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        file_async::shutdown();
        return 0;
    }

    char path[] = "/tmp/asyncflow-file-XXXXXX";
    const int fd = ::mkstemp(path);
    if (fd < 0) {
        std::cout << "mkstemp failed\n";
        file_async::shutdown();
        return 1;
    }
    af::UniqueFd file(fd);

    std::atomic<int> completed{0};
    std::atomic<char> byte_read{0};
    const bool started = file_async::start_task<FileRoundTripTask>(
        file.get(),
        &completed,
        &byte_read);
    AF_ASSERT(started);

    if (!started || !wait_until(completed, 1)) {
        std::cout << "io_uring file task timed out\n";
        file.reset();
        static_cast<void>(::unlink(path));
        file_async::shutdown();
        return 1;
    }

    std::cout << "io_uring file byte=" << byte_read.load(std::memory_order_acquire) << '\n';
    file.reset();
    static_cast<void>(::unlink(path));
    file_async::shutdown();
    return 0;
#else
    std::cout << "io_uring file example is Linux-only\n";
    return 0;
#endif
}
