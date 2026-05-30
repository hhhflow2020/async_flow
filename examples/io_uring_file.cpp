#include <cstdint>
#include <cstdlib>
#include <iostream>

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
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(FileThread thread) noexcept {
        return thread == FileThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using file_async = af::AsyncRuntime<FileRuntimeTraits>;
using FileTask = file_async::Task;

#if defined(__linux__)
class FileRoundTripTask final : public FileTask {
public:
    explicit FileRoundTripTask(FileTask::FactoryToken token) : FileTask(token) {}

    bool do_it(int fd, char* byte_read) {
        file_.reset(FileThread::IO_0, fd);
        byte_read_ = byte_read;
        return schedule(FileThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Write,
        Fsync,
        SeekStart,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::SeekStart:
            return seek_start();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_some(*this, &value_, sizeof(value_), write_);
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
        state_ = State::SeekStart;
        return again();
    }

    af::TaskResult seek_start() {
        if (::lseek(file_.fd(), 0, SEEK_SET) < 0) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_some(*this, &read_, sizeof(read_), read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_)) {
            return failed();
        }
        *byte_read_ = read_;
        return done();
    }

    State state_{State::Write};
    af::IoFile<FileThread> file_{};
    char value_{'I'};
    char read_{0};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    char* byte_read_{nullptr};
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

    char byte_read{0};
    const bool started = file_async::start_task<FileRoundTripTask>(
        file.get(),
        &byte_read);
    AF_ASSERT(started);

    if (!started) {
        std::cout << "io_uring file task did not start\n";
        file.reset();
        static_cast<void>(::unlink(path));
        file_async::shutdown();
        return 1;
    }

    file_async::shutdown();
    std::cout << "io_uring file byte=" << byte_read << '\n';
    file.reset();
    static_cast<void>(::unlink(path));
    return 0;
#else
    std::cout << "io_uring file example is Linux-only\n";
    return 0;
#endif
}
