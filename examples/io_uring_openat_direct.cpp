#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <iostream>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {

enum class DirectOpenThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct DirectOpenRuntimeTraits {
    using Thread = DirectOpenThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(DirectOpenThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(DirectOpenThread thread) noexcept {
        return thread == DirectOpenThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using direct_open_async = af::AsyncRuntime<DirectOpenRuntimeTraits>;
using DirectOpenTask = direct_open_async::Task;

#if defined(__linux__)
[[nodiscard]] bool unsupported_direct_open_error(int error) noexcept {
    return error == EINVAL || error == EBADF || error == ENOSYS || error == ENXIO
#ifdef EOPNOTSUPP
        || error == EOPNOTSUPP
#endif
        ;
}

class DirectOpenRoundTripTask final : public DirectOpenTask {
public:
    explicit DirectOpenRoundTripTask(DirectOpenTask::FactoryToken token) : DirectOpenTask(token) {}

    bool do_it(
        const char* path,
        int* error,
        char* byte_read) {
        path_ = path;
        error_ = error;
        byte_read_ = byte_read;
        return schedule(DirectOpenThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Open,
        Write,
        Fsync,
        Read,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_sparse_slot();

        case State::Open:
            return open_direct();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::Unregister:
            return complete(0);
        }
        return complete(EIO);
    }

    af::TaskResult register_sparse_slot() {
        const int sparse = -1;
        int error = 0;
        if (!direct_open_async::io_register_files(DirectOpenThread::IO_0, &sparse, 1, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        registered_ = true;
        state_ = State::Open;
        return again();
    }

    af::TaskResult open_direct() {
        const af::IoStatus status = af::io_openat_direct(
            *this,
            DirectOpenThread::IO_0,
            AT_FDCWD,
            path_,
            O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC,
            0600U,
            0,
            &file_,
            open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status = file_.write_at(*this, &value_, sizeof(value_), 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return complete(status.failed() ? status.error : EIO);
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
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status = file_.read_at(*this, &read_, sizeof(read_), 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (!direct_open_async::io_unregister_files(DirectOpenThread::IO_0, &unregister_error) &&
                error == 0) {
                error = unregister_error == 0 ? EIO : unregister_error;
            }
            registered_ = false;
        }
        *byte_read_ = read_;
        *error_ = error;
        return done();
    }

    State state_{State::Register};
    const char* path_{nullptr};
    af::IoFixedFile<DirectOpenThread> file_{};
    char value_{'D'};
    char read_{0};
    bool registered_{false};
    af::IoOpState open_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    int* error_{nullptr};
    char* byte_read_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    direct_open_async::init();
    if (!direct_open_async::io_uring_backend_available(DirectOpenThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        direct_open_async::shutdown();
        return 0;
    }

    char path[] = "/tmp/asyncflow-openat-direct-XXXXXX";
    int seed = ::mkstemp(path);
    if (seed < 0) {
        std::cout << "mkstemp failed\n";
        direct_open_async::shutdown();
        return 1;
    }
    ::close(seed);
    static_cast<void>(::unlink(path));

    int error{0};
    char byte_read{0};
    const bool started = direct_open_async::start_task<DirectOpenRoundTripTask>(
        path,
        &error,
        &byte_read);
    AF_ASSERT(started);

    if (!started) {
        std::cout << "io_uring openat direct task did not start\n";
        static_cast<void>(::unlink(path));
        direct_open_async::shutdown();
        return 1;
    }
    direct_open_async::shutdown();

    if (error != 0) {
        std::cout << "io_uring openat direct "
                  << (unsupported_direct_open_error(error) ? "unsupported" : "failed")
                  << " error=" << error << '\n';
        static_cast<void>(::unlink(path));
        return unsupported_direct_open_error(error) ? 0 : 1;
    }

    std::cout << "io_uring openat direct byte=" << byte_read << '\n';
    static_cast<void>(::unlink(path));
    return 0;
#else
    std::cout << "io_uring openat direct example is Linux-only\n";
    return 0;
#endif
}
