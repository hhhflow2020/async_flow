#include <array>
#include <cstdio>
#include <cstdint>
#include <iostream>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {

struct OpenAtIoThreadTag;

struct OpenAtRuntimeTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<OpenAtIoThreadTag, 1, af::ThreadKind::IoUring, "openat-io">());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;
};

using openat_async = af::AsyncRuntime<OpenAtRuntimeTraits>;
using OpenAtTaskBase = openat_async::Task;
using OpenAtThread = openat_async::Thread;

struct OpenAtThreads {
    static constexpr OpenAtThread IO_0 =
        openat_async::thread_group<OpenAtIoThreadTag>().template at<0>();
};

#if defined(__linux__)
class OpenAtRoundTripTask final : public OpenAtTaskBase {
public:
    explicit OpenAtRoundTripTask(OpenAtTaskBase::FactoryToken token) : OpenAtTaskBase(token) {}

    bool do_it(const char *path, char *byte_read) {
        const int written = std::snprintf(path_.data(), path_.size(), "%s", path);
        if (written < 0 || static_cast<std::size_t>(written) >= path_.size()) {
            return false;
        }
        byte_read_ = byte_read;
        return schedule(OpenAtThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Open,
        Write,
        Fsync,
        Read,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Open:
            return open_file();

        case State::Write:
            return write_value();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();
        }
        return failed();
    }

    af::TaskResult open_file() {
        int fd = -1;
        const af::IoStatus status =
            af::io_openat(*this, OpenAtThreads::IO_0, AT_FDCWD, path_.data(),
                          O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, 0600U, &fd, open_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return failed();
        }
        owned_.reset(fd);
        file_.reset(OpenAtThreads::IO_0, owned_.get());
        state_ = State::Write;
        return again();
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
        if (!status.ready() || status.bytes != sizeof(read_) || read_ != value_) {
            return failed();
        }
        *byte_read_ = read_;
        return done();
    }

    State state_{State::Open};
    std::array<char, 160> path_{};
    af::UniqueFd owned_{};
    af::IoFile<OpenAtThread> file_{};
    char value_{'O'};
    char read_{0};
    af::IoOpState open_{};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    char *byte_read_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    openat_async::init();
    if (!openat_async::io_uring_backend_available(OpenAtThreads::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        openat_async::shutdown();
        return 0;
    }

    std::array<char, 160> path{};
    const int written = std::snprintf(path.data(), path.size(), "/tmp/asyncflow-openat-%ld",
                                      static_cast<long>(::getpid()));
    if (written < 0 || static_cast<std::size_t>(written) >= path.size()) {
        std::cerr << "path formatting failed\n";
        openat_async::shutdown();
        return 1;
    }
    static_cast<void>(::unlink(path.data()));

    char byte_read{0};
    const bool started = openat_async::start_task<OpenAtRoundTripTask>(path.data(), &byte_read);
    if (!started) {
        std::cerr << "io_uring openat task did not start\n";
        static_cast<void>(::unlink(path.data()));
        openat_async::shutdown();
        return 1;
    }

    openat_async::shutdown();
    std::cout << "io_uring openat byte=" << byte_read << '\n';
    static_cast<void>(::unlink(path.data()));
    return 0;
#else
    std::cout << "io_uring openat example is Linux-only\n";
    return 0;
#endif
}
