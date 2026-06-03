#pragma once

#include "io_shutdown_runtime.hpp"

#if defined(__linux__)
#include <sys/socket.h>
#endif

namespace io_shutdown_example {

#if defined(__linux__)

class ShutdownWriteTask final : public ShutdownTaskBase {
public:
    explicit ShutdownWriteTask(ShutdownTaskBase::FactoryToken token) : ShutdownTaskBase(token) {}

    bool do_it(int fd, int *error) {
        stream_.reset(ShutdownThreads::IO_0, fd);
        error_ = error;
        return schedule(ShutdownThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.shutdown(*this, SHUT_WR, shutdown_);
        if (status.pending()) {
            return pending();
        }
        *error_ = status.ready() ? 0 : status.error;
        return done();
    }

    af::TcpStream<ShutdownThread> stream_{};
    af::IoOpState shutdown_{};
    int *error_{nullptr};
};

#else

class ShutdownWriteTask final : public ShutdownTaskBase {
public:
    explicit ShutdownWriteTask(ShutdownTaskBase::FactoryToken token) : ShutdownTaskBase(token) {}

    bool do_it(int fd, int *error) {
        static_cast<void>(fd);
        static_cast<void>(error);
        return false;
    }

private:
    af::TaskResult run() override {
        return failed();
    }
};

#endif

} // namespace io_shutdown_example
