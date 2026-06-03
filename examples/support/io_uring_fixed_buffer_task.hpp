#pragma once

#include <cerrno>
#include <cstdint>

#include "io_uring_fixed_buffer_runtime.hpp"

#if defined(__linux__)
#include <sys/uio.h>

namespace io_uring_fixed_buffer_example {

class FixedBufferRoundTripTask final : public FixedBufferTask {
public:
    explicit FixedBufferRoundTripTask(FixedBufferTask::FactoryToken token)
        : FixedBufferTask(token) {}

    bool do_it(int fd, FixedBufferRoundTripResult *result) {
        file_.reset(FixedBufferThreads::IO_0, fd);
        result_ = result;
        return schedule(FixedBufferThreads::IO_0);
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
            return complete(0);
        }
        return complete(EIO);
    }

    af::TaskResult register_buffer() {
        iovec iov{buffer_, sizeof(buffer_)};
        int error = 0;
        if (!fixed_async::io_register_buffers(FixedBufferThreads::IO_0, &iov, 1, &error)) {
            return complete(error == 0 ? EIO : error);
        }
        registered_ = true;
        buffer_[0] = value_;
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status =
            file_.write_fixed_at(*this, af::IoFixedBuffer{buffer_, 1, 0}, 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U) {
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
        const af::IoStatus status =
            file_.read_fixed_at(*this, af::IoFixedBuffer{buffer_, 1, 0}, 0, read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (!fixed_async::io_unregister_buffers(FixedBufferThreads::IO_0, &unregister_error) &&
                error == 0) {
                error = unregister_error == 0 ? EIO : unregister_error;
            }
            registered_ = false;
        }
        result_->byte_read = buffer_[0];
        result_->error = error;
        return done();
    }

    State state_{State::Register};
    af::IoFile<FixedBufferThread> file_{};
    alignas(64) char buffer_[64]{};
    char value_{'R'};
    bool registered_{false};
    af::IoOpState write_{};
    af::IoOpState fsync_{};
    af::IoOpState read_{};
    FixedBufferRoundTripResult *result_{nullptr};
};

} // namespace io_uring_fixed_buffer_example

#else

namespace io_uring_fixed_buffer_example {

class FixedBufferRoundTripTask final : public FixedBufferTask {
public:
    explicit FixedBufferRoundTripTask(FixedBufferTask::FactoryToken token)
        : FixedBufferTask(token) {}

    bool do_it(int fd, FixedBufferRoundTripResult *result) {
        static_cast<void>(fd);
        static_cast<void>(result);
        return false;
    }

private:
    af::TaskResult run() override {
        return failed();
    }
};

} // namespace io_uring_fixed_buffer_example

#endif
