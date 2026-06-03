#pragma once

#include <chrono>
#include <cstdint>

#include "io_timeout_runtime.hpp"

namespace io_timeout_example {

class ReadWithTimeoutTask final : public Task {
public:
    explicit ReadWithTimeoutTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, std::chrono::nanoseconds timeout, int *error) {
        fd_ = fd;
        timeout_ = timeout;
        error_ = error;
        return schedule(AppThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Resume,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Arm:
            return arm_read();

        case State::Resume:
            return resume_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_ = State::Resume;
        deadline_.set_after(timeout_);
        const af::IoStatus status =
            af::io_read_some(*this, AppThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.pending()) {
            return failed();
        }

        const af::IoStatus timeout = af::arm_io_timeout(*this, AppThreads::IO_0, deadline_, read_);
        if (!timeout.pending()) {
            return failed();
        }
        return pending();
    }

    af::TaskResult resume_read() {
        const af::IoStatus timeout = af::arm_io_timeout(*this, AppThreads::IO_0, deadline_, read_);
        if (timeout.pending()) {
            return pending();
        }
        if (timeout.failed()) {
            *error_ = timeout.error;
            return done();
        }
        if (!timeout.ready()) {
            return failed();
        }

        const af::IoStatus status =
            af::io_read_some(*this, AppThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        *error_ = status.failed() ? status.error : 0;
        return done();
    }

    State state_{State::Arm};
    int fd_{-1};
    std::chrono::nanoseconds timeout_{0};
    char value_{0};
    af::IoOpState read_{};
    af::IoDeadline deadline_{};
    int *error_{nullptr};
};

} // namespace io_timeout_example
