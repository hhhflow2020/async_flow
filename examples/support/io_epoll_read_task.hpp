#pragma once

#include <atomic>
#include <cstdint>
#include <iostream>

#include "../app_runtime.hpp"

namespace io_epoll_example {

#if defined(__linux__)

class ReadOneByteTask final : public Task {
public:
    explicit ReadOneByteTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, std::atomic<int> *armed) {
        fd_ = fd;
        armed_ = armed;
        return schedule(AppThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        ArmRead,
        Consume,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ArmRead:
            return arm_read();

        case State::Consume:
            return consume();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_ = State::Consume;
        const af::IoStatus status =
            af::io_read_some(*this, AppThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.pending()) {
            return failed();
        }
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    af::TaskResult consume() {
        const af::IoStatus status =
            af::io_read_some(*this, AppThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }
        std::cout << "IO_0 received byte: " << value_ << '\n';
        return done();
    }

    State state_{State::ArmRead};
    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<int> *armed_{nullptr};
};

#else

class ReadOneByteTask final : public Task {
public:
    explicit ReadOneByteTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, std::atomic<int> *armed) {
        static_cast<void>(fd);
        static_cast<void>(armed);
        return false;
    }

private:
    af::TaskResult run() override {
        return failed();
    }
};

#endif

} // namespace io_epoll_example
