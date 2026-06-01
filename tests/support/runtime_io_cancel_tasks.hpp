#pragma once

class CancellableSocketReadTask final : public IoTaskBase {
public:
    explicit CancellableSocketReadTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, std::atomic<af::IoOpState *> *state, std::atomic<int> *armed,
               std::atomic<int> *completed, std::atomic<int> *error) {
        fd_ = fd;
        state_ = state;
        armed_ = armed;
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Arm,
        Finish,
    };

    af::TaskResult run() override {
        switch (state_machine_) {
        case State::Arm:
            return arm_read();

        case State::Finish:
            return finish_read();
        }
        return failed();
    }

    af::TaskResult arm_read() {
        state_machine_ = State::Finish;
        const af::IoStatus status =
            af::io_read_some(*this, IoTestThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.pending()) {
            return failed();
        }
        state_->store(&read_, std::memory_order_release);
        armed_->fetch_add(1, std::memory_order_release);
        return pending();
    }

    af::TaskResult finish_read() {
        const af::IoStatus status =
            af::io_read_some(*this, IoTestThreads::IO_0, fd_, &value_, sizeof(value_), read_);
        if (!status.failed() || status.error != ECANCELED) {
            return failed();
        }
        error_->store(status.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_machine_{State::Arm};
    int fd_{-1};
    char value_{0};
    af::IoOpState read_{};
    std::atomic<af::IoOpState *> *state_{nullptr};
    std::atomic<int> *armed_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
};

class CancelIoStateTask final : public IoTaskBase {
public:
    explicit CancelIoStateTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<af::IoOpState *> *state, bool cancel_twice, std::atomic<int> *completed,
               std::atomic<int> *first_result, std::atomic<int> *second_result,
               std::atomic<int> *error) {
        state_ = state;
        cancel_twice_ = cancel_twice;
        completed_ = completed;
        first_result_ = first_result;
        second_result_ = second_result;
        error_ = error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState *state = state_->load(std::memory_order_acquire);
        if (state == nullptr) {
            return failed();
        }

        const bool first = IoRuntime::cancel_io(IoTestThreads::IO_0, *state);
        first_result_->store(first ? 1 : 0, std::memory_order_release);
        error_->store(state->wait.error, std::memory_order_release);

        if (cancel_twice_) {
            const bool second = IoRuntime::cancel_io(IoTestThreads::IO_0, *state);
            second_result_->store(second ? 1 : 0, std::memory_order_release);
        }

        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<af::IoOpState *> *state_{nullptr};
    bool cancel_twice_{false};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *first_result_{nullptr};
    std::atomic<int> *second_result_{nullptr};
    std::atomic<int> *error_{nullptr};
};

class CancelIdleIoStateTask final : public IoTaskBase {
public:
    explicit CancelIdleIoStateTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *result, std::atomic<int> *error) {
        completed_ = completed;
        result_ = result;
        error_ = error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        af::IoOpState state{};
        const bool ok = IoRuntime::cancel_io(IoTestThreads::IO_0, state);
        result_->store(ok ? 1 : 0, std::memory_order_release);
        error_->store(state.wait.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *result_{nullptr};
    std::atomic<int> *error_{nullptr};
};
