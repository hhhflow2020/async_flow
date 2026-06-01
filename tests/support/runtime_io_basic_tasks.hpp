#pragma once

class IoHopTask final : public IoTaskBase {
public:
    explicit IoHopTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<std::uint16_t>* ran_on) {
        completed_ = completed;
        ran_on_ = ran_on;
        return schedule(IoTestThread::Logic_0);
    }

private:
    enum class State : std::uint8_t {
        Logic,
        Io,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Logic:
            state_ = State::Io;
            return pending_on(IoTestThread::IO_0);

        case State::Io:
            ran_on_->store(IoRuntime::current_thread_index(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        return failed();
    }

    State state_{State::Logic};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::uint16_t>* ran_on_{nullptr};
};

class WorkerIoWaitRejectedTask final : public IoTaskBase {
public:
    explicit WorkerIoWaitRejectedTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int>* completed, std::atomic<int>* error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::Logic_0);
    }

private:
    af::TaskResult run() override {
        af::IoResult result{};
        const bool ok = wait_io(IoTestThread::Logic_0, 0, af::io_readable, &result);
        if (ok) {
            return failed();
        }
        error_->store(result.error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* error_{nullptr};
};

