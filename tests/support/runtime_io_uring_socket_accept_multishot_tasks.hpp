#pragma once

class UringTcpAcceptMultishotTask final : public UringIoTaskBase {
public:
    explicit UringTcpAcceptMultishotTask(UringIoTaskBase::FactoryToken token)
        : UringIoTaskBase(token) {}

    bool do_it(int fd, int target_accepts, std::atomic<int> *armed, std::atomic<int> *completed,
               std::atomic<int> *accepted_count, std::atomic<int> *error) {
        listener_.reset(IoTestThreads::IO_0, fd);
        target_accepts_ = target_accepts;
        armed_ = armed;
        completed_ = completed;
        accepted_count_ = accepted_count;
        error_ = error;
        return schedule(IoTestThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Accept,
        Cancel,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Accept:
            return accept_one();
        case State::Cancel:
            return finish_cancel();
        }
        return failed();
    }

    af::TaskResult accept_one() {
        const af::IoStatus status =
            listener_.accept_multishot(*this, nullptr, nullptr, &accepted_fd_, accept_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (status.failed()) {
            error_->store(status.error, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!status.ready() || accepted_fd_ < 0) {
            return failed();
        }

        ::close(accepted_fd_);
        accepted_fd_ = -1;
        const int accepted = accepted_count_->fetch_add(1, std::memory_order_acq_rel) + 1;
        if (accepted < target_accepts_) {
            return pending();
        }

        if (!accept_.waiting) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        if (!UringIoRuntime::cancel_io(IoTestThreads::IO_0, accept_)) {
            error_->store(accept_.wait.error == 0 ? EIO : accept_.wait.error,
                          std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }
        state_ = State::Cancel;
        return pending();
    }

    af::TaskResult finish_cancel() {
        int ignored = -1;
        const af::IoStatus status =
            listener_.accept_multishot(*this, nullptr, nullptr, &ignored, accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.failed() || status.error != ECANCELED) {
            return failed();
        }
        error_->store(0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Accept};
    af::TcpListener<IoTestThread> listener_{};
    int accepted_fd_{-1};
    int target_accepts_{0};
    bool armed_once_{false};
    af::IoOpState accept_{};
    std::atomic<int> *armed_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *accepted_count_{nullptr};
    std::atomic<int> *error_{nullptr};
};
