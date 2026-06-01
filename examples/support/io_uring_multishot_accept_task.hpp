#pragma once

#include <cerrno>
#include <cstdint>

#include "io_uring_multishot_accept_runtime.hpp"

#if defined(__linux__)

namespace io_uring_multishot_accept_example {

class MultishotAcceptTask final : public AcceptTask {
public:
    explicit MultishotAcceptTask(AcceptTask::FactoryToken token) : AcceptTask(token) {}

    bool do_it(int fd, int target_accepts, MultishotAcceptResult *result) {
        if (fd < 0 || target_accepts <= 0 || result == nullptr) {
            return false;
        }
        listener_.reset(AcceptThreads::IO_0, fd);
        target_accepts_ = target_accepts;
        result_ = result;
        return schedule(AcceptThreads::IO_0);
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
        return complete(EIO);
    }

    af::TaskResult accept_one() {
        const af::IoStatus status =
            listener_.accept_multishot(*this, nullptr, nullptr, &accepted_fd_, accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }

        af::UniqueFd accepted(accepted_fd_);
        accepted_fd_ = -1;
        ++result_->accepted_count;
        if (result_->accepted_count < target_accepts_) {
            return pending();
        }
        if (!accept_.waiting) {
            return complete(0);
        }
        if (!accept_async::cancel_io(AcceptThreads::IO_0, accept_)) {
            return complete(accept_.wait.error == 0 ? EIO : accept_.wait.error);
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
        return complete(status.failed() && status.error == ECANCELED ? 0 : EIO);
    }

    af::TaskResult complete(int error) {
        result_->error = error;
        return done();
    }

    State state_{State::Accept};
    af::TcpListener<AcceptThread> listener_{};
    int accepted_fd_{-1};
    int target_accepts_{0};
    af::IoOpState accept_{};
    MultishotAcceptResult *result_{nullptr};
};

} // namespace io_uring_multishot_accept_example

#endif
