#pragma once

class UringSocketCreateTask final : public UringIoTaskBase {
public:
    explicit UringSocketCreateTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *error) {
        completed_ = completed;
        error_ = error;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        CreateSocket,
        FinishSocket,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::CreateSocket:
            return create_socket();
        case State::FinishSocket:
            return finish_socket();
        }
        return failed();
    }

    af::TaskResult create_socket() {
        state_ = State::FinishSocket;
        return consume_socket_status(af::io_socket(*this, IoTestThread::IO_0, AF_INET,
                                                   SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                                                   &opened_fd_, socket_));
    }

    af::TaskResult finish_socket() {
        return consume_socket_status(af::io_socket(*this, IoTestThread::IO_0, AF_INET,
                                                   SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                                                   &opened_fd_, socket_));
    }

    af::TaskResult consume_socket_status(const af::IoStatus status) {
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || opened_fd_ < 0) {
            return complete(status.failed() ? status.error : EIO);
        }
        af::UniqueFd fd(opened_fd_);
        opened_fd_ = -1;
        return complete(0);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::CreateSocket};
    af::IoOpState socket_{};
    int opened_fd_{-1};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
};
