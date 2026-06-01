#pragma once

class SocketLifecycleSetupTask final : public IoTaskBase {
public:
    explicit SocketLifecycleSetupTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(std::atomic<int> *completed, std::atomic<int> *error, std::atomic<int> *reuse_value,
               std::atomic<int> *local_port, std::atomic<std::uint16_t> *ran_on) {
        completed_ = completed;
        error_ = error;
        reuse_value_ = reuse_value;
        local_port_ = local_port;
        ran_on_ = ran_on;
        return schedule(IoTestThreads::IO_0);
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
        return consume_socket_status(af::io_socket(*this, IoTestThreads::IO_0, AF_INET,
                                                   SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0,
                                                   &opened_fd_, socket_));
    }

    af::TaskResult finish_socket() {
        return consume_socket_status(af::io_socket(*this, IoTestThreads::IO_0, AF_INET,
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
        owned_.reset(opened_fd_);
        opened_fd_ = -1;
        listener_.reset(IoTestThreads::IO_0, owned_.get());
        return configure_listener();
    }

    af::TaskResult configure_listener() {
        ran_on_->store(IoRuntime::current_thread_index(), std::memory_order_release);

        const int one = 1;
        const af::IoStatus set_status =
            listener_.setsockopt(*this, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        if (!set_status.ready()) {
            return complete(set_status.error);
        }

        int reuse = 0;
        socklen_t reuse_size = sizeof(reuse);
        const af::IoStatus get_status =
            listener_.getsockopt(*this, SOL_SOCKET, SO_REUSEADDR, &reuse, &reuse_size);
        if (!get_status.ready()) {
            return complete(get_status.error);
        }
        reuse_value_->store(reuse, std::memory_order_release);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        const af::IoStatus bind_status =
            listener_.bind(*this, reinterpret_cast<const sockaddr *>(&address), sizeof(address));
        if (!bind_status.ready()) {
            return complete(bind_status.error);
        }

        const af::IoStatus listen_status = listener_.listen(*this, 16);
        if (!listen_status.ready()) {
            return complete(listen_status.error);
        }

        sockaddr_in local{};
        socklen_t local_size = sizeof(local);
        const af::IoStatus name_status =
            listener_.getsockname(*this, reinterpret_cast<sockaddr *>(&local), &local_size);
        if (!name_status.ready()) {
            return complete(name_status.error);
        }
        if (local.sin_family != AF_INET || local.sin_port == 0 || local_size == 0U) {
            return complete(EIO);
        }
        local_port_->store(static_cast<int>(ntohs(local.sin_port)), std::memory_order_release);
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
    af::UniqueFd owned_{};
    af::TcpListener<IoTestThread> listener_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *error_{nullptr};
    std::atomic<int> *reuse_value_{nullptr};
    std::atomic<int> *local_port_{nullptr};
    std::atomic<std::uint16_t> *ran_on_{nullptr};
};
