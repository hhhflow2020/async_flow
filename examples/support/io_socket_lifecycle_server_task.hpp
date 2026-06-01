#pragma once

#include <cerrno>
#include <cstdint>

#include "io_socket_lifecycle_client_task.hpp"

#if !defined(_WIN32)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_socket_lifecycle_example {

class SocketLifecycleServerTask final : public SocketTask {
public:
    explicit SocketLifecycleServerTask(SocketTask::FactoryToken token) : SocketTask(token) {}

    bool do_it(SocketLifecycleServerResult *server_result,
               SocketLifecycleClientResult *client_result) {
        if (server_result == nullptr || client_result == nullptr) {
            return false;
        }
        server_result_ = server_result;
        client_result_ = client_result;
        return schedule(SocketThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        CreateListener,
        FinishListener,
        AcceptClient,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::CreateListener:
            return create_listener();
        case State::FinishListener:
            return finish_listener();
        case State::AcceptClient:
            return accept_client();
        }
        return finish(EIO);
    }

    af::TaskResult create_listener() {
        state_ = State::FinishListener;
        return consume_listener_status(af::io_socket(*this, SocketThread::IO_0, AF_INET,
                                                     lifecycle_stream_socket_type(), 0,
                                                     &listener_fd_, socket_));
    }

    af::TaskResult finish_listener() {
        return consume_listener_status(af::io_socket(*this, SocketThread::IO_0, AF_INET,
                                                     lifecycle_stream_socket_type(), 0,
                                                     &listener_fd_, socket_));
    }

    af::TaskResult consume_listener_status(af::IoStatus status) {
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || listener_fd_ < 0) {
            return finish(status.failed() ? status.error : EIO);
        }

        listener_owned_.reset(listener_fd_);
        listener_fd_ = -1;
        int flag_error = 0;
        if (!apply_lifecycle_socket_flags(listener_owned_.get(), flag_error)) {
            return finish(flag_error);
        }
        listener_.reset(SocketThread::IO_0, listener_owned_.get());
        return configure_listener();
    }

    af::TaskResult configure_listener() {
        const int one = 1;
        af::IoStatus status =
            listener_.setsockopt(*this, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        status =
            listener_.bind(*this, reinterpret_cast<const sockaddr *>(&address), sizeof(address));
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }

        status = listener_.listen(*this, 16);
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }

        socklen_t address_size = sizeof(bound_address_);
        status = listener_.getsockname(*this, reinterpret_cast<sockaddr *>(&bound_address_),
                                       &address_size);
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }
        if (bound_address_.sin_family != AF_INET || bound_address_.sin_port == 0 ||
            address_size == 0U) {
            return finish(EIO);
        }

        if (!socket_async::start_task<SocketLifecycleClientTask>(bound_address_, client_result_)) {
            return finish(EAGAIN);
        }

        state_ = State::AcceptClient;
        return accept_client();
    }

    af::TaskResult accept_client() {
        const af::IoStatus status =
            listener_.accept_some(*this, nullptr, nullptr, &accepted_fd_, accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || accepted_fd_ < 0) {
            return finish(status.failed() ? status.error : EIO);
        }

        accepted_owned_.reset(accepted_fd_);
        accepted_fd_ = -1;
        af::TcpStream<SocketThread> accepted_stream(SocketThread::IO_0, accepted_owned_.get());
        sockaddr_storage peer{};
        socklen_t peer_size = sizeof(peer);
        const af::IoStatus peer_status =
            accepted_stream.getpeername(*this, reinterpret_cast<sockaddr *>(&peer), &peer_size);
        if (!peer_status.ready() || peer_size == 0U) {
            return finish(peer_status.failed() ? peer_status.error : EIO);
        }

        server_result_->ok = true;
        server_result_->error = 0;
        server_result_->port = ntohs(bound_address_.sin_port);
        return done();
    }

    af::TaskResult finish(int error) {
        server_result_->ok = false;
        server_result_->error = error == 0 ? EIO : error;
        server_result_->port = ntohs(bound_address_.sin_port);
        return failed();
    }

    State state_{State::CreateListener};
    af::IoOpState socket_{};
    af::IoOpState accept_{};
    int listener_fd_{-1};
    int accepted_fd_{-1};
    af::UniqueFd listener_owned_{};
    af::UniqueFd accepted_owned_{};
    af::TcpListener<SocketThread> listener_{};
    sockaddr_in bound_address_{};
    SocketLifecycleServerResult *server_result_{nullptr};
    SocketLifecycleClientResult *client_result_{nullptr};
};

} // namespace io_socket_lifecycle_example

#endif
