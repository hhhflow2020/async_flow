#pragma once

#include <cerrno>
#include <cstdint>
#include <cstring>

#include "io_uring_send_zc_runtime.hpp"

#if defined(__linux__)
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_uring_send_zc_example {

class SendZcClientTask final : public SendZcTaskBase {
public:
    explicit SendZcClientTask(SendZcTaskBase::FactoryToken token) : SendZcTaskBase(token) {}

    bool do_it(sockaddr_in server, socklen_t server_size, SendZcClientResult* result) {
        if (server_size == 0U || result == nullptr) {
            return false;
        }
        server_ = server;
        server_size_ = server_size;
        result_ = result;
        return schedule(SendZcThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        CreateSocket,
        FinishSocket,
        Connect,
        Receive,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::CreateSocket:
            return create_socket();
        case State::FinishSocket:
            return finish_socket();
        case State::Connect:
            return connect();
        case State::Receive:
            return receive_payload();
        }
        return finish(EIO);
    }

    af::TaskResult create_socket() {
        state_ = State::FinishSocket;
        return consume_socket_status(af::io_socket(
            *this,
            SendZcThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &fd_,
            socket_));
    }

    af::TaskResult finish_socket() {
        return consume_socket_status(af::io_socket(
            *this,
            SendZcThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &fd_,
            socket_));
    }

    af::TaskResult consume_socket_status(af::IoStatus status) {
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd_ < 0) {
            return finish(status.failed() ? status.error : EIO);
        }

        owned_.reset(fd_);
        fd_ = -1;
        stream_.reset(SendZcThread::IO_0, owned_.get());
        state_ = State::Connect;
        return again();
    }

    af::TaskResult connect() {
        const af::IoStatus status = stream_.connect(
            *this,
            reinterpret_cast<const sockaddr*>(&server_),
            server_size_,
            connect_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }

        state_ = State::Receive;
        return again();
    }

    af::TaskResult receive_payload() {
        const af::IoStatus status = stream_.recv_some(
            *this,
            result_->received.data() + result_->bytes_read,
            result_->received.size() - result_->bytes_read,
            recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return finish(status.failed() ? status.error : EIO);
        }

        result_->bytes_read += status.bytes;
        if (result_->bytes_read < result_->received.size()) {
            return again();
        }

        result_->payload_match =
            std::memcmp(result_->received.data(), send_zc_payload, send_zc_payload_size) == 0;
        result_->ok = result_->payload_match;
        result_->error = result_->payload_match ? 0 : EIO;
        return result_->payload_match ? done() : failed();
    }

    af::TaskResult finish(int error) {
        result_->ok = false;
        result_->error = error == 0 ? EIO : error;
        return failed();
    }

    State state_{State::CreateSocket};
    sockaddr_in server_{};
    socklen_t server_size_{sizeof(server_)};
    int fd_{-1};
    af::UniqueFd owned_{};
    af::TcpStream<SendZcThread> stream_{};
    af::IoOpState socket_{};
    af::IoOpState connect_{};
    af::IoOpState recv_{};
    SendZcClientResult* result_{nullptr};
};

} // namespace io_uring_send_zc_example

#endif
