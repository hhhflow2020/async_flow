#pragma once

#include <cstdint>

#include "io_tcp_connect_accept_runtime.hpp"

#include <netinet/in.h>
#include <sys/socket.h>

namespace io_tcp_connect_accept_example {

class TcpServerTask final : public TcpTask {
public:
    explicit TcpServerTask(TcpTask::FactoryToken token) : TcpTask(token) {}

    bool do_it(int fd, bool *ok, char *request_seen) {
        listener_.reset(TcpThreads::IO_0, fd);
        ok_ = ok;
        request_seen_ = request_seen;
        return schedule(TcpThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        AcceptClient,
        ReceiveRequest,
        SendResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::AcceptClient:
            return accept_client();
        case State::ReceiveRequest:
            return receive_request();
        case State::SendResponse:
            return send_response();
        }
        return failed();
    }

    af::TaskResult accept_client() {
        peer_size_ = sizeof(peer_);
        int fd = -1;
        const af::IoStatus status = listener_.accept_some(
            *this, reinterpret_cast<sockaddr *>(&peer_), &peer_size_, &fd, accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return failed();
        }

        accepted_.reset(fd);
        stream_.reset(TcpThreads::IO_0, accepted_.get());
        state_ = State::ReceiveRequest;
        return again();
    }

    af::TaskResult receive_request() {
        const af::IoStatus status = stream_.recv_some(*this, &request_, sizeof(request_), read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }

        *request_seen_ = request_;
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        const af::IoStatus status = stream_.send_some(*this, &response_, sizeof(response_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        *ok_ = true;
        return done();
    }

    State state_{State::AcceptClient};
    af::TcpListener<TcpThread> listener_{};
    af::TcpStream<TcpThread> stream_{};
    af::UniqueFd accepted_{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    char request_{0};
    char response_{'R'};
    af::IoOpState accept_{};
    af::IoOpState read_{};
    af::IoOpState write_{};
    bool *ok_{nullptr};
    char *request_seen_{nullptr};
};

} // namespace io_tcp_connect_accept_example
