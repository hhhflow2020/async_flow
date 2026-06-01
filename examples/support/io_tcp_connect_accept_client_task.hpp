#pragma once

#include <cstdint>

#include "io_tcp_connect_accept_runtime.hpp"

#if !defined(_WIN32)
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_tcp_connect_accept_example {

class TcpClientTask final : public TcpTask {
public:
    explicit TcpClientTask(TcpTask::FactoryToken token) : TcpTask(token) {}

    bool do_it(int fd, sockaddr_in server, socklen_t server_size, bool *ok, char *response_seen) {
        stream_.reset(TcpThreads::IO_0, fd);
        server_ = server;
        server_size_ = server_size;
        ok_ = ok;
        response_seen_ = response_seen;
        return schedule(TcpThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Connect,
        SendRequest,
        ReceiveResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Connect:
            return connect();
        case State::SendRequest:
            return send_request();
        case State::ReceiveResponse:
            return receive_response();
        }
        return failed();
    }

    af::TaskResult connect() {
        const af::IoStatus status = stream_.connect(
            *this, reinterpret_cast<const sockaddr *>(&server_), server_size_, connect_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }

        state_ = State::SendRequest;
        return again();
    }

    af::TaskResult send_request() {
        const af::IoStatus status = stream_.send_some(*this, &request_, sizeof(request_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }

        state_ = State::ReceiveResponse;
        return again();
    }

    af::TaskResult receive_response() {
        const af::IoStatus status = stream_.recv_some(*this, &response_, sizeof(response_), read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        *response_seen_ = response_;
        *ok_ = true;
        return done();
    }

    State state_{State::Connect};
    af::TcpStream<TcpThread> stream_{};
    sockaddr_in server_{};
    socklen_t server_size_{sizeof(server_)};
    char request_{'Q'};
    char response_{0};
    af::IoOpState connect_{};
    af::IoOpState write_{};
    af::IoOpState read_{};
    bool *ok_{nullptr};
    char *response_seen_{nullptr};
};

} // namespace io_tcp_connect_accept_example

#endif
