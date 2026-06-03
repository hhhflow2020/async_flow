#pragma once

#include <cstdint>

#include "io_uring_datagram_runtime.hpp"

#include <netinet/in.h>
#include <sys/socket.h>

namespace io_uring_datagram_example {

class DatagramClientTask final : public DatagramTask {
public:
    explicit DatagramClientTask(DatagramTask::FactoryToken token) : DatagramTask(token) {}

    bool do_it(int fd, sockaddr_in server, socklen_t server_size, bool *ok, char *response_seen) {
        socket_.reset(DatagramThreads::IO_0, fd);
        server_ = server;
        server_size_ = server_size;
        ok_ = ok;
        response_seen_ = response_seen;
        return schedule(DatagramThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        SendRequest,
        ReceiveResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::SendRequest:
            return send_request();
        case State::ReceiveResponse:
            return receive_response();
        }
        return failed();
    }

    af::TaskResult send_request() {
        const af::IoStatus status =
            socket_.send_to_some(*this, &request_, sizeof(request_),
                                 reinterpret_cast<const sockaddr *>(&server_), server_size_, send_);
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
        peer_size_ = sizeof(peer_);
        const af::IoStatus status =
            socket_.recv_from_some(*this, &response_, sizeof(response_),
                                   reinterpret_cast<sockaddr *>(&peer_), &peer_size_, recv_);
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

    State state_{State::SendRequest};
    af::UdpSocket<DatagramThread> socket_{};
    sockaddr_in server_{};
    socklen_t server_size_{sizeof(server_)};
    char request_{'Q'};
    char response_{0};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState send_{};
    af::IoOpState recv_{};
    bool *ok_{nullptr};
    char *response_seen_{nullptr};
};

} // namespace io_uring_datagram_example
