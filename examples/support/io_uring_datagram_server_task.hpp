#pragma once

#include <cstdint>

#include "io_uring_datagram_runtime.hpp"

#if !defined(_WIN32)
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_uring_datagram_example {

class DatagramServerTask final : public DatagramTask {
public:
    explicit DatagramServerTask(DatagramTask::FactoryToken token) : DatagramTask(token) {}

    bool do_it(int fd, bool *ok, char *request_seen) {
        socket_.reset(DatagramThread::IO_0, fd);
        ok_ = ok;
        request_seen_ = request_seen;
        return schedule(DatagramThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        ReceiveRequest,
        SendResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ReceiveRequest:
            return receive_request();
        case State::SendResponse:
            return send_response();
        }
        return failed();
    }

    af::TaskResult receive_request() {
        peer_size_ = sizeof(peer_);
        const af::IoStatus status =
            socket_.recv_from_some(*this, &request_, sizeof(request_),
                                   reinterpret_cast<sockaddr *>(&peer_), &peer_size_, recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_) || peer_size_ == 0U) {
            return failed();
        }

        *request_seen_ = request_;
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        const af::IoStatus status =
            socket_.send_to_some(*this, &response_, sizeof(response_),
                                 reinterpret_cast<const sockaddr *>(&peer_), peer_size_, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        *ok_ = true;
        return done();
    }

    State state_{State::ReceiveRequest};
    af::UdpSocket<DatagramThread> socket_{};
    char request_{0};
    char response_{'R'};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState recv_{};
    af::IoOpState send_{};
    bool *ok_{nullptr};
    char *request_seen_{nullptr};
};

} // namespace io_uring_datagram_example

#endif
