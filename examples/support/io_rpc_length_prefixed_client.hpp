#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "io_rpc_length_prefixed_runtime.hpp"

#if defined(__linux__)

namespace io_rpc_length_prefixed_example {

class RpcClientTask final : public RpcTask {
public:
    explicit RpcClientTask(RpcTask::FactoryToken token) : RpcTask(token) {}

    bool do_it(int fd, sockaddr_in server, socklen_t server_size, bool *ok, int *error,
               bool *response_ok) {
        stream_.reset(RpcThreads::IO_0, fd);
        server_ = server;
        server_size_ = server_size;
        ok_ = ok;
        error_ = error;
        response_ok_ = response_ok;
        return schedule(RpcThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Connect,
        SendRequestHeader,
        SendRequestBody,
        ReadResponseHeader,
        ReadResponseBody,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Connect:
            return connect();
        case State::SendRequestHeader:
            return send_request_header();
        case State::SendRequestBody:
            return send_request_body();
        case State::ReadResponseHeader:
            return read_response_header();
        case State::ReadResponseBody:
            return read_response_body();
        }
        return failed();
    }

    af::TaskResult complete(int error) {
        *error_ = error;
        *ok_ = true;
        return done();
    }

    af::TaskResult connect() {
        const af::IoStatus status = stream_.connect(
            *this, reinterpret_cast<const sockaddr *>(&server_), server_size_, connect_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }

        const char request[] = "PING";
        request_size_ = sizeof(request) - 1U;
        std::memcpy(request_, request, request_size_);
        request_written_ = 0;
        request_header_written_ = 0;
        const std::uint32_t net_len = htonl(static_cast<std::uint32_t>(request_size_));
        std::memcpy(request_header_, &net_len, sizeof(net_len));

        state_ = State::SendRequestHeader;
        return again();
    }

    af::TaskResult send_request_header() {
        const af::IoStatus status = stream_.send_some(
            *this, request_header_ + request_header_written_, 4U - request_header_written_, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        request_header_written_ += status.bytes;
        if (request_header_written_ < 4U) {
            return again();
        }

        state_ = State::SendRequestBody;
        return again();
    }

    af::TaskResult send_request_body() {
        const af::IoStatus status = stream_.send_some(*this, request_ + request_written_,
                                                      request_size_ - request_written_, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        request_written_ += status.bytes;
        if (request_written_ < request_size_) {
            return again();
        }

        state_ = State::ReadResponseHeader;
        return again();
    }

    af::TaskResult read_response_header() {
        const af::IoStatus status = stream_.recv_some(
            *this, response_header_ + response_header_read_, 4U - response_header_read_, read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        response_header_read_ += status.bytes;
        if (response_header_read_ < 4U) {
            return again();
        }

        std::uint32_t net_len = 0;
        std::memcpy(&net_len, response_header_, sizeof(net_len));
        response_size_ = static_cast<std::size_t>(ntohl(net_len));
        response_read_ = 0;
        if (response_size_ > kMaxFrameBytes) {
            return complete(EMSGSIZE);
        }

        state_ = State::ReadResponseBody;
        return again();
    }

    af::TaskResult read_response_body() {
        if (response_size_ == 0U) {
            return complete(0);
        }

        const af::IoStatus status = stream_.recv_some(*this, response_ + response_read_,
                                                      response_size_ - response_read_, read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        response_read_ += status.bytes;
        if (response_read_ < response_size_) {
            return again();
        }

        const bool ok = response_size_ == 4U && std::memcmp(response_, "PONG", 4U) == 0;
        *response_ok_ = ok;
        return complete(0);
    }

    State state_{State::Connect};
    af::TcpStream<RpcThread> stream_{};
    sockaddr_in server_{};
    socklen_t server_size_{sizeof(server_)};

    af::IoOpState connect_{};
    af::IoOpState write_{};
    af::IoOpState read_{};

    char request_header_[4]{};
    std::size_t request_header_written_{0};
    char request_[kMaxFrameBytes]{};
    std::size_t request_size_{0};
    std::size_t request_written_{0};

    char response_header_[4]{};
    std::size_t response_header_read_{0};
    char response_[kMaxFrameBytes]{};
    std::size_t response_size_{0};
    std::size_t response_read_{0};

    bool *ok_{nullptr};
    int *error_{nullptr};
    bool *response_ok_{nullptr};
};

} // namespace io_rpc_length_prefixed_example

#endif
