#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "io_rpc_length_prefixed_runtime.hpp"

#if defined(__linux__)

namespace io_rpc_length_prefixed_example {

class RpcServerTask;

class RpcProcessTask final : public RpcTask {
public:
    explicit RpcProcessTask(RpcTask::FactoryToken token) : RpcTask(token) {}

    bool do_it(RpcServerTask* server);

private:
    af::TaskResult run() override;

    RpcServerTask* server_{nullptr};
};

class RpcServerTask final : public RpcTask {
public:
    explicit RpcServerTask(RpcTask::FactoryToken token) : RpcTask(token) {}

    bool do_it(int listener_fd, bool* ok, int* error) {
        listener_.reset(RpcThread::IO_0, listener_fd);
        ok_ = ok;
        error_ = error;
        return schedule(RpcThread::IO_0);
    }

    [[nodiscard]] const char* request_data() const noexcept {
        return request_;
    }

    [[nodiscard]] std::size_t request_size() const noexcept {
        return request_size_;
    }

    void set_response(const char* data, std::size_t size) noexcept {
        if (data == nullptr) {
            response_size_ = 0;
            return;
        }
        if (size > kMaxFrameBytes) {
            size = kMaxFrameBytes;
        }
        std::memcpy(response_, data, size);
        response_size_ = size;
        response_written_ = 0;
        response_header_written_ = 0;

        const std::uint32_t net_len = htonl(static_cast<std::uint32_t>(response_size_));
        std::memcpy(response_header_, &net_len, sizeof(net_len));
        state_ = State::WriteResponseHeader;
    }

private:
    friend class RpcProcessTask;

    enum class State : std::uint8_t {
        Accept,
        ReadRequestHeader,
        ReadRequestBody,
        WaitLogic,
        WriteResponseHeader,
        WriteResponseBody,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Accept:
            return accept_client();
        case State::ReadRequestHeader:
            return read_request_header();
        case State::ReadRequestBody:
            return read_request_body();
        case State::WaitLogic:
            return pending();
        case State::WriteResponseHeader:
            return write_response_header();
        case State::WriteResponseBody:
            return write_response_body();
        }
        return failed();
    }

    af::TaskResult accept_client() {
        int fd = -1;
        const af::IoStatus status = listener_.accept_some(*this, nullptr, nullptr, &fd, accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return complete(status.failed() ? status.error : EIO);
        }

        accepted_.reset(fd);
        stream_.reset(RpcThread::IO_0, accepted_.get());
        state_ = State::ReadRequestHeader;
        return again();
    }

    af::TaskResult read_request_header() {
        const af::IoStatus status =
            stream_.recv_some(*this, request_header_ + request_header_read_, 4U - request_header_read_, read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        request_header_read_ += status.bytes;
        if (request_header_read_ < 4U) {
            return again();
        }

        std::uint32_t net_len = 0;
        std::memcpy(&net_len, request_header_, sizeof(net_len));
        request_size_ = static_cast<std::size_t>(ntohl(net_len));
        request_read_ = 0;
        if (request_size_ > kMaxFrameBytes) {
            return complete(EMSGSIZE);
        }

        state_ = State::ReadRequestBody;
        return again();
    }

    af::TaskResult read_request_body() {
        if (request_size_ == 0U) {
            request_read_ = 0;
            return dispatch_logic();
        }

        const af::IoStatus status = stream_.recv_some(
            *this,
            request_ + request_read_,
            request_size_ - request_read_,
            read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        request_read_ += status.bytes;
        if (request_read_ < request_size_) {
            return again();
        }

        return dispatch_logic();
    }

    af::TaskResult dispatch_logic() {
        state_ = State::WaitLogic;
        const bool started = rpc_async::start_task<RpcProcessTask>(this);
        if (!started) {
            return complete(EAGAIN);
        }
        return pending();
    }

    af::TaskResult write_response_header() {
        const af::IoStatus status = stream_.send_some(
            *this,
            response_header_ + response_header_written_,
            4U - response_header_written_,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        response_header_written_ += status.bytes;
        if (response_header_written_ < 4U) {
            return again();
        }

        state_ = State::WriteResponseBody;
        return again();
    }

    af::TaskResult write_response_body() {
        if (response_size_ == 0U) {
            return complete(0);
        }

        const af::IoStatus status = stream_.send_some(
            *this,
            response_ + response_written_,
            response_size_ - response_written_,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        response_written_ += status.bytes;
        if (response_written_ < response_size_) {
            return again();
        }

        return complete(0);
    }

    af::TaskResult complete(int error) {
        *error_ = error;
        *ok_ = true;
        return done();
    }

    State state_{State::Accept};
    af::TcpListener<RpcThread> listener_{};
    af::TcpStream<RpcThread> stream_{};
    af::UniqueFd accepted_{};

    af::IoOpState accept_{};
    af::IoOpState read_{};
    af::IoOpState write_{};

    char request_header_[4]{};
    std::size_t request_header_read_{0};
    char request_[kMaxFrameBytes]{};
    std::size_t request_size_{0};
    std::size_t request_read_{0};

    char response_header_[4]{};
    std::size_t response_header_written_{0};
    char response_[kMaxFrameBytes]{};
    std::size_t response_size_{0};
    std::size_t response_written_{0};

    bool* ok_{nullptr};
    int* error_{nullptr};
};

inline bool RpcProcessTask::do_it(RpcServerTask* server) {
    server_ = server;
    return schedule(RpcThread::Logic_0);
}

inline af::TaskResult RpcProcessTask::run() {
    if (server_ == nullptr) {
        return done();
    }

    const char* request = server_->request_data();
    const std::size_t request_size = server_->request_size();
    if (request_size == 4U && std::memcmp(request, "PING", 4U) == 0) {
        server_->set_response("PONG", 4U);
    } else {
        server_->set_response(request, request_size);
    }

    if (!rpc_async::post(RpcThread::IO_0, server_)) {
        return done();
    }
    return done();
}

} // namespace io_rpc_length_prefixed_example

#endif
