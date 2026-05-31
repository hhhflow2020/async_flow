#pragma once

#include <cstdint>

#include "io_vectored_runtime.hpp"

#if defined(__linux__)

namespace io_vectored_example {

class ServerTask final : public VectoredTask {
public:
    explicit ServerTask(VectoredTask::FactoryToken token) : VectoredTask(token) {}

    bool do_it(int fd, bool* ok, int* request_seen) {
        stream_.reset(VectoredThread::IO_0, fd);
        ok_ = ok;
        request_seen_ = request_seen;
        return schedule(VectoredThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        ReadRequest,
        SendResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ReadRequest:
            return read_request();

        case State::SendResponse:
            return send_response();
        }
        return failed();
    }

    af::TaskResult read_request() {
        request_iov_[0] = iovec{&request_[0], 1};
        request_iov_[1] = iovec{&request_[1], 1};
        const af::IoStatus status = stream_.recvv_some(*this, request_iov_, 2, read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }

        const int combined =
            (static_cast<int>(request_[0]) << 8) | static_cast<unsigned char>(request_[1]);
        *request_seen_ = combined;
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        response_iov_[0] = iovec{&response_[0], 1};
        response_iov_[1] = iovec{&response_[1], 1};
        const af::IoStatus status = stream_.sendv_some(*this, response_iov_, 2, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        *ok_ = true;
        return done();
    }

    State state_{State::ReadRequest};
    af::TcpStream<VectoredThread> stream_{};
    char request_[2]{};
    char response_[2]{'O', 'K'};
    iovec request_iov_[2]{};
    iovec response_iov_[2]{};
    af::IoOpState read_{};
    af::IoOpState write_{};
    bool* ok_{nullptr};
    int* request_seen_{nullptr};
};

class ClientTask final : public VectoredTask {
public:
    explicit ClientTask(VectoredTask::FactoryToken token) : VectoredTask(token) {}

    bool do_it(int fd, bool* ok, int* response_seen) {
        stream_.reset(VectoredThread::IO_0, fd);
        ok_ = ok;
        response_seen_ = response_seen;
        return schedule(VectoredThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        SendRequest,
        ReadResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::SendRequest:
            return send_request();

        case State::ReadResponse:
            return read_response();
        }
        return failed();
    }

    af::TaskResult send_request() {
        request_iov_[0] = iovec{&request_[0], 1};
        request_iov_[1] = iovec{&request_[1], 1};
        const af::IoStatus status = stream_.sendv_some(*this, request_iov_, 2, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }

        state_ = State::ReadResponse;
        return again();
    }

    af::TaskResult read_response() {
        response_iov_[0] = iovec{&response_[0], 1};
        response_iov_[1] = iovec{&response_[1], 1};
        const af::IoStatus status = stream_.recvv_some(*this, response_iov_, 2, read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        const int combined =
            (static_cast<int>(response_[0]) << 8) | static_cast<unsigned char>(response_[1]);
        *response_seen_ = combined;
        *ok_ = true;
        return done();
    }

    State state_{State::SendRequest};
    af::TcpStream<VectoredThread> stream_{};
    char request_[2]{'H', 'I'};
    char response_[2]{};
    iovec request_iov_[2]{};
    iovec response_iov_[2]{};
    af::IoOpState write_{};
    af::IoOpState read_{};
    bool* ok_{nullptr};
    int* response_seen_{nullptr};
};

} // namespace io_vectored_example

#endif
