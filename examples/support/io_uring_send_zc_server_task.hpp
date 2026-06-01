#pragma once

#include <cerrno>
#include <cstdint>

#include "io_uring_send_zc_runtime.hpp"

#if defined(__linux__)
#include <sys/socket.h>

namespace io_uring_send_zc_example {

class SendZcServerTask final : public SendZcTaskBase {
public:
    explicit SendZcServerTask(SendZcTaskBase::FactoryToken token) : SendZcTaskBase(token) {}

    bool do_it(int listener_fd, SendZcServerResult *result) {
        if (listener_fd < 0 || result == nullptr) {
            return false;
        }
        listener_.reset(SendZcThread::IO_0, listener_fd);
        result_ = result;
        return schedule(SendZcThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        AcceptClient,
        SendPayload,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::AcceptClient:
            return accept_client();
        case State::SendPayload:
            return send_next();
        }
        return finish(EIO);
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

        accepted_.reset(accepted_fd_);
        accepted_fd_ = -1;
        stream_.reset(SendZcThread::IO_0, accepted_.get());
        state_ = State::SendPayload;
        return again();
    }

    af::TaskResult send_next() {
        if (sent_ >= send_zc_payload_size) {
            result_->ok = true;
            result_->error = 0;
            result_->bytes_sent = sent_;
            return done();
        }

        const std::size_t remaining = send_zc_payload_size - sent_;
        const std::size_t count = remaining < chunk_size ? remaining : chunk_size;
        const af::IoStatus status =
            stream_.send_zc_some(*this, send_zc_payload + sent_, count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > remaining) {
            return finish(status.failed() ? status.error : EIO);
        }

        sent_ += status.bytes;
        result_->bytes_sent = sent_;
        return again();
    }

    af::TaskResult finish(int error) {
        result_->ok = false;
        result_->error = error == 0 ? EIO : error;
        result_->bytes_sent = sent_;
        return failed();
    }

    static constexpr std::size_t chunk_size = 4096;

    State state_{State::AcceptClient};
    af::TcpListener<SendZcThread> listener_{};
    af::TcpStream<SendZcThread> stream_{};
    af::UniqueFd accepted_{};
    int accepted_fd_{-1};
    std::size_t sent_{0};
    af::IoOpState accept_{};
    af::IoOpState send_{};
    SendZcServerResult *result_{nullptr};
};

} // namespace io_uring_send_zc_example

#endif
