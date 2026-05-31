#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "io_tcp_echo_runtime.hpp"

#if !defined(_WIN32)

namespace io_tcp_echo_example {

class EchoSessionTask final : public EchoTask {
public:
    explicit EchoSessionTask(EchoTask::FactoryToken token) : EchoTask(token) {}

    bool do_it(
        af::UniqueFd fd,
        EchoThread io_thread,
        EchoSessionResult* result) {
        fd_ = std::move(fd);
        if (!fd_ || result == nullptr) {
            return false;
        }

        io_thread_ = io_thread;
        result_ = result;
        result_->io_thread = io_thread_;
        stream_.reset(io_thread_, fd_.get());
        return schedule(io_thread_);
    }

private:
    enum class State : std::uint8_t {
        Receive,
        Compute,
        Send,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Receive:
            return receive_request();
        case State::Compute:
            return compute_response();
        case State::Send:
            return send_response();
        }
        return finish(EIO);
    }

    af::TaskResult receive_request() {
        const af::IoStatus status = stream_.recv_some(
            *this,
            payload_.data() + received_,
            payload_.size() - received_,
            read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return finish(status.failed() ? status.error : EIO);
        }

        received_ += status.bytes;
        if (received_ < payload_.size()) {
            return again();
        }

        state_ = State::Compute;
        return pending_on(EchoThread::Compute_0);
    }

    af::TaskResult compute_response() {
        lowercase_ascii(payload_);
        state_ = State::Send;
        return pending_on(io_thread_);
    }

    af::TaskResult send_response() {
        const af::IoStatus status = stream_.send_some(
            *this,
            payload_.data() + sent_,
            payload_.size() - sent_,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return finish(status.failed() ? status.error : EIO);
        }

        sent_ += status.bytes;
        if (sent_ < payload_.size()) {
            return again();
        }

        result_->ok = true;
        result_->error = 0;
        return done();
    }

    af::TaskResult finish(int error) {
        result_->ok = false;
        result_->error = error == 0 ? EIO : error;
        return failed();
    }

    State state_{State::Receive};
    EchoThread io_thread_{EchoThread::IO_0};
    af::UniqueFd fd_{};
    af::TcpStream<EchoThread> stream_{};
    EchoPayload payload_{};
    std::size_t received_{0};
    std::size_t sent_{0};
    af::IoOpState read_{};
    af::IoOpState write_{};
    EchoSessionResult* result_{nullptr};
};

} // namespace io_tcp_echo_example

#endif
