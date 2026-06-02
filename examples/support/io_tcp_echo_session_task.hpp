#pragma once

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "io_tcp_echo_server_state.hpp"

#if !defined(_WIN32)

namespace io_tcp_echo_example {

class EchoSessionTask final : public EchoTask {
public:
    explicit EchoSessionTask(EchoTask::FactoryToken token) : EchoTask(token) {}

    bool do_it(af::UniqueFd fd, EchoThread io_thread, EchoServerState *state) {
        fd_ = std::move(fd);
        if (!fd_ || state == nullptr) {
            return false;
        }

        io_thread_ = io_thread;
        server_state_ = state;
        stream_.reset(io_thread_, fd_.get());
        server_state_->active_sessions.fetch_add(1, std::memory_order_acq_rel);
        if (!schedule(io_thread_)) {
            server_state_->active_sessions.fetch_sub(1, std::memory_order_acq_rel);
            return false;
        }
        return true;
    }

private:
    enum class State : std::uint8_t {
        Receive,
        Send,
    };

    af::TaskResult run() override {
        switch (phase_) {
        case State::Receive:
            return receive();
        case State::Send:
            return send();
        }
        return finish(EIO);
    }

    af::TaskResult receive() {
        const af::IoStatus status = stream_.recv_some(*this, buffer_.data(), buffer_.size(), read_);
        if (status.pending()) {
            return pending();
        }
        if (status.closed()) {
            return finish(0);
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }

        available_ = status.bytes;
        sent_ = 0;
        session_bytes_received_ += status.bytes;
        phase_ = State::Send;
        return again();
    }

    af::TaskResult send() {
        const af::IoStatus status =
            stream_.send_some(*this, buffer_.data() + sent_, available_ - sent_, write_);
        if (status.pending()) {
            return pending();
        }
        if (status.closed()) {
            return finish(EPIPE);
        }
        if (!status.ready() || status.bytes == 0U) {
            return finish(status.failed() ? status.error : EIO);
        }

        sent_ += status.bytes;
        session_bytes_sent_ += status.bytes;
        if (sent_ < available_) {
            return again();
        }

        phase_ = State::Receive;
        return again();
    }

    af::TaskResult finish(int error) {
        server_state_->bytes_received.fetch_add(session_bytes_received_, std::memory_order_relaxed);
        server_state_->bytes_sent.fetch_add(session_bytes_sent_, std::memory_order_relaxed);
        server_state_->active_sessions.fetch_sub(1, std::memory_order_acq_rel);

        if (error == 0) {
            server_state_->completed_sessions.fetch_add(1, std::memory_order_acq_rel);
            LOG(INFO) << "tcp echo session closed bytes_in=" << session_bytes_received_
                      << " bytes_out=" << session_bytes_sent_;
            return done();
        }

        server_state_->failed_sessions.fetch_add(1, std::memory_order_acq_rel);
        LOG(ERROR) << "tcp echo session failed error=" << error
                   << " bytes_in=" << session_bytes_received_
                   << " bytes_out=" << session_bytes_sent_;
        return failed();
    }

    State phase_{State::Receive};
    EchoThread io_thread_{EchoThreads::IO_0};
    af::UniqueFd fd_{};
    af::TcpStream<EchoThread> stream_{};
    std::array<char, echo_session_buffer_size> buffer_{};
    std::size_t available_{0};
    std::size_t sent_{0};
    std::uint64_t session_bytes_received_{0};
    std::uint64_t session_bytes_sent_{0};
    af::IoOpState read_{};
    af::IoOpState write_{};
    EchoServerState *server_state_{nullptr};
};

} // namespace io_tcp_echo_example

#endif
