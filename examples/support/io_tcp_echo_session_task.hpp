#pragma once

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "io_tcp_echo_server_state.hpp"

namespace io_tcp_echo_example {

class EchoSessionTask final : public EchoTask {
public:
    explicit EchoSessionTask(EchoTask::FactoryToken token) : EchoTask(token) {}

    bool do_it(af::UniqueFd fd, EchoThread io_thread, EchoServerState *state,
               std::uint64_t session_id) {
        fd_ = std::move(fd);
        if (!fd_ || state == nullptr) {
            return false;
        }

        io_thread_ = io_thread;
        server_state_ = state;
        session_id_ = session_id;
        stream_.reset(io_thread_, fd_.get());
        const std::uint64_t active_sessions = echo_session_started(*server_state_);
        if (!schedule(io_thread_)) {
            echo_session_start_aborted(*server_state_);
            LOG(ERROR) << "tcp echo session task start failed session=" << session_id_;
            return false;
        }
        LOG(INFO) << "tcp echo session task started session=" << session_id_ << " fd=" << fd_.get()
                  << " io_thread=" << echo_async::thread_index(io_thread_)
                  << " active_sessions=" << active_sessions;
        return true;
    }

private:
    enum class State : std::uint8_t {
        Receive,
        Compute,
        Send,
    };

    af::TaskResult run() override {
        switch (phase_) {
        case State::Receive:
            return receive();
        case State::Compute:
            return compute();
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
        if (status.bytes == 0U) {
            return finish(0);
        }

        available_ = status.bytes;
        sent_ = 0;
        session_bytes_received_ += status.bytes;
        LOG(INFO) << "tcp echo session received session=" << session_id_
                  << " bytes=" << status.bytes << " total_in=" << session_bytes_received_
                  << " io_thread=" << echo_async::current_thread_index()
                  << " compute_thread=" << echo_async::thread_index(EchoThreads::Compute_0);
        phase_ = State::Compute;
        return pending_on(EchoThreads::Compute_0);
    }

    af::TaskResult compute() {
        const std::size_t converted = echo_lowercase_ascii(buffer_.data(), available_);
        LOG(INFO) << "tcp echo session lowercase converted session=" << session_id_
                  << " bytes=" << available_ << " uppercase_converted=" << converted
                  << " compute_thread=" << echo_async::current_thread_index()
                  << " io_thread=" << echo_async::thread_index(io_thread_);
        phase_ = State::Send;
        return pending_on(io_thread_);
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
        LOG(INFO) << "tcp echo session sent session=" << session_id_ << " bytes=" << status.bytes
                  << " total_out=" << session_bytes_sent_
                  << " io_thread=" << echo_async::current_thread_index();
        if (sent_ < available_) {
            return again();
        }

        phase_ = State::Receive;
        return again();
    }

    af::TaskResult finish(int error) {
        if (error == 0) {
            echo_session_finished(*server_state_, true, session_bytes_received_,
                                  session_bytes_sent_);
            LOG(INFO) << "tcp echo session disconnected session=" << session_id_
                      << " bytes_in=" << session_bytes_received_
                      << " bytes_out=" << session_bytes_sent_;
            LOG(INFO) << "tcp echo session task ended session=" << session_id_ << " error=0";
            return done();
        }

        echo_session_finished(*server_state_, false, session_bytes_received_, session_bytes_sent_);
        LOG(ERROR) << "tcp echo session task ended session=" << session_id_ << " error=" << error
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
    std::uint64_t session_id_{0};
    af::IoOpState read_{};
    af::IoOpState write_{};
    EchoServerState *server_state_{nullptr};
};

} // namespace io_tcp_echo_example
