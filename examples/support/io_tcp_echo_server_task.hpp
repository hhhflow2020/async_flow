#pragma once

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "io_tcp_echo_session_task.hpp"
#include "io_tcp_echo_sockets.hpp"

#if !defined(_WIN32)
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_tcp_echo_example {

class EchoServerTask final : public EchoTask {
public:
    explicit EchoServerTask(EchoTask::FactoryToken token) : EchoTask(token) {}

    bool do_it(int listener_fd, EchoServerState *state, std::uint64_t max_accepts = 0) {
        if (listener_fd < 0 || state == nullptr) {
            return false;
        }

        listener_.reset(EchoThreads::IO_0, listener_fd);
        state_ = state;
        max_accepts_ = max_accepts;
        const bool scheduled = schedule(EchoThreads::IO_0);
        if (scheduled) {
            LOG(INFO) << "tcp echo accept task started io_thread="
                      << echo_async::thread_index(EchoThreads::IO_0)
                      << " max_accepts=" << max_accepts_;
        }
        return scheduled;
    }

private:
    af::TaskResult run() override {
        if (state_->stop_requested.load(std::memory_order_acquire)) {
            return finish(0);
        }

        peer_size_ = sizeof(peer_);
        int accepted_fd = -1;
        const af::IoStatus status = listener_.accept_some(
            *this, reinterpret_cast<sockaddr *>(&peer_), &peer_size_, &accepted_fd, accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || accepted_fd < 0) {
            if (state_->stop_requested.load(std::memory_order_acquire)) {
                return finish(0);
            }
            return finish(status.failed() ? status.error : EIO);
        }

        af::UniqueFd accepted(accepted_fd);
        if (state_->stop_requested.load(std::memory_order_acquire)) {
            return finish(0);
        }

        echo_set_tcp_nodelay(accepted.get());
        echo_set_keepalive(accepted.get());

        const std::uint64_t accepted_index =
            state_->accepted.fetch_add(1, std::memory_order_acq_rel);
        const std::uint64_t session_id = accepted_index + 1U;
        const EchoThread io_thread = echo_io_thread(static_cast<std::size_t>(accepted_index));
        LOG(INFO) << "tcp echo server accepted connection session=" << session_id
                  << " fd=" << accepted.get()
                  << " io_thread=" << echo_async::thread_index(io_thread);
        LOG(INFO) << "tcp echo session task starting session=" << session_id
                  << " io_thread=" << echo_async::thread_index(io_thread);
        if (!echo_async::start_task<EchoSessionTask>(std::move(accepted), io_thread, state_,
                                                     session_id)) {
            state_->rejected.fetch_add(1, std::memory_order_acq_rel);
            LOG(ERROR) << "tcp echo server rejected session task session=" << session_id;
            if (max_accepts_ != 0 && session_id >= max_accepts_) {
                state_->stop_requested.store(true, std::memory_order_release);
                return finish(0);
            }
            return again();
        }
        LOG(INFO) << "tcp echo session task started session=" << session_id;

        if (max_accepts_ != 0 && session_id >= max_accepts_) {
            state_->stop_requested.store(true, std::memory_order_release);
            return finish(0);
        }
        return again();
    }

    af::TaskResult finish(int error) {
        echo_accept_finished(*state_, error);
        if (error == 0) {
            LOG(INFO) << "tcp echo accept task ended accepted="
                      << state_->accepted.load(std::memory_order_acquire)
                      << " rejected=" << state_->rejected.load(std::memory_order_acquire);
            return done();
        }
        LOG(ERROR) << "tcp echo accept task ended error=" << error;
        return failed();
    }

    af::TcpListener<EchoThread> listener_{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState accept_{};
    EchoServerState *state_{nullptr};
    std::uint64_t max_accepts_{0};
};

} // namespace io_tcp_echo_example

#endif
