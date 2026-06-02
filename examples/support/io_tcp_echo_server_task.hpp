#pragma once

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "io_tcp_echo_session_task.hpp"

#if !defined(_WIN32)
#include <netinet/in.h>
#include <sys/socket.h>

namespace io_tcp_echo_example {

class EchoServerTask final : public EchoTask {
public:
    explicit EchoServerTask(EchoTask::FactoryToken token) : EchoTask(token) {}

    bool do_it(int listener_fd, EchoSessionResult *sessions, std::size_t session_count, bool *ok,
               int *error, std::atomic<bool> *completed) {
        if (listener_fd < 0 || sessions == nullptr || session_count == 0U || ok == nullptr ||
            error == nullptr || completed == nullptr) {
            return false;
        }

        listener_.reset(EchoThreads::IO_0, listener_fd);
        sessions_ = sessions;
        session_count_ = session_count;
        ok_ = ok;
        error_ = error;
        completed_ = completed;
        return schedule(EchoThreads::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        int accepted_fd = -1;
        const af::IoStatus status = listener_.accept_some(
            *this, reinterpret_cast<sockaddr *>(&peer_), &peer_size_, &accepted_fd, accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || accepted_fd < 0) {
            return finish(status.failed() ? status.error : EIO);
        }

        af::UniqueFd accepted(accepted_fd);
        const EchoThread io_thread = echo_io_thread(accepted_count_);
        LOG(INFO) << "tcp echo server accepted session=" << accepted_count_
                  << " io_thread=" << echo_async::thread_index(io_thread);
        if (!echo_async::start_task<EchoSessionTask>(std::move(accepted), io_thread,
                                                     &sessions_[accepted_count_])) {
            return finish(EAGAIN);
        }

        ++accepted_count_;
        if (accepted_count_ < session_count_) {
            return again();
        }

        *ok_ = true;
        *error_ = 0;
        completed_->store(true, std::memory_order_release);
        LOG(INFO) << "tcp echo server accepted all sessions count=" << accepted_count_;
        return done();
    }

    af::TaskResult finish(int error) {
        *ok_ = false;
        *error_ = error == 0 ? EIO : error;
        completed_->store(true, std::memory_order_release);
        LOG(ERROR) << "tcp echo server failed error=" << *error_;
        return failed();
    }

    af::TcpListener<EchoThread> listener_{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState accept_{};
    EchoSessionResult *sessions_{nullptr};
    std::size_t session_count_{0};
    std::size_t accepted_count_{0};
    bool *ok_{nullptr};
    int *error_{nullptr};
    std::atomic<bool> *completed_{nullptr};
};

} // namespace io_tcp_echo_example

#endif
