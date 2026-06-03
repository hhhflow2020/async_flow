#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>

#include "io_uring_accept_direct_runtime.hpp"

#if defined(__linux__)

namespace io_uring_accept_direct_example {

class DirectAcceptRoundTripTask final : public DirectAcceptTask {
public:
    explicit DirectAcceptRoundTripTask(DirectAcceptTask::FactoryToken token)
        : DirectAcceptTask(token) {}

    bool do_it(int listener_fd, std::atomic<int> *armed, std::atomic<int> *error,
               int *packed_read) {
        listener_.reset(DirectAcceptThreads::IO_0, listener_fd);
        armed_ = armed;
        error_ = error;
        packed_read_ = packed_read;
        return schedule(DirectAcceptThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Accept,
        Recv,
        Send,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_sparse_slot();

        case State::Accept:
            return accept_direct();

        case State::Recv:
            return recv_request();

        case State::Send:
            return send_response();

        case State::Unregister:
            return complete(0);
        }
        return complete(EIO);
    }

    af::TaskResult register_sparse_slot() {
        const int sparse = -1;
        int error = 0;
        if (!direct_accept_async::io_register_files(DirectAcceptThreads::IO_0, &sparse, 1,
                                                    &error)) {
            return complete(error == 0 ? EIO : error);
        }
        registered_ = true;
        state_ = State::Accept;
        return again();
    }

    af::TaskResult accept_direct() {
        const af::IoStatus status =
            listener_.accept_direct(*this, nullptr, nullptr, 0, &accepted_, accept_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (!status.ready()) {
            return complete(status.failed() ? status.error : EIO);
        }
        if (!accepted_.valid()) {
            return complete(EIO);
        }
        state_ = State::Recv;
        return again();
    }

    af::TaskResult recv_request() {
        request_iov_[0] = iovec{&request_[0], 1};
        request_iov_[1] = iovec{&request_[1], 1};
        const af::IoStatus status = accepted_.recvv_some(*this, request_iov_, 2, recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return complete(status.failed() ? status.error : EIO);
        }
        *packed_read_ = pack_request();
        state_ = State::Send;
        return again();
    }

    af::TaskResult send_response() {
        response_iov_[0] = iovec{&response_[0], 1};
        response_iov_[1] = iovec{&response_[1], 1};
        const af::IoStatus status = accepted_.sendv_some(*this, response_iov_, 2, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (!direct_accept_async::io_unregister_files(DirectAcceptThreads::IO_0,
                                                          &unregister_error) &&
                error == 0) {
                error = unregister_error == 0 ? EIO : unregister_error;
            }
            registered_ = false;
        }
        error_->store(error, std::memory_order_release);
        return done();
    }

    [[nodiscard]] int pack_request() const noexcept {
        return (static_cast<int>(static_cast<unsigned char>(request_[0])) << 8) |
               static_cast<int>(static_cast<unsigned char>(request_[1]));
    }

    State state_{State::Register};
    af::TcpListener<DirectAcceptThread> listener_{};
    af::IoFixedFile<DirectAcceptThread> accepted_{};
    char request_[2]{};
    char response_[2]{'O', 'K'};
    iovec request_iov_[2]{};
    iovec response_iov_[2]{};
    bool registered_{false};
    bool armed_once_{false};
    af::IoOpState accept_{};
    af::IoOpState recv_{};
    af::IoOpState send_{};
    std::atomic<int> *armed_{nullptr};
    std::atomic<int> *error_{nullptr};
    int *packed_read_{nullptr};
};

} // namespace io_uring_accept_direct_example

#else

namespace io_uring_accept_direct_example {

class DirectAcceptRoundTripTask final : public DirectAcceptTask {
public:
    explicit DirectAcceptRoundTripTask(DirectAcceptTask::FactoryToken token)
        : DirectAcceptTask(token) {}

    bool do_it(int listener_fd, std::atomic<int> *armed, std::atomic<int> *error,
               int *packed_read) {
        static_cast<void>(listener_fd);
        static_cast<void>(armed);
        static_cast<void>(error);
        static_cast<void>(packed_read);
        return false;
    }

private:
    af::TaskResult run() override {
        return failed();
    }
};

} // namespace io_uring_accept_direct_example

#endif
