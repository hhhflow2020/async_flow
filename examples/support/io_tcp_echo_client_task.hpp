#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "io_tcp_echo_runtime.hpp"

#include <netinet/in.h>
#include <sys/socket.h>

namespace io_tcp_echo_example {

class EchoClientTask final : public EchoTask {
public:
    explicit EchoClientTask(EchoTask::FactoryToken token) : EchoTask(token) {}

    bool do_it(af::UniqueFd fd, EchoThread io_thread, sockaddr_in server, socklen_t server_size,
               EchoPayload request, EchoClientResult *result, std::uint64_t client_id) {
        fd_ = std::move(fd);
        if (!fd_ || result == nullptr) {
            return false;
        }

        io_thread_ = io_thread;
        server_ = server;
        server_size_ = server_size;
        request_ = request;
        result_ = result;
        client_id_ = client_id;
        result_->io_thread = io_thread_;
        stream_.reset(io_thread_, fd_.get());
        const bool scheduled = schedule(io_thread_);
        if (scheduled) {
            LOG(INFO) << "tcp echo client task started client=" << client_id_ << " fd=" << fd_.get()
                      << " io_thread=" << echo_async::thread_index(io_thread_);
        }
        return scheduled;
    }

private:
    enum class State : std::uint8_t {
        Connect,
        Send,
        Receive,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Connect:
            return connect();
        case State::Send:
            return send_request();
        case State::Receive:
            return receive_response();
        }
        return finish(EIO);
    }

    af::TaskResult connect() {
        const af::IoStatus status = stream_.connect(
            *this, reinterpret_cast<const sockaddr *>(&server_), server_size_, connect_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
        }

        LOG(INFO) << "tcp echo client connected client=" << client_id_
                  << " io_thread=" << echo_async::current_thread_index();
        state_ = State::Send;
        return again();
    }

    af::TaskResult send_request() {
        const af::IoStatus status =
            stream_.send_some(*this, request_.data() + sent_, request_.size() - sent_, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return finish(status.failed() ? status.error : EIO);
        }

        sent_ += status.bytes;
        if (sent_ < request_.size()) {
            return again();
        }

        LOG(INFO) << "tcp echo client sent request client=" << client_id_ << " bytes=" << sent_
                  << " io_thread=" << echo_async::current_thread_index();
        state_ = State::Receive;
        return again();
    }

    af::TaskResult receive_response() {
        const af::IoStatus status =
            stream_.recv_some(*this, result_->response.data() + result_->received,
                              result_->response.size() - result_->received, read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return finish(status.failed() ? status.error : EIO);
        }

        result_->received += status.bytes;
        if (result_->received < result_->response.size()) {
            return again();
        }

        result_->ok = true;
        result_->error = 0;
        result_->completed.store(true, std::memory_order_release);
        LOG(INFO) << "tcp echo client received response client=" << client_id_
                  << " bytes=" << result_->received
                  << " io_thread=" << echo_async::current_thread_index();
        LOG(INFO) << "tcp echo client task ended client=" << client_id_ << " error=0";
        return done();
    }

    af::TaskResult finish(int error) {
        result_->ok = false;
        result_->error = error == 0 ? EIO : error;
        result_->completed.store(true, std::memory_order_release);
        LOG(ERROR) << "tcp echo client task ended client=" << client_id_
                   << " error=" << result_->error;
        return failed();
    }

    State state_{State::Connect};
    EchoThread io_thread_{EchoThreads::IO_0};
    af::UniqueFd fd_{};
    af::TcpStream<EchoThread> stream_{};
    sockaddr_in server_{};
    socklen_t server_size_{sizeof(server_)};
    EchoPayload request_{};
    std::size_t sent_{0};
    std::uint64_t client_id_{0};
    af::IoOpState connect_{};
    af::IoOpState write_{};
    af::IoOpState read_{};
    EchoClientResult *result_{nullptr};
};

} // namespace io_tcp_echo_example
