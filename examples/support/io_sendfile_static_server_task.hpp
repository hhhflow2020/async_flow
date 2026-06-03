#pragma once

#include <cerrno>
#include <cstdint>

#include "io_sendfile_static_runtime.hpp"

#if defined(__linux__)
#include <sys/socket.h>

namespace io_sendfile_static_example {

class StaticSendfileServerTask final : public SendfileTaskBase {
public:
    explicit StaticSendfileServerTask(SendfileTaskBase::FactoryToken token)
        : SendfileTaskBase(token) {}

    bool do_it(int listener_fd, int file_fd, SendfileServerResult *result) {
        if (listener_fd < 0 || file_fd < 0 || result == nullptr) {
            return false;
        }
        listener_.reset(SendfileThreads::IO_0, listener_fd);
        file_fd_ = file_fd;
        result_ = result;
        return schedule(SendfileThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        AcceptClient,
        SendFile,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::AcceptClient:
            return accept_client();
        case State::SendFile:
            return send_next_chunk();
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
        stream_.reset(SendfileThreads::IO_0, accepted_.get());
        state_ = State::SendFile;
        return again();
    }

    af::TaskResult send_next_chunk() {
        if (sent_ >= sendfile_payload_size) {
            result_->ok = true;
            result_->error = 0;
            result_->bytes_sent = sent_;
            return done();
        }

        const std::size_t remaining = sendfile_payload_size - sent_;
        const std::size_t count = remaining < chunk_size ? remaining : chunk_size;
        const af::IoStatus status = stream_.sendfile_some(*this, file_fd_, &offset_, count, send_);
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
    af::TcpListener<SendfileThread> listener_{};
    af::TcpStream<SendfileThread> stream_{};
    af::UniqueFd accepted_{};
    int accepted_fd_{-1};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    std::size_t sent_{0};
    af::IoOpState accept_{};
    af::IoOpState send_{};
    SendfileServerResult *result_{nullptr};
};

} // namespace io_sendfile_static_example

#else

namespace io_sendfile_static_example {

class StaticSendfileServerTask final : public SendfileTaskBase {
public:
    explicit StaticSendfileServerTask(SendfileTaskBase::FactoryToken token)
        : SendfileTaskBase(token) {}

    bool do_it(int listener_fd, int file_fd, SendfileServerResult *result) {
        static_cast<void>(listener_fd);
        static_cast<void>(file_fd);
        static_cast<void>(result);
        return false;
    }

private:
    af::TaskResult run() override {
        return failed();
    }
};

} // namespace io_sendfile_static_example

#endif
