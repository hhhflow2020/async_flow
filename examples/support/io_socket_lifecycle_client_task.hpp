#pragma once

#include <cerrno>
#include <cstdint>

#include "io_socket_lifecycle_flags.hpp"
#include "io_socket_lifecycle_runtime.hpp"

#include <netinet/in.h>
#include <sys/socket.h>

namespace io_socket_lifecycle_example {

class SocketLifecycleClientTask final : public SocketTask {
public:
    explicit SocketLifecycleClientTask(SocketTask::FactoryToken token) : SocketTask(token) {}

    bool do_it(sockaddr_in server, SocketLifecycleClientResult *result) {
        if (result == nullptr) {
            return false;
        }
        server_ = server;
        result_ = result;
        return schedule(SocketThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        CreateSocket,
        FinishSocket,
        Connect,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::CreateSocket:
            return create_socket();
        case State::FinishSocket:
            return finish_socket();
        case State::Connect:
            return connect();
        }
        return finish(EIO);
    }

    af::TaskResult create_socket() {
        state_ = State::FinishSocket;
        return consume_socket_status(af::io_socket(
            *this, SocketThreads::IO_0, AF_INET, lifecycle_stream_socket_type(), 0, &fd_, socket_));
    }

    af::TaskResult finish_socket() {
        return consume_socket_status(af::io_socket(
            *this, SocketThreads::IO_0, AF_INET, lifecycle_stream_socket_type(), 0, &fd_, socket_));
    }

    af::TaskResult consume_socket_status(af::IoStatus status) {
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd_ < 0) {
            return finish(status.failed() ? status.error : EIO);
        }

        owned_.reset(fd_);
        fd_ = -1;
        int flag_error = 0;
        if (!apply_lifecycle_socket_flags(owned_.get(), flag_error)) {
            return finish(flag_error);
        }
        stream_.reset(SocketThreads::IO_0, owned_.get());
        state_ = State::Connect;
        return again();
    }

    af::TaskResult connect() {
        const af::IoStatus status = stream_.connect(
            *this, reinterpret_cast<const sockaddr *>(&server_), sizeof(server_), connect_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return finish(status.failed() ? status.error : EIO);
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

    State state_{State::CreateSocket};
    sockaddr_in server_{};
    int fd_{-1};
    af::UniqueFd owned_{};
    af::TcpStream<SocketThread> stream_{};
    af::IoOpState socket_{};
    af::IoOpState connect_{};
    SocketLifecycleClientResult *result_{nullptr};
};

} // namespace io_socket_lifecycle_example
