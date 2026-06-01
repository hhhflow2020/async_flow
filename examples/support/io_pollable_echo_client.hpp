#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "io_pollable_client_runtime.hpp"

#if defined(__linux__)
#include <sys/socket.h>
#endif

namespace io_pollable_client_example {

#if defined(__linux__)

struct PollableStep {
    std::uint32_t want{0};
    bool done{false};
    int error{0};
};

class PollableEchoClient {
public:
    PollableEchoClient() = default;
    explicit PollableEchoClient(int fd) : fd_(fd) {}

    void reset(int fd) noexcept {
        fd_ = fd;
        state_ = State::Send;
        sent_ = 0;
        received_ = 0;
        last_error_ = 0;
        std::memset(response_, 0, sizeof(response_));
    }

    [[nodiscard]] int fd() const noexcept {
        return fd_;
    }

    [[nodiscard]] const char *response_data() const noexcept {
        return response_;
    }

    [[nodiscard]] std::size_t response_size() const noexcept {
        return received_;
    }

    [[nodiscard]] PollableStep step() noexcept {
        if (fd_ < 0) {
            return PollableStep{0, false, EBADF};
        }

        switch (state_) {
        case State::Send:
            return step_send();
        case State::Recv:
            return step_recv();
        case State::Done:
            return PollableStep{0, true, 0};
        case State::Error:
            return PollableStep{0, false, last_error_ == 0 ? EIO : last_error_};
        }
        return PollableStep{0, false, EIO};
    }

private:
    enum class State : std::uint8_t {
        Send,
        Recv,
        Done,
        Error,
    };

    PollableStep step_send() noexcept {
        while (sent_ < sizeof(request_)) {
            const ssize_t n = ::send(fd_, request_ + sent_, sizeof(request_) - sent_, MSG_NOSIGNAL);
            if (n > 0) {
                sent_ += static_cast<std::size_t>(n);
                continue;
            }
            if (n == 0) {
                last_error_ = EPIPE;
                state_ = State::Error;
                return PollableStep{0, false, last_error_};
            }

            const int err = errno;
            if (err == EINTR) {
                continue;
            }
            if (err == EAGAIN || err == EWOULDBLOCK) {
                return PollableStep{af::io_writable, false, 0};
            }
            last_error_ = err == 0 ? EIO : err;
            state_ = State::Error;
            return PollableStep{0, false, last_error_};
        }

        state_ = State::Recv;
        return PollableStep{0, false, 0};
    }

    PollableStep step_recv() noexcept {
        while (received_ < sizeof(response_)) {
            const ssize_t n = ::recv(fd_, response_ + received_, sizeof(response_) - received_, 0);
            if (n > 0) {
                received_ += static_cast<std::size_t>(n);
                continue;
            }
            if (n == 0) {
                last_error_ = ECONNRESET;
                state_ = State::Error;
                return PollableStep{0, false, last_error_};
            }

            const int err = errno;
            if (err == EINTR) {
                continue;
            }
            if (err == EAGAIN || err == EWOULDBLOCK) {
                return PollableStep{af::io_readable, false, 0};
            }
            last_error_ = err == 0 ? EIO : err;
            state_ = State::Error;
            return PollableStep{0, false, last_error_};
        }

        state_ = State::Done;
        return PollableStep{0, true, 0};
    }

    int fd_{-1};
    State state_{State::Send};
    std::size_t sent_{0};
    std::size_t received_{0};
    int last_error_{0};
    char response_[4]{};
    static constexpr char request_[4] = {'P', 'I', 'N', 'G'};
};

#endif

} // namespace io_pollable_client_example
