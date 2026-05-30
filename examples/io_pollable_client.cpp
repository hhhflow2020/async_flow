#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

enum class ClientThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct ClientRuntimeTraits {
    using Thread = ClientThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(ClientThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(ClientThread thread) noexcept {
        return thread == ClientThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
    }
};

using client_async = af::AsyncRuntime<ClientRuntimeTraits>;
using Task = client_async::Task;

#if defined(__linux__)
struct PollableStep {
    std::uint32_t want{0};
    bool done{false};
    int error{0};
};

// This class emulates a "third-party" nonblocking client API:
// call step(); if it returns want!=0 then the caller must wait for readiness
// on fd() and call step() again on resume.
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

    [[nodiscard]] const char* response_data() const noexcept {
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
            const ssize_t n = ::send(
                fd_,
                request_ + sent_,
                sizeof(request_) - sent_,
                MSG_NOSIGNAL);
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
            const ssize_t n = ::recv(
                fd_,
                response_ + received_,
                sizeof(response_) - received_,
                0);
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

constexpr char PollableEchoClient::request_[4];

class PollableClientTask final : public Task {
public:
    explicit PollableClientTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd) {
        client_.reset(fd);
        state_ = State::Drive;
        wait_.reset();
        return schedule(ClientThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Drive,
        Wait,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Drive:
            return drive();
        case State::Wait:
            return drive();
        }
        return failed();
    }

    af::TaskResult drive() {
        for (;;) {
            const PollableStep step = client_.step();
            if (step.error != 0) {
                std::cout << "pollable client failed: " << step.error << '\n';
                return failed();
            }
            if (step.done) {
                std::cout << "pollable client response="
                          << std::string_view(client_.response_data(), client_.response_size())
                          << '\n';
                return done();
            }
            if (step.want == 0U) {
                continue;
            }

            state_ = State::Wait;
            if (!wait_io(ClientThread::IO_0, client_.fd(), step.want, &wait_.wait)) {
                std::cout << "io_wait failed: " << wait_.wait.error << '\n';
                return failed();
            }
            wait_.waiting = true;
            wait_.wait_kind = af::IoWaitKind::Readiness;
            return pending();
        }
    }

    State state_{State::Drive};
    PollableEchoClient client_{};
    af::IoOpState wait_{};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    client_async::init();
    if (!client_async::io_backend_available(ClientThread::IO_0)) {
        std::cout << "epoll backend unavailable\n";
        client_async::shutdown();
        return 0;
    }

    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        std::cout << "socketpair failed\n";
        client_async::shutdown();
        return 1;
    }

    const bool started = client_async::start_task<PollableClientTask>(fds[0]);
    AF_ASSERT(started);
    if (!started) {
        ::close(fds[0]);
        ::close(fds[1]);
        client_async::shutdown();
        return 1;
    }

    // Peer side: echo exactly 4 bytes back.
    char request[4]{};
    std::size_t read_bytes = 0;
    const auto read_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (read_bytes < sizeof(request) && std::chrono::steady_clock::now() < read_deadline) {
        const ssize_t n = ::read(fds[1], request + read_bytes, sizeof(request) - read_bytes);
        if (n > 0) {
            read_bytes += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        break;
    }
    if (read_bytes == sizeof(request)) {
        std::size_t written = 0;
        const auto write_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (written < sizeof(request)) {
            if (std::chrono::steady_clock::now() >= write_deadline) {
                break;
            }
            const ssize_t n = ::write(fds[1], request + written, sizeof(request) - written);
            if (n > 0) {
                written += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR) {
                continue;
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            break;
        }
    }

    client_async::shutdown();

    ::close(fds[0]);
    ::close(fds[1]);
    return 0;
#else
    std::cout << "pollable client example is Linux-only\n";
    return 0;
#endif
}
