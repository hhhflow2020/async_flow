#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

enum class SocketThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct SocketRuntimeTraits {
    using Thread = SocketThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(SocketThread::enum_thread_index_end);
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(SocketThread thread) noexcept {
        return thread == SocketThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using socket_async = af::AsyncRuntime<SocketRuntimeTraits>;
using SocketTask = socket_async::Task;

#if defined(__linux__)
bool wait_until(std::atomic<int>& value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

class SocketLifecycleTask final : public SocketTask {
public:
    explicit SocketLifecycleTask(SocketTask::FactoryToken token) : SocketTask(token) {}

    bool do_it(
        sockaddr_in* bound_address,
        std::atomic<int>* ready,
        std::atomic<int>* accepted,
        std::atomic<int>* error) {
        bound_address_ = bound_address;
        ready_ = ready;
        accepted_ = accepted;
        error_ = error;
        return schedule(SocketThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        CreateListener,
        FinishListener,
        AcceptClient,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::CreateListener:
            return create_listener();
        case State::FinishListener:
            return finish_listener();
        case State::AcceptClient:
            return accept_client();
        }
        return failed();
    }

    af::TaskResult create_listener() {
        state_ = State::FinishListener;
        return consume_listener_status(af::io_socket(
            *this,
            SocketThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &listener_fd_,
            socket_));
    }

    af::TaskResult finish_listener() {
        return consume_listener_status(af::io_socket(
            *this,
            SocketThread::IO_0,
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0,
            &listener_fd_,
            socket_));
    }

    af::TaskResult consume_listener_status(const af::IoStatus status) {
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || listener_fd_ < 0) {
            return complete(status.failed() ? status.error : EIO);
        }
        listener_owned_.reset(listener_fd_);
        listener_fd_ = -1;
        listener_.reset(SocketThread::IO_0, listener_owned_.get());
        return configure_listener();
    }

    af::TaskResult configure_listener() {
        const int one = 1;
        af::IoStatus status = listener_.setsockopt(
            *this,
            SOL_SOCKET,
            SO_REUSEADDR,
            &one,
            sizeof(one));
        if (!status.ready()) {
            return complete(status.error);
        }

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        status = listener_.bind(
            *this,
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address));
        if (!status.ready()) {
            return complete(status.error);
        }

        status = listener_.listen(*this, 16);
        if (!status.ready()) {
            return complete(status.error);
        }

        socklen_t address_size = sizeof(*bound_address_);
        if (::getsockname(
                listener_owned_.get(),
                reinterpret_cast<sockaddr*>(bound_address_),
                &address_size) != 0) {
            return complete(errno == 0 ? EIO : errno);
        }

        state_ = State::AcceptClient;
        ready_->fetch_add(1, std::memory_order_release);
        return accept_client();
    }

    af::TaskResult accept_client() {
        const af::IoStatus status = listener_.accept_some(
            *this,
            nullptr,
            nullptr,
            &accepted_fd_,
            accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || accepted_fd_ < 0) {
            return complete(status.failed() ? status.error : EIO);
        }

        accepted_owned_.reset(accepted_fd_);
        accepted_fd_ = -1;
        accepted_->fetch_add(1, std::memory_order_release);
        return complete(0);
    }

    af::TaskResult complete(int error) {
        error_->store(error, std::memory_order_release);
        return done();
    }

    State state_{State::CreateListener};
    af::IoOpState socket_{};
    af::IoOpState accept_{};
    int listener_fd_{-1};
    int accepted_fd_{-1};
    af::UniqueFd listener_owned_{};
    af::UniqueFd accepted_owned_{};
    af::TcpListener<SocketThread> listener_{};
    sockaddr_in* bound_address_{nullptr};
    std::atomic<int>* ready_{nullptr};
    std::atomic<int>* accepted_{nullptr};
    std::atomic<int>* error_{nullptr};
};
#endif

int main() {
#if defined(__linux__)
    socket_async::init();
    if (!socket_async::io_backend_available(SocketThread::IO_0)) {
        std::cout << "socket lifecycle backend unavailable\n";
        socket_async::shutdown();
        return 0;
    }

    sockaddr_in address{};
    std::atomic<int> ready{0};
    std::atomic<int> accepted{0};
    std::atomic<int> error{-1};
    if (!socket_async::start_task<SocketLifecycleTask>(&address, &ready, &accepted, &error)) {
        std::cerr << "failed to start socket lifecycle task\n";
        socket_async::shutdown();
        return 1;
    }
    if (!wait_until(ready, 1)) {
        std::cerr << "socket lifecycle listener was not ready\n";
        socket_async::shutdown();
        return 1;
    }

    af::UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!client) {
        std::cerr << "client socket failed\n";
        socket_async::shutdown();
        return 1;
    }
    const int rc = ::connect(
        client.get(),
        reinterpret_cast<const sockaddr*>(&address),
        sizeof(address));
    if (rc != 0 && errno != EINPROGRESS) {
        std::cerr << "client connect failed: " << errno << '\n';
        socket_async::shutdown();
        return 1;
    }

    if (!wait_until(accepted, 1)) {
        std::cerr << "socket lifecycle accept timed out\n";
        socket_async::shutdown();
        return 1;
    }

    const bool has_uring = socket_async::io_uring_backend_available(SocketThread::IO_0);
    std::cout << "socket lifecycle backend="
              << (has_uring ? "io_uring" : "epoll")
              << " port=" << ntohs(address.sin_port)
              << " error=" << error.load(std::memory_order_acquire) << '\n';
    socket_async::shutdown();
    return error.load(std::memory_order_acquire) == 0 ? 0 : 1;
#else
    std::cout << "socket lifecycle example is Linux-only\n";
    return 0;
#endif
}
