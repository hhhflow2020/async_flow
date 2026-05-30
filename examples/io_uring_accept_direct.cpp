#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace {

enum class DirectAcceptThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct DirectAcceptRuntimeTraits {
    using Thread = DirectAcceptThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(DirectAcceptThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(DirectAcceptThread thread) noexcept {
        return thread == DirectAcceptThread::IO_0 ? af::ThreadKind::IoUring
                                                  : af::ThreadKind::Worker;
    }
};

using direct_accept_async = af::AsyncRuntime<DirectAcceptRuntimeTraits>;
using DirectAcceptTask = direct_accept_async::Task;

bool wait_until_armed_or_error(std::atomic<int>& armed, std::atomic<int>& error) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (armed.load(std::memory_order_acquire) == 0 &&
           error.load(std::memory_order_acquire) == 0) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

#if defined(__linux__)
[[nodiscard]] bool unsupported_direct_accept_error(int error) noexcept {
    return error == EINVAL || error == EBADF || error == ENOSYS || error == ENXIO
#ifdef EOPNOTSUPP
        || error == EOPNOTSUPP
#endif
        ;
}

bool create_loopback_listener(af::UniqueFd& listener, sockaddr_in& address) {
    listener.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!listener) {
        return false;
    }

    const int one = 1;
    static_cast<void>(::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)));

    address = sockaddr_in{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
        ::listen(listener.get(), 16) != 0) {
        return false;
    }

    socklen_t address_size = sizeof(address);
    return ::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address), &address_size) == 0;
}

bool write_exact_until(int fd, const char* input, std::size_t size) {
    std::size_t offset = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (offset < size) {
        const ssize_t n = ::write(fd, input + offset, size - offset);
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != ENOTCONN) {
            return false;
        }
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

bool read_exact_until(int fd, char* output, std::size_t size) {
    std::size_t offset = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (offset < size) {
        const ssize_t n = ::read(fd, output + offset, size - offset);
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0) {
            return false;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

class DirectAcceptRoundTripTask final : public DirectAcceptTask {
public:
    explicit DirectAcceptRoundTripTask(DirectAcceptTask::FactoryToken token)
        : DirectAcceptTask(token) {}

    bool do_it(
        int listener_fd,
        std::atomic<int>* armed,
        std::atomic<int>* error,
        int* packed_read) {
        listener_.reset(DirectAcceptThread::IO_0, listener_fd);
        armed_ = armed;
        error_ = error;
        packed_read_ = packed_read;
        return schedule(DirectAcceptThread::IO_0);
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
        if (!direct_accept_async::io_register_files(
                DirectAcceptThread::IO_0,
                &sparse,
                1,
                &error)) {
            return complete(error == 0 ? EIO : error);
        }
        registered_ = true;
        state_ = State::Accept;
        return again();
    }

    af::TaskResult accept_direct() {
        const af::IoStatus status = listener_.accept_direct(
            *this,
            nullptr,
            nullptr,
            0,
            &accepted_,
            accept_);
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
        const af::IoStatus status =
            accepted_.recvv_some(*this, request_iov_, 2, recv_);
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
        const af::IoStatus status =
            accepted_.sendv_some(*this, response_iov_, 2, send_);
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
            if (!direct_accept_async::io_unregister_files(
                    DirectAcceptThread::IO_0,
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
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* error_{nullptr};
    int* packed_read_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    direct_accept_async::init();
    if (!direct_accept_async::io_uring_backend_available(DirectAcceptThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        direct_accept_async::shutdown();
        return 0;
    }

    af::UniqueFd listener;
    sockaddr_in address{};
    if (!create_loopback_listener(listener, address)) {
        std::cerr << "listener setup failed\n";
        direct_accept_async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    std::atomic<int> error{0};
    int packed_read = 0;
    const bool started = direct_accept_async::start_task<DirectAcceptRoundTripTask>(
        listener.get(),
        &armed,
        &error,
        &packed_read);
    AF_ASSERT(started);
    if (!started) {
        direct_accept_async::shutdown();
        return 1;
    }

    if (!wait_until_armed_or_error(armed, error)) {
        std::cerr << "io_uring accept direct task did not arm\n";
        direct_accept_async::shutdown();
        return 1;
    }
    if (armed.load(std::memory_order_acquire) == 0) {
        const int task_error = error.load(std::memory_order_acquire);
        std::cout << "io_uring accept direct "
                  << (unsupported_direct_accept_error(task_error) ? "unsupported" : "failed")
                  << " error=" << task_error << '\n';
        direct_accept_async::shutdown();
        return unsupported_direct_accept_error(task_error) ? 0 : 1;
    }

    af::UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!client) {
        std::cerr << "client socket failed\n";
        direct_accept_async::shutdown();
        return 1;
    }
    const int connect_rc = ::connect(
        client.get(),
        reinterpret_cast<sockaddr*>(&address),
        sizeof(address));
    if (connect_rc != 0 && errno != EINPROGRESS) {
        std::cerr << "client connect failed\n";
        direct_accept_async::shutdown();
        return 1;
    }

    const char request[2]{'A', 'B'};
    if (!write_exact_until(client.get(), request, sizeof(request))) {
        std::cerr << "client write failed\n";
        direct_accept_async::shutdown();
        return 1;
    }

    direct_accept_async::shutdown();

    const int task_error = error.load(std::memory_order_acquire);
    if (task_error != 0) {
        std::cout << "io_uring accept direct "
                  << (unsupported_direct_accept_error(task_error) ? "unsupported" : "failed")
                  << " error=" << task_error << '\n';
        return unsupported_direct_accept_error(task_error) ? 0 : 1;
    }

    char response[2]{};
    if (!read_exact_until(client.get(), response, sizeof(response))) {
        std::cerr << "client read failed\n";
        return 1;
    }

    std::cout << "io_uring accept direct packed="
              << packed_read
              << " response=" << response[0] << response[1] << '\n';
    return 0;
#else
    std::cout << "io_uring accept direct example is Linux-only\n";
    return 0;
#endif
}
