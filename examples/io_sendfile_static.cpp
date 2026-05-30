#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

enum class SendfileThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct SendfileRuntimeTraits {
    using Thread = SendfileThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(SendfileThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(SendfileThread thread) noexcept {
        return thread == SendfileThread::IO_0 ? af::ThreadKind::Epoll : af::ThreadKind::Worker;
    }
};

using sendfile_async = af::AsyncRuntime<SendfileRuntimeTraits>;
using SendfileTaskBase = sendfile_async::Task;

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

#if defined(__linux__)
class StaticSendfileTask final : public SendfileTaskBase {
public:
    explicit StaticSendfileTask(SendfileTaskBase::FactoryToken token)
        : SendfileTaskBase(token) {}

    bool do_it(
        int socket_fd,
        int file_fd,
        std::size_t total_size,
        std::atomic<int>* completed,
        std::atomic<std::size_t>* bytes_sent) {
        stream_.reset(SendfileThread::IO_0, socket_fd);
        file_fd_ = file_fd;
        total_size_ = total_size;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(SendfileThread::IO_0);
    }

private:
    af::TaskResult run() override {
        return send_next_chunk();
    }

    af::TaskResult send_next_chunk() {
        if (sent_ >= total_size_) {
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        const std::size_t remaining = total_size_ - sent_;
        const std::size_t count = remaining < chunk_size_ ? remaining : chunk_size_;
        const af::IoStatus status =
            stream_.sendfile_some(*this, file_fd_, &offset_, count, send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U || status.bytes > remaining) {
            return failed();
        }

        sent_ += status.bytes;
        bytes_sent_->store(sent_, std::memory_order_release);
        return again();
    }

    af::TcpStream<SendfileThread> stream_{};
    int file_fd_{-1};
    af::IoOffset offset_{0};
    std::size_t total_size_{0};
    std::size_t sent_{0};
    std::size_t chunk_size_{4096};
    af::IoOpState send_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<std::size_t>* bytes_sent_{nullptr};
};

bool create_loopback_listener(af::UniqueFd& listener, sockaddr_in& address) {
    listener.reset(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!listener) {
        return false;
    }

    int one = 1;
    static_cast<void>(::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)));

    address = sockaddr_in{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
        ::listen(listener.get(), 8) != 0) {
        return false;
    }

    socklen_t address_size = sizeof(address);
    return ::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address), &address_size) == 0;
}

int accept_until_ready(int listener_fd) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (;;) {
        const int fd = ::accept4(listener_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd >= 0) {
            return fd;
        }
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (std::chrono::steady_clock::now() > deadline) {
            return -1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
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
#endif

} // namespace

int main() {
#if defined(__linux__)
    const char payload[] =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 20\r\n"
        "\r\n"
        "asyncflow sendfile\n";
    constexpr std::size_t payload_size = sizeof(payload) - 1U;

    char path[96]{};
    const int path_written = std::snprintf(
        path,
        sizeof(path),
        "/tmp/asyncflow-sendfile-static-%ld",
        static_cast<long>(::getpid()));
    if (path_written < 0 || static_cast<std::size_t>(path_written) >= sizeof(path)) {
        std::cerr << "path formatting failed\n";
        return 1;
    }

    af::UniqueFd file(::open(path, O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, 0600));
    static_cast<void>(::unlink(path));
    if (!file || ::write(file.get(), payload, payload_size) != static_cast<ssize_t>(payload_size)) {
        std::cerr << "file setup failed\n";
        return 1;
    }

    af::UniqueFd listener;
    sockaddr_in address{};
    if (!create_loopback_listener(listener, address)) {
        std::cerr << "listener setup failed\n";
        return 1;
    }

    af::UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!client) {
        std::cerr << "client socket failed\n";
        return 1;
    }
    const int connect_rc = ::connect(
        client.get(),
        reinterpret_cast<sockaddr*>(&address),
        sizeof(address));
    if (connect_rc != 0 && errno != EINPROGRESS) {
        std::cerr << "connect failed\n";
        return 1;
    }

    af::UniqueFd accepted(accept_until_ready(listener.get()));
    if (!accepted) {
        std::cerr << "accept timed out\n";
        return 1;
    }

    sendfile_async::init();
    std::atomic<int> completed{0};
    std::atomic<std::size_t> bytes_sent{0};
    const bool started = sendfile_async::start_task<StaticSendfileTask>(
        accepted.get(),
        file.get(),
        payload_size,
        &completed,
        &bytes_sent);
    if (!started || !wait_until(completed, 1)) {
        std::cerr << "sendfile task timed out\n";
        sendfile_async::shutdown();
        return 1;
    }

    char received[payload_size]{};
    if (!read_exact_until(client.get(), received, payload_size) ||
        std::memcmp(received, payload, payload_size) != 0) {
        std::cerr << "payload mismatch\n";
        sendfile_async::shutdown();
        return 1;
    }

    std::cout << "sendfile bytes=" << bytes_sent.load(std::memory_order_acquire) << '\n';
    sendfile_async::shutdown();
    return 0;
#else
    std::cout << "sendfile example is Linux-only\n";
    return 0;
#endif
}
