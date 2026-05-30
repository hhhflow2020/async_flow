#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace {

enum class VectoredThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct VectoredRuntimeTraits {
    using Thread = VectoredThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(VectoredThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;

    static constexpr af::ThreadKind thread_kind(VectoredThread thread) noexcept {
        return thread == VectoredThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using vectored_async = af::AsyncRuntime<VectoredRuntimeTraits>;
using VectoredTask = vectored_async::Task;

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
class ServerTask final : public VectoredTask {
public:
    explicit ServerTask(VectoredTask::FactoryToken token) : VectoredTask(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<int>* request_seen) {
        stream_.reset(VectoredThread::IO_0, fd);
        completed_ = completed;
        request_seen_ = request_seen;
        return schedule(VectoredThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        ReadRequest,
        SendResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ReadRequest:
            return read_request();

        case State::SendResponse:
            return send_response();
        }
        return failed();
    }

    af::TaskResult read_request() {
        request_iov_[0] = iovec{&request_[0], 1};
        request_iov_[1] = iovec{&request_[1], 1};
        const af::IoStatus status = stream_.recvv_some(*this, request_iov_, 2, read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }

        const int combined =
            (static_cast<int>(request_[0]) << 8) | static_cast<unsigned char>(request_[1]);
        request_seen_->store(combined, std::memory_order_release);
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        response_iov_[0] = iovec{&response_[0], 1};
        response_iov_[1] = iovec{&response_[1], 1};
        const af::IoStatus status = stream_.sendv_some(*this, response_iov_, 2, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::ReadRequest};
    af::TcpStream<VectoredThread> stream_{};
    char request_[2]{};
    char response_[2]{'O', 'K'};
    iovec request_iov_[2]{};
    iovec response_iov_[2]{};
    af::IoOpState read_{};
    af::IoOpState write_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* request_seen_{nullptr};
};

class ClientTask final : public VectoredTask {
public:
    explicit ClientTask(VectoredTask::FactoryToken token) : VectoredTask(token) {}

    bool do_it(int fd, std::atomic<int>* completed, std::atomic<int>* response_seen) {
        stream_.reset(VectoredThread::IO_0, fd);
        completed_ = completed;
        response_seen_ = response_seen;
        return schedule(VectoredThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        SendRequest,
        ReadResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::SendRequest:
            return send_request();

        case State::ReadResponse:
            return read_response();
        }
        return failed();
    }

    af::TaskResult send_request() {
        request_iov_[0] = iovec{&request_[0], 1};
        request_iov_[1] = iovec{&request_[1], 1};
        const af::IoStatus status = stream_.sendv_some(*this, request_iov_, 2, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }

        state_ = State::ReadResponse;
        return again();
    }

    af::TaskResult read_response() {
        response_iov_[0] = iovec{&response_[0], 1};
        response_iov_[1] = iovec{&response_[1], 1};
        const af::IoStatus status = stream_.recvv_some(*this, response_iov_, 2, read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        const int combined =
            (static_cast<int>(response_[0]) << 8) | static_cast<unsigned char>(response_[1]);
        response_seen_->store(combined, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::SendRequest};
    af::TcpStream<VectoredThread> stream_{};
    char request_[2]{'H', 'I'};
    char response_[2]{};
    iovec request_iov_[2]{};
    iovec response_iov_[2]{};
    af::IoOpState write_{};
    af::IoOpState read_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* response_seen_{nullptr};
};

class DatagramReceiverTask final : public VectoredTask {
public:
    explicit DatagramReceiverTask(VectoredTask::FactoryToken token) : VectoredTask(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* payload_seen) {
        socket_.reset(VectoredThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        payload_seen_ = payload_seen;
        return schedule(VectoredThread::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        iov_[0] = iovec{&payload_[0], 1};
        iov_[1] = iovec{&payload_[1], 1};
        const af::IoStatus status = socket_.recvv_from_some(
            *this,
            iov_,
            2,
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_)) {
            return failed();
        }

        const int combined =
            (static_cast<unsigned char>(payload_[0]) << 8) |
            static_cast<unsigned char>(payload_[1]);
        payload_seen_->store(combined, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::UdpSocket<VectoredThread> socket_{};
    char payload_[2]{};
    iovec iov_[2]{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState read_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* payload_seen_{nullptr};
};

class DatagramSenderTask final : public VectoredTask {
public:
    explicit DatagramSenderTask(VectoredTask::FactoryToken token) : VectoredTask(token) {}

    bool do_it(
        int fd,
        sockaddr_in address,
        socklen_t address_size,
        std::atomic<int>* completed,
        std::atomic<int>* bytes_sent) {
        socket_.reset(VectoredThread::IO_0, fd);
        address_ = address;
        address_size_ = address_size;
        completed_ = completed;
        bytes_sent_ = bytes_sent;
        return schedule(VectoredThread::IO_0);
    }

private:
    af::TaskResult run() override {
        iov_[0] = iovec{&payload_[0], 1};
        iov_[1] = iovec{&payload_[1], 1};
        const af::IoStatus status = socket_.sendv_to_some(
            *this,
            iov_,
            2,
            reinterpret_cast<const sockaddr*>(&address_),
            address_size_,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(payload_)) {
            return failed();
        }

        bytes_sent_->store(static_cast<int>(status.bytes), std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::UdpSocket<VectoredThread> socket_{};
    sockaddr_in address_{};
    socklen_t address_size_{sizeof(address_)};
    char payload_[2]{'U', 'D'};
    iovec iov_[2]{};
    af::IoOpState write_{};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* bytes_sent_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    vectored_async::init();

    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        std::cerr << "socketpair failed\n";
        vectored_async::shutdown();
        return 1;
    }
    af::UniqueFd server_fd(fds[0]);
    af::UniqueFd client_fd(fds[1]);

    std::atomic<int> server_done{0};
    std::atomic<int> client_done{0};
    std::atomic<int> request_seen{0};
    std::atomic<int> response_seen{0};

    const bool server_started =
        vectored_async::start_task<ServerTask>(server_fd.get(), &server_done, &request_seen);
    const bool client_started =
        vectored_async::start_task<ClientTask>(client_fd.get(), &client_done, &response_seen);
    if (!server_started || !client_started ||
        !wait_until(server_done, 1) ||
        !wait_until(client_done, 1)) {
        std::cerr << "vectored io round trip timed out\n";
        vectored_async::shutdown();
        return 1;
    }

    const char* backend =
        vectored_async::io_uring_backend_available(VectoredThread::IO_0)
            ? "io_uring"
            : "epoll-fallback";
    std::cout << "vectored stream backend=" << backend << '\n';
    std::cout << "request=0x" << std::hex << request_seen.load(std::memory_order_acquire)
              << " response=0x" << response_seen.load(std::memory_order_acquire) << '\n';

    af::UniqueFd udp_receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    af::UniqueFd udp_sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!udp_receiver || !udp_sender) {
        std::cerr << "udp socket failed\n";
        vectored_async::shutdown();
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(udp_receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        std::cerr << "udp bind failed\n";
        vectored_async::shutdown();
        return 1;
    }
    socklen_t address_size = sizeof(address);
    if (::getsockname(udp_receiver.get(), reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
        std::cerr << "udp getsockname failed\n";
        vectored_async::shutdown();
        return 1;
    }

    std::atomic<int> datagram_armed{0};
    std::atomic<int> datagram_recv_done{0};
    std::atomic<int> datagram_send_done{0};
    std::atomic<int> datagram_seen{0};
    std::atomic<int> datagram_bytes_sent{0};
    if (!vectored_async::start_task<DatagramReceiverTask>(
            udp_receiver.get(),
            &datagram_armed,
            &datagram_recv_done,
            &datagram_seen) ||
        !wait_until(datagram_armed, 1) ||
        !vectored_async::start_task<DatagramSenderTask>(
            udp_sender.get(),
            address,
            address_size,
            &datagram_send_done,
            &datagram_bytes_sent) ||
        !wait_until(datagram_send_done, 1) ||
        !wait_until(datagram_recv_done, 1)) {
        std::cerr << "vectored datagram round trip timed out\n";
        vectored_async::shutdown();
        return 1;
    }

    std::cout << "datagram=0x" << datagram_seen.load(std::memory_order_acquire)
              << " bytes_sent=" << std::dec
              << datagram_bytes_sent.load(std::memory_order_acquire) << '\n';

    vectored_async::shutdown();
    return 0;
#else
    std::cout << "vectored io example is Linux-only\n";
    return 0;
#endif
}
