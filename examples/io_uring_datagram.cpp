#include <cstdint>
#include <iostream>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

enum class DatagramThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct DatagramRuntimeTraits {
    using Thread = DatagramThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(DatagramThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(DatagramThread thread) noexcept {
        return thread == DatagramThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using datagram_async = af::AsyncRuntime<DatagramRuntimeTraits>;
using DatagramTask = datagram_async::Task;

#if defined(__linux__)
class DatagramServerTask final : public DatagramTask {
public:
    explicit DatagramServerTask(DatagramTask::FactoryToken token) : DatagramTask(token) {}

    bool do_it(int fd, bool* ok, char* request_seen) {
        socket_.reset(DatagramThread::IO_0, fd);
        ok_ = ok;
        request_seen_ = request_seen;
        return schedule(DatagramThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        ReceiveRequest,
        SendResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ReceiveRequest:
            return receive_request();

        case State::SendResponse:
            return send_response();
        }
        return failed();
    }

    af::TaskResult receive_request() {
        peer_size_ = sizeof(peer_);
        const af::IoStatus status = socket_.recv_from_some(
            *this,
            &request_,
            sizeof(request_),
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_) || peer_size_ == 0U) {
            return failed();
        }

        *request_seen_ = request_;
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        const af::IoStatus status = socket_.send_to_some(
            *this,
            &response_,
            sizeof(response_),
            reinterpret_cast<const sockaddr*>(&peer_),
            peer_size_,
            send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        *ok_ = true;
        return done();
    }

    State state_{State::ReceiveRequest};
    af::UdpSocket<DatagramThread> socket_{};
    char request_{0};
    char response_{'R'};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState recv_{};
    af::IoOpState send_{};
    bool* ok_{nullptr};
    char* request_seen_{nullptr};
};

class DatagramClientTask final : public DatagramTask {
public:
    explicit DatagramClientTask(DatagramTask::FactoryToken token) : DatagramTask(token) {}

    bool do_it(
        int fd,
        sockaddr_in server,
        socklen_t server_size,
        bool* ok,
        char* response_seen) {
        socket_.reset(DatagramThread::IO_0, fd);
        server_ = server;
        server_size_ = server_size;
        ok_ = ok;
        response_seen_ = response_seen;
        return schedule(DatagramThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        SendRequest,
        ReceiveResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::SendRequest:
            return send_request();

        case State::ReceiveResponse:
            return receive_response();
        }
        return failed();
    }

    af::TaskResult send_request() {
        const af::IoStatus status = socket_.send_to_some(
            *this,
            &request_,
            sizeof(request_),
            reinterpret_cast<const sockaddr*>(&server_),
            server_size_,
            send_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }

        state_ = State::ReceiveResponse;
        return again();
    }

    af::TaskResult receive_response() {
        peer_size_ = sizeof(peer_);
        const af::IoStatus status = socket_.recv_from_some(
            *this,
            &response_,
            sizeof(response_),
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        *response_seen_ = response_;
        *ok_ = true;
        return done();
    }

    State state_{State::SendRequest};
    af::UdpSocket<DatagramThread> socket_{};
    sockaddr_in server_{};
    socklen_t server_size_{sizeof(server_)};
    char request_{'Q'};
    char response_{0};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState send_{};
    af::IoOpState recv_{};
    bool* ok_{nullptr};
    char* response_seen_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    datagram_async::init();
    if (!datagram_async::io_backend_available(DatagramThread::IO_0)) {
        std::cout << "IO backend unavailable\n";
        datagram_async::shutdown();
        return 0;
    }

    const char* backend =
        datagram_async::io_uring_backend_available(DatagramThread::IO_0)
            ? "enabled"
            : "epoll-fallback";
    std::cout << "io_uring datagram backend="
              << backend << '\n';

    af::UniqueFd server(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    af::UniqueFd client(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!server || !client) {
        std::cout << "udp socket failed\n";
        datagram_async::shutdown();
        return 1;
    }

    sockaddr_in server_address{};
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    server_address.sin_port = 0;
    if (::bind(server.get(), reinterpret_cast<sockaddr*>(&server_address), sizeof(server_address)) != 0) {
        std::cout << "udp bind failed\n";
        datagram_async::shutdown();
        return 1;
    }

    socklen_t server_size = sizeof(server_address);
    if (::getsockname(
            server.get(),
            reinterpret_cast<sockaddr*>(&server_address),
            &server_size) != 0) {
        std::cout << "udp getsockname failed\n";
        datagram_async::shutdown();
        return 1;
    }

    bool server_ok = false;
    bool client_ok = false;
    char request_seen = 0;
    char response_seen = 0;

    const bool server_started = datagram_async::start_task<DatagramServerTask>(
        server.get(),
        &server_ok,
        &request_seen);
    const bool client_started = datagram_async::start_task<DatagramClientTask>(
        client.get(),
        server_address,
        server_size,
        &client_ok,
        &response_seen);
    AF_ASSERT(server_started && client_started);

    if (!server_started || !client_started) {
        std::cout << "io_uring datagram task start failed\n";
        datagram_async::shutdown();
        return 1;
    }

    datagram_async::shutdown();
    if (!server_ok || !client_ok) {
        std::cout << "io_uring datagram round trip failed\n";
        return 1;
    }

    std::cout << "server request=" << request_seen
              << " client response=" << response_seen << '\n';
    return 0;
#else
    std::cout << "io_uring datagram example is Linux-only\n";
    return 0;
#endif
}
