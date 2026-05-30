#include <cstdint>
#include <iostream>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

enum class TcpThread : std::int16_t {
    enum_thread_index_start = -1,
    IO_0,
    enum_thread_index_end,
};

struct TcpRuntimeTraits {
    using Thread = TcpThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(TcpThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(TcpThread thread) noexcept {
        return thread == TcpThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using tcp_async = af::AsyncRuntime<TcpRuntimeTraits>;
using TcpTask = tcp_async::Task;

#if defined(__linux__)
class TcpServerTask final : public TcpTask {
public:
    explicit TcpServerTask(TcpTask::FactoryToken token) : TcpTask(token) {}

    bool do_it(int fd, bool* ok, char* request_seen) {
        listener_.reset(TcpThread::IO_0, fd);
        ok_ = ok;
        request_seen_ = request_seen;
        return schedule(TcpThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        AcceptClient,
        ReceiveRequest,
        SendResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::AcceptClient:
            return accept_client();

        case State::ReceiveRequest:
            return receive_request();

        case State::SendResponse:
            return send_response();
        }
        return failed();
    }

    af::TaskResult accept_client() {
        peer_size_ = sizeof(peer_);
        int fd = -1;
        const af::IoStatus status = listener_.accept_some(
            *this,
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            &fd,
            accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return failed();
        }

        accepted_.reset(fd);
        stream_.reset(TcpThread::IO_0, accepted_.get());
        state_ = State::ReceiveRequest;
        return again();
    }

    af::TaskResult receive_request() {
        const af::IoStatus status = stream_.recv_some(*this, &request_, sizeof(request_), read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }

        *request_seen_ = request_;
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        const af::IoStatus status = stream_.send_some(*this, &response_, sizeof(response_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        *ok_ = true;
        return done();
    }

    State state_{State::AcceptClient};
    af::TcpListener<TcpThread> listener_{};
    af::TcpStream<TcpThread> stream_{};
    af::UniqueFd accepted_{};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    char request_{0};
    char response_{'R'};
    af::IoOpState accept_{};
    af::IoOpState read_{};
    af::IoOpState write_{};
    bool* ok_{nullptr};
    char* request_seen_{nullptr};
};

class TcpClientTask final : public TcpTask {
public:
    explicit TcpClientTask(TcpTask::FactoryToken token) : TcpTask(token) {}

    bool do_it(
        int fd,
        sockaddr_in server,
        socklen_t server_size,
        bool* ok,
        char* response_seen) {
        stream_.reset(TcpThread::IO_0, fd);
        server_ = server;
        server_size_ = server_size;
        ok_ = ok;
        response_seen_ = response_seen;
        return schedule(TcpThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Connect,
        SendRequest,
        ReceiveResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Connect:
            return connect();

        case State::SendRequest:
            return send_request();

        case State::ReceiveResponse:
            return receive_response();
        }
        return failed();
    }

    af::TaskResult connect() {
        const af::IoStatus status = stream_.connect(
            *this,
            reinterpret_cast<const sockaddr*>(&server_),
            server_size_,
            connect_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }

        state_ = State::SendRequest;
        return again();
    }

    af::TaskResult send_request() {
        const af::IoStatus status = stream_.send_some(*this, &request_, sizeof(request_), write_);
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
        const af::IoStatus status = stream_.recv_some(*this, &response_, sizeof(response_), read_);
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

    State state_{State::Connect};
    af::TcpStream<TcpThread> stream_{};
    sockaddr_in server_{};
    socklen_t server_size_{sizeof(server_)};
    char request_{'Q'};
    char response_{0};
    af::IoOpState connect_{};
    af::IoOpState write_{};
    af::IoOpState read_{};
    bool* ok_{nullptr};
    char* response_seen_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    tcp_async::init();
    if (!tcp_async::io_backend_available(TcpThread::IO_0)) {
        std::cout << "IO backend unavailable\n";
        tcp_async::shutdown();
        return 0;
    }

    const char* backend =
        tcp_async::io_uring_backend_available(TcpThread::IO_0)
            ? "enabled"
            : "epoll-fallback";
    std::cout << "io_uring tcp connect/accept backend=" << backend << '\n';

    af::UniqueFd listener(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    af::UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!listener || !client) {
        std::cout << "tcp socket failed\n";
        tcp_async::shutdown();
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
        ::listen(listener.get(), 16) != 0) {
        std::cout << "tcp bind/listen failed\n";
        tcp_async::shutdown();
        return 1;
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
        std::cout << "tcp getsockname failed\n";
        tcp_async::shutdown();
        return 1;
    }

    bool server_ok = false;
    bool client_ok = false;
    char request_seen = 0;
    char response_seen = 0;

    const bool server_started = tcp_async::start_task<TcpServerTask>(
        listener.get(),
        &server_ok,
        &request_seen);
    const bool client_started = tcp_async::start_task<TcpClientTask>(
        client.get(),
        address,
        address_size,
        &client_ok,
        &response_seen);
    AF_ASSERT(server_started && client_started);

    if (!server_started || !client_started) {
        std::cout << "tcp connect/accept task start failed\n";
        tcp_async::shutdown();
        return 1;
    }

    tcp_async::shutdown();
    if (!server_ok || !client_ok) {
        std::cout << "tcp connect/accept round trip failed\n";
        return 1;
    }

    std::cout << "server request=" << request_seen
              << " client response=" << response_seen << '\n';
    return 0;
#else
    std::cout << "tcp connect/accept example is Linux-only\n";
    return 0;
#endif
}
