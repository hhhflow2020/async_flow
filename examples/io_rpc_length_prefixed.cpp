#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>

#include "af/async_flow.hpp"

#if defined(__linux__)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

enum class RpcThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct RpcRuntimeTraits {
    using Thread = RpcThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(RpcThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::WaitForTasks;

    static constexpr af::ThreadKind thread_kind(RpcThread thread) noexcept {
        return thread == RpcThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using rpc_async = af::AsyncRuntime<RpcRuntimeTraits>;
using RpcTask = rpc_async::Task;

#if defined(__linux__)
static constexpr std::size_t kMaxFrameBytes = 4096;

class RpcServerTask;

class RpcProcessTask final : public RpcTask {
public:
    explicit RpcProcessTask(RpcTask::FactoryToken token) : RpcTask(token) {}

    bool do_it(RpcServerTask* server);

private:
    af::TaskResult run() override;

    RpcServerTask* server_{nullptr};
};

class RpcServerTask final : public RpcTask {
public:
    explicit RpcServerTask(RpcTask::FactoryToken token) : RpcTask(token) {}

    bool do_it(int listener_fd, bool* ok, int* error) {
        listener_.reset(RpcThread::IO_0, listener_fd);
        ok_ = ok;
        error_ = error;
        return schedule(RpcThread::IO_0);
    }

    [[nodiscard]] const char* request_data() const noexcept {
        return request_;
    }

    [[nodiscard]] std::size_t request_size() const noexcept {
        return request_size_;
    }

    void set_response(const char* data, std::size_t size) noexcept {
        if (data == nullptr) {
            response_size_ = 0;
            return;
        }
        if (size > kMaxFrameBytes) {
            size = kMaxFrameBytes;
        }
        std::memcpy(response_, data, size);
        response_size_ = size;
        response_written_ = 0;
        response_header_written_ = 0;

        const std::uint32_t net_len = htonl(static_cast<std::uint32_t>(response_size_));
        std::memcpy(response_header_, &net_len, sizeof(net_len));
        state_ = State::WriteResponseHeader;
    }

private:
    friend class RpcProcessTask;

    enum class State : std::uint8_t {
        Accept,
        ReadRequestHeader,
        ReadRequestBody,
        WaitLogic,
        WriteResponseHeader,
        WriteResponseBody,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Accept:
            return accept_client();
        case State::ReadRequestHeader:
            return read_request_header();
        case State::ReadRequestBody:
            return read_request_body();
        case State::WaitLogic:
            return pending();
        case State::WriteResponseHeader:
            return write_response_header();
        case State::WriteResponseBody:
            return write_response_body();
        }
        return failed();
    }

    af::TaskResult accept_client() {
        int fd = -1;
        const af::IoStatus status = listener_.accept_some(*this, nullptr, nullptr, &fd, accept_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || fd < 0) {
            return complete(status.failed() ? status.error : EIO);
        }

        accepted_.reset(fd);
        stream_.reset(RpcThread::IO_0, accepted_.get());
        state_ = State::ReadRequestHeader;
        return again();
    }

    af::TaskResult read_request_header() {
        const af::IoStatus status =
            stream_.recv_some(*this, request_header_ + request_header_read_, 4U - request_header_read_, read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        request_header_read_ += status.bytes;
        if (request_header_read_ < 4U) {
            return again();
        }

        std::uint32_t net_len = 0;
        std::memcpy(&net_len, request_header_, sizeof(net_len));
        request_size_ = static_cast<std::size_t>(ntohl(net_len));
        request_read_ = 0;
        if (request_size_ > kMaxFrameBytes) {
            return complete(EMSGSIZE);
        }

        state_ = State::ReadRequestBody;
        return again();
    }

    af::TaskResult read_request_body() {
        if (request_size_ == 0U) {
            request_read_ = 0;
            return dispatch_logic();
        }

        const af::IoStatus status = stream_.recv_some(
            *this,
            request_ + request_read_,
            request_size_ - request_read_,
            read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        request_read_ += status.bytes;
        if (request_read_ < request_size_) {
            return again();
        }

        return dispatch_logic();
    }

    af::TaskResult dispatch_logic() {
        state_ = State::WaitLogic;
        const bool started = rpc_async::start_task<RpcProcessTask>(this);
        if (!started) {
            return complete(EAGAIN);
        }
        return pending();
    }

    af::TaskResult write_response_header() {
        const af::IoStatus status = stream_.send_some(
            *this,
            response_header_ + response_header_written_,
            4U - response_header_written_,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        response_header_written_ += status.bytes;
        if (response_header_written_ < 4U) {
            return again();
        }

        state_ = State::WriteResponseBody;
        return again();
    }

    af::TaskResult write_response_body() {
        if (response_size_ == 0U) {
            return complete(0);
        }

        const af::IoStatus status = stream_.send_some(
            *this,
            response_ + response_written_,
            response_size_ - response_written_,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        response_written_ += status.bytes;
        if (response_written_ < response_size_) {
            return again();
        }

        return complete(0);
    }

    af::TaskResult complete(int error) {
        *error_ = error;
        *ok_ = true;
        return done();
    }

    State state_{State::Accept};
    af::TcpListener<RpcThread> listener_{};
    af::TcpStream<RpcThread> stream_{};
    af::UniqueFd accepted_{};

    af::IoOpState accept_{};
    af::IoOpState read_{};
    af::IoOpState write_{};

    char request_header_[4]{};
    std::size_t request_header_read_{0};
    char request_[kMaxFrameBytes]{};
    std::size_t request_size_{0};
    std::size_t request_read_{0};

    char response_header_[4]{};
    std::size_t response_header_written_{0};
    char response_[kMaxFrameBytes]{};
    std::size_t response_size_{0};
    std::size_t response_written_{0};

    bool* ok_{nullptr};
    int* error_{nullptr};
};

bool RpcProcessTask::do_it(RpcServerTask* server) {
    server_ = server;
    return schedule(RpcThread::Logic_0);
}

af::TaskResult RpcProcessTask::run() {
    if (server_ == nullptr) {
        return done();
    }

    const char* request = server_->request_data();
    const std::size_t request_size = server_->request_size();
    if (request_size == 4U && std::memcmp(request, "PING", 4U) == 0) {
        server_->set_response("PONG", 4U);
    } else {
        server_->set_response(request, request_size);
    }

    if (!rpc_async::post(RpcThread::IO_0, server_)) {
        return done();
    }
    return done();
}

class RpcClientTask final : public RpcTask {
public:
    explicit RpcClientTask(RpcTask::FactoryToken token) : RpcTask(token) {}

    bool do_it(
        int fd,
        sockaddr_in server,
        socklen_t server_size,
        bool* ok,
        int* error,
        bool* response_ok) {
        stream_.reset(RpcThread::IO_0, fd);
        server_ = server;
        server_size_ = server_size;
        ok_ = ok;
        error_ = error;
        response_ok_ = response_ok;
        return schedule(RpcThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Connect,
        SendRequestHeader,
        SendRequestBody,
        ReadResponseHeader,
        ReadResponseBody,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Connect:
            return connect();
        case State::SendRequestHeader:
            return send_request_header();
        case State::SendRequestBody:
            return send_request_body();
        case State::ReadResponseHeader:
            return read_response_header();
        case State::ReadResponseBody:
            return read_response_body();
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
            return complete(status.failed() ? status.error : EIO);
        }

        const char request[] = "PING";
        request_size_ = sizeof(request) - 1U;
        std::memcpy(request_, request, request_size_);
        request_written_ = 0;
        request_header_written_ = 0;
        const std::uint32_t net_len = htonl(static_cast<std::uint32_t>(request_size_));
        std::memcpy(request_header_, &net_len, sizeof(net_len));

        state_ = State::SendRequestHeader;
        return again();
    }

    af::TaskResult send_request_header() {
        const af::IoStatus status = stream_.send_some(
            *this,
            request_header_ + request_header_written_,
            4U - request_header_written_,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        request_header_written_ += status.bytes;
        if (request_header_written_ < 4U) {
            return again();
        }

        state_ = State::SendRequestBody;
        return again();
    }

    af::TaskResult send_request_body() {
        const af::IoStatus status = stream_.send_some(
            *this,
            request_ + request_written_,
            request_size_ - request_written_,
            write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        request_written_ += status.bytes;
        if (request_written_ < request_size_) {
            return again();
        }

        state_ = State::ReadResponseHeader;
        return again();
    }

    af::TaskResult read_response_header() {
        const af::IoStatus status = stream_.recv_some(
            *this,
            response_header_ + response_header_read_,
            4U - response_header_read_,
            read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        response_header_read_ += status.bytes;
        if (response_header_read_ < 4U) {
            return again();
        }

        std::uint32_t net_len = 0;
        std::memcpy(&net_len, response_header_, sizeof(net_len));
        response_size_ = static_cast<std::size_t>(ntohl(net_len));
        response_read_ = 0;
        if (response_size_ > kMaxFrameBytes) {
            return complete(EMSGSIZE);
        }

        state_ = State::ReadResponseBody;
        return again();
    }

    af::TaskResult read_response_body() {
        if (response_size_ == 0U) {
            return complete(0);
        }

        const af::IoStatus status = stream_.recv_some(
            *this,
            response_ + response_read_,
            response_size_ - response_read_,
            read_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes == 0U) {
            return complete(status.failed() ? status.error : EIO);
        }
        response_read_ += status.bytes;
        if (response_read_ < response_size_) {
            return again();
        }

        const bool ok = response_size_ == 4U && std::memcmp(response_, "PONG", 4U) == 0;
        *response_ok_ = ok;
        return complete(0);
    }

    af::TaskResult complete(int error) {
        *error_ = error;
        *ok_ = true;
        return done();
    }

    State state_{State::Connect};
    af::TcpStream<RpcThread> stream_{};
    sockaddr_in server_{};
    socklen_t server_size_{sizeof(server_)};

    af::IoOpState connect_{};
    af::IoOpState write_{};
    af::IoOpState read_{};

    char request_header_[4]{};
    std::size_t request_header_written_{0};
    char request_[kMaxFrameBytes]{};
    std::size_t request_size_{0};
    std::size_t request_written_{0};

    char response_header_[4]{};
    std::size_t response_header_read_{0};
    char response_[kMaxFrameBytes]{};
    std::size_t response_size_{0};
    std::size_t response_read_{0};

    bool* ok_{nullptr};
    int* error_{nullptr};
    bool* response_ok_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    rpc_async::init();
    if (!rpc_async::io_backend_available(RpcThread::IO_0)) {
        std::cout << "IO backend unavailable\n";
        rpc_async::shutdown();
        return 0;
    }

    const char* backend =
        rpc_async::io_uring_backend_available(RpcThread::IO_0)
            ? "enabled"
            : "epoll-fallback";
    std::cout << "rpc length-prefixed backend=" << backend << '\n';

    af::UniqueFd listener(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    af::UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!listener || !client) {
        std::cout << "tcp socket failed\n";
        rpc_async::shutdown();
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
        ::listen(listener.get(), 16) != 0) {
        std::cout << "tcp bind/listen failed\n";
        rpc_async::shutdown();
        return 1;
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
        std::cout << "tcp getsockname failed\n";
        rpc_async::shutdown();
        return 1;
    }

    bool server_ok = false;
    bool client_ok = false;
    int server_error = 0;
    int client_error = 0;
    bool response_ok = false;

    const bool server_started = rpc_async::start_task<RpcServerTask>(
        listener.get(),
        &server_ok,
        &server_error);
    const bool client_started = rpc_async::start_task<RpcClientTask>(
        client.get(),
        address,
        address_size,
        &client_ok,
        &client_error,
        &response_ok);
    AF_ASSERT(server_started && client_started);

    if (!server_started || !client_started) {
        std::cout << "rpc task start failed\n";
        rpc_async::shutdown();
        return 1;
    }

    rpc_async::wait_for_idle();
    rpc_async::shutdown();

    if (!server_ok || !client_ok) {
        std::cout << "rpc round trip failed\n";
        return 1;
    }

    if (server_error != 0 || client_error != 0) {
        std::cout << "rpc failed: server_error=" << server_error
                  << " client_error=" << client_error << '\n';
        return 1;
    }

    std::cout << "rpc response_ok=" << (response_ok ? 1 : 0) << '\n';
    return 0;
#else
    std::cout << "rpc length-prefixed example is Linux-only\n";
    return 0;
#endif
}
