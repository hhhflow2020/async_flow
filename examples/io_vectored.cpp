#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "af/async_flow.hpp"

#if defined(__linux__)
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

    vectored_async::shutdown();
    return 0;
#else
    std::cout << "vectored io example is Linux-only\n";
    return 0;
#endif
}
