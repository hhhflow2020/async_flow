#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "app_runtime.hpp"

#if defined(__linux__)
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {

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
class StreamEchoTask final : public Task {
public:
    explicit StreamEchoTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, std::atomic<int>* armed) {
        stream_.reset(AppThread::IO_0, fd);
        armed_ = armed;
        return schedule(AppThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Read,
        Write,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Read:
            return read_request();

        case State::Write:
            return write_response();
        }
        return failed();
    }

    af::TaskResult read_request() {
        const af::IoStatus status = stream_.recv_some(*this, &request_, sizeof(request_), read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }

        state_ = State::Write;
        return again();
    }

    af::TaskResult write_response() {
        const af::IoStatus status = stream_.send_some(*this, &response_, sizeof(response_), write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }

        std::cout << "stream request=" << request_ << " response=" << response_ << '\n';
        return done();
    }

    State state_{State::Read};
    af::TcpStream<AppThread> stream_{};
    char request_{0};
    char response_{'S'};
    af::IoOpState read_{};
    af::IoOpState write_{};
    std::atomic<int>* armed_{nullptr};
};

class UdpReceiveTask final : public Task {
public:
    explicit UdpReceiveTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(int fd, std::atomic<int>* armed) {
        socket_.reset(AppThread::IO_0, fd);
        armed_ = armed;
        return schedule(AppThread::IO_0);
    }

private:
    af::TaskResult run() override {
        peer_size_ = sizeof(peer_);
        const af::IoStatus status = socket_.recv_from_some(
            *this,
            &value_,
            sizeof(value_),
            reinterpret_cast<sockaddr*>(&peer_),
            &peer_size_,
            recv_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(value_)) {
            return failed();
        }

        std::cout << "udp datagram=" << value_ << '\n';
        return done();
    }

    af::UdpSocket<AppThread> socket_{};
    char value_{0};
    sockaddr_storage peer_{};
    socklen_t peer_size_{sizeof(peer_)};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
};
#endif

} // namespace

int main() {
#if defined(__linux__)
    async::init();
    if (!async::io_backend_available(AppThread::IO_0)) {
        std::cout << "epoll backend unavailable\n";
        async::shutdown();
        return 0;
    }

    int stream_fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, stream_fds) != 0) {
        std::cout << "socketpair failed\n";
        async::shutdown();
        return 1;
    }
    af::UniqueFd stream_server(stream_fds[0]);
    af::UniqueFd stream_client(stream_fds[1]);

    std::atomic<int> stream_armed{0};
    const bool stream_started = async::start_task<StreamEchoTask>(stream_server.get(), &stream_armed);
    AF_ASSERT(stream_started);

    if (stream_started && wait_until(stream_armed, 1)) {
        const char value = 'T';
        static_cast<void>(::write(stream_client.get(), &value, sizeof(value)));
    } else {
        std::cout << "stream adapter task did not arm\n";
        async::shutdown();
        return 1;
    }

    af::UniqueFd udp_receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    af::UniqueFd udp_sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!udp_receiver || !udp_sender) {
        std::cout << "udp socket failed\n";
        async::shutdown();
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(udp_receiver.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        std::cout << "udp bind failed\n";
        async::shutdown();
        return 1;
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(
            udp_receiver.get(),
            reinterpret_cast<sockaddr*>(&address),
            &address_size) != 0) {
        std::cout << "udp getsockname failed\n";
        async::shutdown();
        return 1;
    }

    std::atomic<int> udp_armed{0};
    const bool udp_started = async::start_task<UdpReceiveTask>(udp_receiver.get(), &udp_armed);
    AF_ASSERT(udp_started);

    if (udp_started && wait_until(udp_armed, 1)) {
        const char value = 'U';
        static_cast<void>(::sendto(
            udp_sender.get(),
            &value,
            sizeof(value),
            0,
            reinterpret_cast<sockaddr*>(&address),
            address_size));
    } else {
        std::cout << "udp adapter task did not arm\n";
        async::shutdown();
        return 1;
    }

    async::shutdown();

    char response = 0;
    static_cast<void>(::read(stream_client.get(), &response, sizeof(response)));
    std::cout << "stream peer received=" << response << '\n';
    return 0;
#else
    std::cout << "IO adapter example is Linux-only\n";
    return 0;
#endif
}
