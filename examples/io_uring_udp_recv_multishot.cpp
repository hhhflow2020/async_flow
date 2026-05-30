#include <atomic>
#include <cerrno>
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

namespace {

enum class UdpRecvThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct UdpRecvRuntimeTraits {
    using Thread = UdpRecvThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(UdpRecvThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(UdpRecvThread thread) noexcept {
        return thread == UdpRecvThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using udp_recv_async = af::AsyncRuntime<UdpRecvRuntimeTraits>;
using UdpRecvTaskBase = udp_recv_async::Task;

bool wait_until_at_least(std::atomic<int>& value, int expected) {
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
class UdpRecvMultishotTask final : public UdpRecvTaskBase {
public:
    explicit UdpRecvMultishotTask(UdpRecvTaskBase::FactoryToken token)
        : UdpRecvTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* packed_read,
        std::atomic<int>* error) {
        socket_.reset(UdpRecvThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        packed_read_ = packed_read;
        error_ = error;
        return schedule(UdpRecvThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Recv,
        Cancel,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_ring();
        case State::Recv:
            return recv_one();
        case State::Cancel:
            return finish_cancel();
        case State::Unregister:
            return unregister_ring();
        }
        return complete(EIO);
    }

    af::TaskResult register_ring() {
        int init_error = 0;
        if (!ring_.init(buffer_count, init_error)) {
            return complete(init_error == 0 ? EIO : init_error);
        }

        af::IoProvidedBuffer buffers[buffer_count]{};
        for (std::uint16_t i = 0; i < buffer_count; ++i) {
            buffers[i] = af::IoProvidedBuffer{&buffers_[i], sizeof(buffers_[i]), i};
        }
        int add_error = 0;
        if (!ring_.add(buffers, buffer_count, add_error)) {
            return complete(add_error == 0 ? EIO : add_error);
        }

        int register_error = 0;
        if (!udp_recv_async::io_register_provided_buffer_ring(
                UdpRecvThread::IO_0,
                ring_.ring(),
                ring_.entries(),
                buffer_group,
                &register_error)) {
            return complete(register_error == 0 ? EIO : register_error);
        }
        registered_ = true;
        state_ = State::Recv;
        return again();
    }

    af::TaskResult recv_one() {
        std::uint16_t buffer_id = 0;
        const af::IoStatus status =
            socket_.recv_multishot(*this, buffer_group, &buffer_id, recv_);
        if (status.pending()) {
            if (!armed_once_) {
                armed_once_ = true;
                armed_->fetch_add(1, std::memory_order_release);
            }
            return pending();
        }
        if (status.failed()) {
            return complete(status.error);
        }
        if (!status.ready() || status.bytes != 1U || buffer_id >= buffer_count) {
            return stop_recv(EIO);
        }

        const int shifted = received_ == 0 ? 8 : 0;
        packed_read_->fetch_or(
            static_cast<int>(static_cast<unsigned char>(buffers_[buffer_id])) << shifted,
            std::memory_order_acq_rel);
        ++received_;

        const af::IoProvidedBuffer buffer{
            &buffers_[buffer_id],
            sizeof(buffers_[buffer_id]),
            buffer_id};
        int add_error = 0;
        if (!ring_.add(&buffer, 1, add_error)) {
            return stop_recv(add_error == 0 ? EIO : add_error);
        }

        if (received_ < target_reads) {
            return pending();
        }
        if (!recv_.waiting) {
            state_ = State::Unregister;
            return again();
        }
        finish_error_ = 0;
        if (!udp_recv_async::cancel_io(UdpRecvThread::IO_0, recv_)) {
            return complete(recv_.wait.error == 0 ? EIO : recv_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    af::TaskResult finish_cancel() {
        std::uint16_t ignored = 0;
        const af::IoStatus status =
            socket_.recv_multishot(*this, buffer_group, &ignored, recv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.failed() || status.error != ECANCELED) {
            return complete(status.failed() ? status.error : EIO);
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_ring() {
        if (registered_) {
            int unregister_error = 0;
            if (!udp_recv_async::io_unregister_provided_buffer_ring(
                    UdpRecvThread::IO_0,
                    buffer_group,
                    &unregister_error)) {
                return complete(unregister_error == 0 ? EIO : unregister_error);
            }
            registered_ = false;
        }
        return complete(finish_error_);
    }

    af::TaskResult complete(int error) {
        if (registered_) {
            int unregister_error = 0;
            if (udp_recv_async::io_unregister_provided_buffer_ring(
                    UdpRecvThread::IO_0,
                    buffer_group,
                    &unregister_error)) {
                registered_ = false;
            }
        }
        error_->store(error, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TaskResult stop_recv(int error) {
        finish_error_ = error == 0 ? EIO : error;
        if (!recv_.waiting) {
            state_ = State::Unregister;
            return again();
        }
        if (!udp_recv_async::cancel_io(UdpRecvThread::IO_0, recv_)) {
            return complete(recv_.wait.error == 0 ? finish_error_ : recv_.wait.error);
        }
        state_ = State::Cancel;
        return pending();
    }

    static constexpr std::uint16_t buffer_group = 10;
    static constexpr unsigned buffer_count = 2;
    static constexpr int target_reads = 2;

    State state_{State::Register};
    af::UdpSocket<UdpRecvThread> socket_{};
    af::IoProvidedBufferRing ring_{};
    char buffers_[buffer_count]{};
    int received_{0};
    int finish_error_{0};
    bool armed_once_{false};
    bool registered_{false};
    af::IoOpState recv_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* packed_read_{nullptr};
    std::atomic<int>* error_{nullptr};
};

bool bind_loopback(int fd, sockaddr_in& address, socklen_t& address_size) {
    address = sockaddr_in{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        return false;
    }
    address_size = sizeof(address);
    return ::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_size) == 0;
}
#endif

} // namespace

int main() {
#if defined(__linux__)
    udp_recv_async::init();
    if (!udp_recv_async::io_uring_backend_available(UdpRecvThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        udp_recv_async::shutdown();
        return 0;
    }

    af::UniqueFd receiver(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    af::UniqueFd sender(::socket(AF_INET, SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!receiver || !sender) {
        std::cout << "socket failed\n";
        udp_recv_async::shutdown();
        return 1;
    }

    sockaddr_in receiver_address{};
    socklen_t receiver_address_size = sizeof(receiver_address);
    sockaddr_in sender_address{};
    socklen_t sender_address_size = sizeof(sender_address);
    if (!bind_loopback(receiver.get(), receiver_address, receiver_address_size) ||
        !bind_loopback(sender.get(), sender_address, sender_address_size) ||
        ::connect(
            receiver.get(),
            reinterpret_cast<sockaddr*>(&sender_address),
            sender_address_size) != 0 ||
        ::connect(
            sender.get(),
            reinterpret_cast<sockaddr*>(&receiver_address),
            receiver_address_size) != 0) {
        std::cout << "udp bind/connect failed\n";
        udp_recv_async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    std::atomic<int> completed{0};
    std::atomic<int> packed_read{0};
    std::atomic<int> error{0};
    const bool started = udp_recv_async::start_task<UdpRecvMultishotTask>(
        receiver.get(),
        &armed,
        &completed,
        &packed_read,
        &error);
    AF_ASSERT(started);

    if (!started || !wait_until_at_least(armed, 1)) {
        if (wait_until_at_least(completed, 1)) {
            std::cout << "io_uring UDP recv_multishot unsupported error="
                      << error.load(std::memory_order_acquire) << '\n';
            udp_recv_async::shutdown();
            return 0;
        }
        std::cout << "io_uring UDP recv_multishot arm timed out\n";
        udp_recv_async::shutdown();
        return 1;
    }

    const char payload[] = {'U', 'M'};
    if (::send(sender.get(), payload, 1, 0) != 1 ||
        ::send(sender.get(), payload + 1, 1, 0) != 1) {
        std::cout << "send payload failed\n";
        udp_recv_async::shutdown();
        return 1;
    }

    if (!wait_until_at_least(completed, 1)) {
        std::cout << "io_uring UDP recv_multishot task timed out\n";
        udp_recv_async::shutdown();
        return 1;
    }

    const int task_error = error.load(std::memory_order_acquire);
    if (task_error != 0) {
        std::cout << "io_uring UDP recv_multishot unsupported error=" << task_error << '\n';
        udp_recv_async::shutdown();
        return 0;
    }

    const int packed = packed_read.load(std::memory_order_acquire);
    std::cout << "io_uring UDP recv_multishot bytes="
              << static_cast<char>((packed >> 8) & 0xff)
              << static_cast<char>(packed & 0xff) << '\n';
    udp_recv_async::shutdown();
    return 0;
#else
    std::cout << "io_uring UDP recv_multishot example is Linux-only\n";
    return 0;
#endif
}
