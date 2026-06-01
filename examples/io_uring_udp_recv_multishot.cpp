#include <atomic>
#include <iostream>

#include "support/io_uring_udp_recv_multishot_socket_setup.hpp"
#include "support/io_uring_udp_recv_multishot_task.hpp"

int main() {
#if defined(__linux__)
    using namespace io_uring_udp_recv_multishot_example;

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
        ::connect(receiver.get(), reinterpret_cast<sockaddr *>(&sender_address),
                  sender_address_size) != 0 ||
        ::connect(sender.get(), reinterpret_cast<sockaddr *>(&receiver_address),
                  receiver_address_size) != 0) {
        std::cout << "udp bind/connect failed\n";
        udp_recv_async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    int packed_read = 0;
    std::atomic<int> error{0};
    const bool started = udp_recv_async::start_task<UdpRecvMultishotTask>(receiver.get(), &armed,
                                                                          &packed_read, &error);
    AF_ASSERT(started);

    if (!started || !wait_until_armed_or_error(armed, error)) {
        std::cout << "io_uring UDP recv_multishot arm timed out\n";
        udp_recv_async::shutdown();
        return 1;
    }
    if (armed.load(std::memory_order_acquire) == 0) {
        const int task_error = error.load(std::memory_order_acquire);
        if (task_error != 0) {
            std::cout << "io_uring UDP recv_multishot unsupported error=" << task_error << '\n';
            udp_recv_async::shutdown();
            return 0;
        }
        std::cout << "io_uring UDP recv_multishot arm timed out\n";
        udp_recv_async::shutdown();
        return 1;
    }

    const char payload[] = {'U', 'M'};
    if (::send(sender.get(), payload, 1, 0) != 1 || ::send(sender.get(), payload + 1, 1, 0) != 1) {
        std::cout << "send payload failed\n";
        udp_recv_async::shutdown();
        return 1;
    }

    udp_recv_async::shutdown();

    const int task_error = error.load(std::memory_order_acquire);
    if (task_error != 0) {
        std::cout << "io_uring UDP recv_multishot unsupported error=" << task_error << '\n';
        return 0;
    }

    const int packed = packed_read;
    std::cout << "io_uring UDP recv_multishot bytes=" << static_cast<char>((packed >> 8) & 0xff)
              << static_cast<char>(packed & 0xff) << '\n';
    return 0;
#else
    std::cout << "io_uring UDP recv_multishot example is Linux-only\n";
    return 0;
#endif
}
