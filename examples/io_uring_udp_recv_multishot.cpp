#include <atomic>
#include <iostream>

#include "support/io_uring_udp_recv_multishot_socket_setup.hpp"
#include "support/io_uring_udp_recv_multishot_task.hpp"

int main() {
    using namespace io_uring_udp_recv_multishot_example;

    if constexpr (!af::supports_io_uring) {
        std::cout << "io_uring UDP recv_multishot example is Linux-only\n";
        return 0;
    }

    udp_recv_async::init();
    if (!udp_recv_async::io_uring_backend_available(UdpRecvThreads::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        udp_recv_async::shutdown();
        return 0;
    }

    UdpRecvMultishotSockets sockets{};
    if (!sockets.connect_loopback()) {
        std::cout << "udp bind/connect failed\n";
        udp_recv_async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    int packed_read = 0;
    std::atomic<int> error{0};
    const bool started = udp_recv_async::start_task<UdpRecvMultishotTask>(
        sockets.receiver.get(), &armed, &packed_read, &error);
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
    if (!sockets.send_payload(payload, sizeof(payload))) {
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
}
