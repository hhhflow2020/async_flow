#include <atomic>
#include <iostream>

#include "support/io_uring_udp_recvmsg_multishot_task.hpp"
#include "support/io_uring_udp_socket_setup.hpp"

int main() {
    using namespace io_uring_udp_recvmsg_multishot_example;

    if constexpr (!af::supports_io_uring) {
        std::cout << "io_uring UDP recvmsg_multishot example is Linux-only\n";
        return 0;
    }

    udp_recvmsg_async::init();
    if (!udp_recvmsg_async::io_uring_backend_available(UdpRecvmsgThreads::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        udp_recvmsg_async::shutdown();
        return 0;
    }

    UdpRecvmsgMultishotSockets sockets{};
    if (!sockets.bind_loopback_pair()) {
        std::cout << "udp bind failed\n";
        udp_recvmsg_async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    int packed_read = 0;
    int peer_count = 0;
    std::atomic<int> error{0};
    const bool started = udp_recvmsg_async::start_task<UdpRecvmsgMultishotTask>(
        sockets.receiver.get(), sockets.sender_port(), &armed, &packed_read, &peer_count, &error);
    AF_ASSERT(started);

    if (!started || !wait_until_armed_or_error(armed, error)) {
        std::cout << "io_uring UDP recvmsg_multishot arm timed out\n";
        udp_recvmsg_async::shutdown();
        return 1;
    }
    if (armed.load(std::memory_order_acquire) == 0) {
        const int task_error = error.load(std::memory_order_acquire);
        if (task_error != 0) {
            std::cout << "io_uring UDP recvmsg_multishot unsupported error=" << task_error << '\n';
            udp_recvmsg_async::shutdown();
            return 0;
        }
        std::cout << "io_uring UDP recvmsg_multishot arm timed out\n";
        udp_recvmsg_async::shutdown();
        return 1;
    }

    const char payload[] = {'R', 'M'};
    if (!sockets.send_payload_to_receiver(payload, sizeof(payload))) {
        std::cout << "send payload failed\n";
        udp_recvmsg_async::shutdown();
        return 1;
    }

    udp_recvmsg_async::shutdown();

    const int task_error = error.load(std::memory_order_acquire);
    if (task_error != 0) {
        std::cout << "io_uring UDP recvmsg_multishot unsupported error=" << task_error << '\n';
        return 0;
    }

    const int packed = packed_read;
    std::cout << "io_uring UDP recvmsg_multishot bytes=" << static_cast<char>((packed >> 8) & 0xff)
              << static_cast<char>(packed & 0xff) << " peers=" << peer_count << '\n';
    return 0;
}
