#include <atomic>
#include <iostream>

#include "support/io_vectored_datagram_tasks.hpp"
#include "support/io_vectored_stream_tasks.hpp"

int main() {
#if defined(__linux__)
    using namespace io_vectored_example;

    vectored_async::init();

    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        std::cerr << "socketpair failed\n";
        vectored_async::shutdown();
        return 1;
    }
    af::UniqueFd server_fd(fds[0]);
    af::UniqueFd client_fd(fds[1]);

    bool server_ok = false;
    bool client_ok = false;
    int request_seen = 0;
    int response_seen = 0;

    const bool server_started =
        vectored_async::start_task<ServerTask>(server_fd.get(), &server_ok, &request_seen);
    const bool client_started =
        vectored_async::start_task<ClientTask>(client_fd.get(), &client_ok, &response_seen);
    if (!server_started || !client_started) {
        std::cerr << "vectored io task start failed\n";
        vectored_async::shutdown();
        return 1;
    }

    const char *backend = vectored_async::io_uring_backend_available(VectoredThreads::IO_0)
                              ? "io_uring"
                              : "epoll-fallback";
    std::cout << "vectored stream backend=" << backend << '\n';

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
    if (::bind(udp_receiver.get(), reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        std::cerr << "udp bind failed\n";
        vectored_async::shutdown();
        return 1;
    }
    socklen_t address_size = sizeof(address);
    if (::getsockname(udp_receiver.get(), reinterpret_cast<sockaddr *>(&address), &address_size) !=
        0) {
        std::cerr << "udp getsockname failed\n";
        vectored_async::shutdown();
        return 1;
    }

    std::atomic<int> datagram_armed{0};
    bool datagram_recv_ok = false;
    bool datagram_send_ok = false;
    int datagram_seen = 0;
    int datagram_bytes_sent = 0;
    if (!vectored_async::start_task<DatagramReceiverTask>(udp_receiver.get(), &datagram_armed,
                                                          &datagram_recv_ok, &datagram_seen) ||
        !wait_until(datagram_armed, 1) ||
        !vectored_async::start_task<DatagramSenderTask>(udp_sender.get(), address, address_size,
                                                        &datagram_send_ok, &datagram_bytes_sent)) {
        std::cerr << "vectored datagram task start/arm failed\n";
        vectored_async::shutdown();
        return 1;
    }

    vectored_async::shutdown();
    if (!server_ok || !client_ok || !datagram_recv_ok || !datagram_send_ok) {
        std::cerr << "vectored io round trip failed\n";
        return 1;
    }

    std::cout << "request=0x" << std::hex << request_seen << " response=0x" << response_seen
              << '\n';
    std::cout << "datagram=0x" << datagram_seen << " bytes_sent=" << std::dec << datagram_bytes_sent
              << '\n';
    return 0;
#else
    std::cout << "vectored io example is Linux-only\n";
    return 0;
#endif
}
