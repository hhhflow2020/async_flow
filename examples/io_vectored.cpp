#include <atomic>
#include <iostream>

#include "support/io_vectored_datagram_tasks.hpp"
#include "support/io_vectored_socket_helpers.hpp"
#include "support/io_vectored_stream_tasks.hpp"

int main() {
    using namespace io_vectored_example;

    if constexpr (!af::supports_io_uring) {
        std::cout << "vectored io example is Linux-only\n";
        return 0;
    }

    vectored_async::init();

    VectoredStreamSocketPair stream{};
    if (!stream.create()) {
        std::cerr << "socketpair failed\n";
        vectored_async::shutdown();
        return 1;
    }

    bool server_ok = false;
    bool client_ok = false;
    int request_seen = 0;
    int response_seen = 0;

    const bool server_started =
        vectored_async::start_task<ServerTask>(stream.server.get(), &server_ok, &request_seen);
    const bool client_started =
        vectored_async::start_task<ClientTask>(stream.client.get(), &client_ok, &response_seen);
    if (!server_started || !client_started) {
        std::cerr << "vectored io task start failed\n";
        vectored_async::shutdown();
        return 1;
    }

    const char *backend = vectored_async::io_uring_backend_available(VectoredThreads::IO_0)
                              ? "io_uring"
                              : "epoll-fallback";
    std::cout << "vectored stream backend=" << backend << '\n';

    VectoredUdpLoopbackSockets udp{};
    if (!udp.create()) {
        std::cerr << "udp socket failed\n";
        vectored_async::shutdown();
        return 1;
    }

    std::atomic<int> datagram_armed{0};
    bool datagram_recv_ok = false;
    bool datagram_send_ok = false;
    int datagram_seen = 0;
    int datagram_bytes_sent = 0;
    if (!vectored_async::start_task<DatagramReceiverTask>(udp.receiver.get(), &datagram_armed,
                                                          &datagram_recv_ok, &datagram_seen) ||
        !wait_until(datagram_armed, 1) ||
        !vectored_async::start_task<DatagramSenderTask>(udp.sender.get(), udp.endpoint(),
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
}
