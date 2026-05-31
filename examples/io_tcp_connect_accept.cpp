#include <iostream>

#include "support/io_tcp_connect_accept_client_task.hpp"
#include "support/io_tcp_connect_accept_server_task.hpp"
#include "support/io_tcp_connect_accept_sockets.hpp"

int main() {
#if !defined(_WIN32)
    using namespace io_tcp_connect_accept_example;

    tcp_async::init();
    if (!tcp_async::io_backend_available(TcpThread::IO_0)) {
        std::cout << "IO backend unavailable\n";
        tcp_async::shutdown();
        return 0;
    }

    std::cout << "tcp connect/accept backend=" << tcp_backend_name() << '\n';

    TcpLoopbackSockets sockets{};
    if (!sockets.create()) {
        std::cout << "tcp socket failed\n";
        tcp_async::shutdown();
        return 1;
    }

    bool server_ok = false;
    bool client_ok = false;
    char request_seen = 0;
    char response_seen = 0;

    const bool server_started = tcp_async::start_task<TcpServerTask>(
        sockets.listener.get(),
        &server_ok,
        &request_seen);
    const bool client_started = tcp_async::start_task<TcpClientTask>(
        sockets.client.get(),
        sockets.address,
        sockets.address_size,
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
    std::cout << "tcp connect/accept example requires POSIX sockets\n";
    return 0;
#endif
}
