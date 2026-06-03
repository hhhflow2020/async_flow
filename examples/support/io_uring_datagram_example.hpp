#pragma once

#include <iostream>

#include "io_uring_datagram_client_task.hpp"
#include "io_uring_datagram_server_task.hpp"
#include "io_uring_datagram_sockets.hpp"

namespace io_uring_datagram_example {

inline int run_datagram_example() {
    datagram_async::init();
    if (!datagram_async::io_backend_available(DatagramThreads::IO_0)) {
        std::cout << "IO backend unavailable\n";
        datagram_async::shutdown();
        return 0;
    }

    std::cout << "datagram backend=" << datagram_backend_name() << '\n';

    DatagramLoopbackSockets sockets{};
    if (!sockets.create()) {
        std::cout << "udp socket failed\n";
        datagram_async::shutdown();
        return 1;
    }

    bool server_ok = false;
    bool client_ok = false;
    char request_seen = 0;
    char response_seen = 0;

    const bool server_started = datagram_async::start_task<DatagramServerTask>(
        sockets.server.get(), &server_ok, &request_seen);
    const bool client_started = datagram_async::start_task<DatagramClientTask>(
        sockets.client.get(), sockets.server_address, sockets.server_size, &client_ok,
        &response_seen);
    AF_ASSERT(server_started && client_started);

    if (!server_started || !client_started) {
        std::cout << "io_uring datagram task start failed\n";
        datagram_async::shutdown();
        return 1;
    }

    datagram_async::shutdown();
    if (!server_ok || !client_ok) {
        std::cout << "io_uring datagram round trip failed\n";
        return 1;
    }

    std::cout << "server request=" << request_seen << " client response=" << response_seen << '\n';
    return 0;
}

} // namespace io_uring_datagram_example
