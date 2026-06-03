#pragma once

#include <iostream>

#include "io_socket_lifecycle_server_task.hpp"

namespace io_socket_lifecycle_example {

inline int run_socket_lifecycle_example() {
    socket_async::init();
    if (!socket_async::io_backend_available(SocketThreads::IO_0)) {
        std::cout << "socket lifecycle backend unavailable\n";
        socket_async::shutdown();
        return 0;
    }

    SocketLifecycleServerResult server{};
    SocketLifecycleClientResult client{};
    if (!socket_async::start_task<SocketLifecycleServerTask>(&server, &client)) {
        std::cerr << "failed to start socket lifecycle task\n";
        socket_async::shutdown();
        return 1;
    }

    socket_async::shutdown();
    std::cout << "socket lifecycle backend=" << socket_lifecycle_backend_name()
              << " port=" << server.port << " server_error=" << server.error
              << " client_error=" << client.error << '\n';
    return server.ok && client.ok ? 0 : 1;
}

} // namespace io_socket_lifecycle_example
