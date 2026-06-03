#include <iostream>

#include "support/io_shutdown_socket_pair.hpp"
#include "support/io_shutdown_task.hpp"

int main() {
    using namespace io_shutdown_example;

    if constexpr (!af::supports_io_uring) {
        std::cout << "io shutdown example is Linux-only\n";
        return 0;
    }

    shutdown_async::init();
    if (!shutdown_async::io_backend_available(ShutdownThreads::IO_0)) {
        std::cout << "io shutdown backend unavailable\n";
        shutdown_async::shutdown();
        return 0;
    }

    ShutdownSocketPair sockets{};
    if (!sockets.create()) {
        std::cerr << "socketpair failed\n";
        shutdown_async::shutdown();
        return 1;
    }

    int error = 0;
    if (!shutdown_async::start_task<ShutdownWriteTask>(sockets.local.get(), &error)) {
        std::cerr << "shutdown task start failed\n";
        shutdown_async::shutdown();
        return 1;
    }

    shutdown_async::shutdown();

    if (error != 0) {
        std::cerr << "shutdown failed error=" << error << '\n';
        return 1;
    }
    if (!sockets.peer_observed_eof()) {
        std::cerr << "peer did not observe EOF\n";
        return 1;
    }

    std::cout << "io shutdown backend="
              << (shutdown_async::io_uring_backend_available(ShutdownThreads::IO_0) ? "io_uring"
                                                                                    : "fallback")
              << " eof=1\n";
    return 0;
}
