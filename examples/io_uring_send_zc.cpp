#include <iostream>

#include "support/io_uring_send_zc_client_task.hpp"
#include "support/io_uring_send_zc_listener.hpp"
#include "support/io_uring_send_zc_server_task.hpp"

int main() {
#if defined(__linux__)
    using namespace io_uring_send_zc_example;

    SendZcLoopbackListener listener{};
    if (!listener.create()) {
        std::cerr << "listener setup failed\n";
        return 1;
    }

    send_zc_async::init();
    const bool has_uring = send_zc_async::io_uring_backend_available(SendZcThreads::IO_0);
    SendZcServerResult server{};
    SendZcClientResult client{};
    const bool server_started =
        send_zc_async::start_task<SendZcServerTask>(listener.fd.get(), &server);
    const bool client_started = send_zc_async::start_task<SendZcClientTask>(
        listener.address, listener.address_size, &client);
    if (!server_started || !client_started) {
        std::cerr << "send_zc tasks did not start\n";
        send_zc_async::shutdown();
        return 1;
    }
    send_zc_async::shutdown();

    if (!server.ok || !client.ok || !client.payload_match) {
        std::cerr << "send_zc failed: server_error=" << server.error
                  << " client_error=" << client.error << '\n';
        return 1;
    }

    std::cout << "send_zc bytes=" << server.bytes_sent << " read=" << client.bytes_read
              << " io_uring=" << (has_uring ? "available" : "fallback") << '\n';
    return 0;
#else
    std::cout << "send_zc example is Linux-only\n";
    return 0;
#endif
}
