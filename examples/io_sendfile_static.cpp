#include <iostream>

#include "support/io_sendfile_static_client_task.hpp"
#include "support/io_sendfile_static_file.hpp"
#include "support/io_sendfile_static_listener.hpp"
#include "support/io_sendfile_static_server_task.hpp"

int main() {
#if defined(__linux__)
    using namespace io_sendfile_static_example;

    SendfileStaticFile file{};
    if (!file.create()) {
        std::cerr << "file setup failed\n";
        return 1;
    }

    SendfileLoopbackListener listener{};
    if (!listener.create()) {
        std::cerr << "listener setup failed\n";
        return 1;
    }

    sendfile_async::init();
    SendfileServerResult server{};
    SendfileClientResult client{};
    const bool server_started = sendfile_async::start_task<StaticSendfileServerTask>(
        listener.fd.get(),
        file.fd.get(),
        &server);
    const bool client_started = sendfile_async::start_task<StaticSendfileClientTask>(
        listener.address,
        listener.address_size,
        &client);
    if (!server_started || !client_started) {
        std::cerr << "sendfile tasks did not start\n";
        sendfile_async::shutdown();
        return 1;
    }
    sendfile_async::shutdown();

    if (!server.ok || !client.ok || !client.payload_match) {
        std::cerr << "sendfile failed: server_error=" << server.error
                  << " client_error=" << client.error << '\n';
        return 1;
    }

    std::cout << "sendfile bytes=" << server.bytes_sent
              << " read=" << client.bytes_read << '\n';
    return 0;
#else
    std::cout << "sendfile example is Linux-only\n";
    return 0;
#endif
}
