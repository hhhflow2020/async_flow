#include <iostream>

#include "support/io_rpc_length_prefixed_client.hpp"
#include "support/io_rpc_length_prefixed_server.hpp"
#include "support/io_rpc_length_prefixed_socket_helpers.hpp"

int main() {
    using namespace io_rpc_length_prefixed_example;

    if constexpr (!af::supports_io_uring) {
        std::cout << "rpc length-prefixed example is Linux-only\n";
        return 0;
    }

    rpc_async::init();
    if (!rpc_async::io_backend_available(RpcThreads::IO_0)) {
        std::cout << "IO backend unavailable\n";
        rpc_async::shutdown();
        return 0;
    }

    const char *backend =
        rpc_async::io_uring_backend_available(RpcThreads::IO_0) ? "enabled" : "epoll-fallback";
    std::cout << "rpc length-prefixed backend=" << backend << '\n';

    RpcLoopbackSockets sockets{};
    if (!sockets.create()) {
        std::cout << "tcp socket failed\n";
        rpc_async::shutdown();
        return 1;
    }

    bool server_ok = false;
    bool client_ok = false;
    int server_error = 0;
    int client_error = 0;
    bool response_ok = false;

    const bool server_started =
        rpc_async::start_task<RpcServerTask>(sockets.listener.get(), &server_ok, &server_error);
    const bool client_started = rpc_async::start_task<RpcClientTask>(
        sockets.client.get(), sockets.endpoint(), &client_ok, &client_error, &response_ok);
    AF_ASSERT(server_started && client_started);

    if (!server_started || !client_started) {
        std::cout << "rpc task start failed\n";
        rpc_async::shutdown();
        return 1;
    }

    rpc_async::wait_for_idle();
    rpc_async::shutdown();

    if (!server_ok || !client_ok) {
        std::cout << "rpc round trip failed\n";
        return 1;
    }

    if (server_error != 0 || client_error != 0) {
        std::cout << "rpc failed: server_error=" << server_error << " client_error=" << client_error
                  << '\n';
        return 1;
    }

    std::cout << "rpc response_ok=" << (response_ok ? 1 : 0) << '\n';
    return 0;
}
