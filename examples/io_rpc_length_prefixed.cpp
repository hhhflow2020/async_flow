#include <iostream>

#include "support/io_rpc_length_prefixed_client.hpp"
#include "support/io_rpc_length_prefixed_server.hpp"

int main() {
#if defined(__linux__)
    using namespace io_rpc_length_prefixed_example;

    rpc_async::init();
    if (!rpc_async::io_backend_available(RpcThread::IO_0)) {
        std::cout << "IO backend unavailable\n";
        rpc_async::shutdown();
        return 0;
    }

    const char* backend =
        rpc_async::io_uring_backend_available(RpcThread::IO_0)
            ? "enabled"
            : "epoll-fallback";
    std::cout << "rpc length-prefixed backend=" << backend << '\n';

    af::UniqueFd listener(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    af::UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!listener || !client) {
        std::cout << "tcp socket failed\n";
        rpc_async::shutdown();
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
        ::listen(listener.get(), 16) != 0) {
        std::cout << "tcp bind/listen failed\n";
        rpc_async::shutdown();
        return 1;
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(listener.get(), reinterpret_cast<sockaddr*>(&address), &address_size) != 0) {
        std::cout << "tcp getsockname failed\n";
        rpc_async::shutdown();
        return 1;
    }

    bool server_ok = false;
    bool client_ok = false;
    int server_error = 0;
    int client_error = 0;
    bool response_ok = false;

    const bool server_started = rpc_async::start_task<RpcServerTask>(
        listener.get(),
        &server_ok,
        &server_error);
    const bool client_started = rpc_async::start_task<RpcClientTask>(
        client.get(),
        address,
        address_size,
        &client_ok,
        &client_error,
        &response_ok);
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
        std::cout << "rpc failed: server_error=" << server_error
                  << " client_error=" << client_error << '\n';
        return 1;
    }

    std::cout << "rpc response_ok=" << (response_ok ? 1 : 0) << '\n';
    return 0;
#else
    std::cout << "rpc length-prefixed example is Linux-only\n";
    return 0;
#endif
}
