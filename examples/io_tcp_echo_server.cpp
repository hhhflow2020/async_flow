#include <array>
#include <iostream>
#include <utility>

#include "support/io_tcp_echo_client_task.hpp"
#include "support/io_tcp_echo_server_task.hpp"
#include "support/io_tcp_echo_sockets.hpp"

int main() {
#if !defined(_WIN32)
    using namespace io_tcp_echo_example;

    echo_async::init();
    if (!echo_async::io_backend_available(EchoThread::IO_0) ||
        !echo_async::io_backend_available(EchoThread::IO_1)) {
        std::cout << "IO backend unavailable\n";
        echo_async::shutdown();
        return 0;
    }

    std::cout << "tcp echo backends="
              << echo_backend_name(EchoThread::IO_0) << ','
              << echo_backend_name(EchoThread::IO_1) << '\n';

    EchoLoopbackListener listener{};
    if (!listener.create()) {
        std::cout << "tcp echo listener failed\n";
        echo_async::shutdown();
        return 1;
    }

    std::array<EchoSessionResult, echo_client_count> sessions{};
    std::array<EchoClientResult, echo_client_count> clients{};
    bool server_ok = false;
    int server_error = 0;

    const bool server_started = echo_async::start_task<EchoServerTask>(
        listener.fd.get(),
        sessions.data(),
        sessions.size(),
        &server_ok,
        &server_error);

    constexpr EchoPayload request0{{'H', 'E', 'L', 'L', 'O', 'I', 'O', 'A'}};
    constexpr EchoPayload request1{{'A', 'S', 'Y', 'N', 'C', 'I', 'O', 'B'}};
    constexpr EchoPayload expected0{{'h', 'e', 'l', 'l', 'o', 'i', 'o', 'a'}};
    constexpr EchoPayload expected1{{'a', 's', 'y', 'n', 'c', 'i', 'o', 'b'}};

    af::UniqueFd client0 = echo_make_tcp_socket();
    af::UniqueFd client1 = echo_make_tcp_socket();
    const bool clients_ready = static_cast<bool>(client0) && static_cast<bool>(client1);
    const bool client0_started = clients_ready && echo_async::start_task<EchoClientTask>(
        std::move(client0),
        EchoThread::IO_0,
        listener.address,
        listener.address_size,
        request0,
        &clients[0]);
    const bool client1_started = clients_ready && echo_async::start_task<EchoClientTask>(
        std::move(client1),
        EchoThread::IO_1,
        listener.address,
        listener.address_size,
        request1,
        &clients[1]);

    AF_ASSERT(server_started && client0_started && client1_started);
    if (!server_started || !client0_started || !client1_started) {
        std::cout << "tcp echo task start failed\n";
        echo_async::shutdown();
        return 1;
    }

    echo_async::shutdown();

    const bool ok =
        server_ok &&
        server_error == 0 &&
        sessions[0].ok &&
        sessions[1].ok &&
        clients[0].ok &&
        clients[1].ok &&
        clients[0].response == expected0 &&
        clients[1].response == expected1;
    if (!ok) {
        std::cout << "tcp echo failed: server_error=" << server_error
                  << " client0_error=" << clients[0].error
                  << " client1_error=" << clients[1].error << '\n';
        return 1;
    }

    std::cout << "echo0=";
    for (char ch : clients[0].response) {
        std::cout << ch;
    }
    std::cout << " echo1=";
    for (char ch : clients[1].response) {
        std::cout << ch;
    }
    std::cout << '\n';
    return 0;
#else
    std::cout << "tcp echo server example requires POSIX sockets\n";
    return 0;
#endif
}
