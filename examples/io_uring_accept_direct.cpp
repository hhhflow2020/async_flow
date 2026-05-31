#include <atomic>
#include <cerrno>
#include <iostream>

#include "support/io_uring_accept_direct_socket_helpers.hpp"
#include "support/io_uring_accept_direct_task.hpp"

int main() {
#if defined(__linux__)
    using namespace io_uring_accept_direct_example;

    direct_accept_async::init();
    if (!direct_accept_async::io_uring_backend_available(DirectAcceptThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        direct_accept_async::shutdown();
        return 0;
    }

    af::UniqueFd listener;
    sockaddr_in address{};
    if (!create_loopback_listener(listener, address)) {
        std::cerr << "listener setup failed\n";
        direct_accept_async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    std::atomic<int> error{0};
    int packed_read = 0;
    const bool started = direct_accept_async::start_task<DirectAcceptRoundTripTask>(
        listener.get(),
        &armed,
        &error,
        &packed_read);
    AF_ASSERT(started);
    if (!started) {
        direct_accept_async::shutdown();
        return 1;
    }

    if (!wait_until_armed_or_error(armed, error)) {
        std::cerr << "io_uring accept direct task did not arm\n";
        direct_accept_async::shutdown();
        return 1;
    }
    if (armed.load(std::memory_order_acquire) == 0) {
        const int task_error = error.load(std::memory_order_acquire);
        std::cout << "io_uring accept direct "
                  << (unsupported_direct_accept_error(task_error) ? "unsupported" : "failed")
                  << " error=" << task_error << '\n';
        direct_accept_async::shutdown();
        return unsupported_direct_accept_error(task_error) ? 0 : 1;
    }

    af::UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!client) {
        std::cerr << "client socket failed\n";
        direct_accept_async::shutdown();
        return 1;
    }
    const int connect_rc = ::connect(
        client.get(),
        reinterpret_cast<sockaddr*>(&address),
        sizeof(address));
    if (connect_rc != 0 && errno != EINPROGRESS) {
        std::cerr << "client connect failed\n";
        direct_accept_async::shutdown();
        return 1;
    }

    const char request[2]{'A', 'B'};
    if (!write_exact_until(client.get(), request, sizeof(request))) {
        std::cerr << "client write failed\n";
        direct_accept_async::shutdown();
        return 1;
    }

    direct_accept_async::shutdown();

    const int task_error = error.load(std::memory_order_acquire);
    if (task_error != 0) {
        std::cout << "io_uring accept direct "
                  << (unsupported_direct_accept_error(task_error) ? "unsupported" : "failed")
                  << " error=" << task_error << '\n';
        return unsupported_direct_accept_error(task_error) ? 0 : 1;
    }

    char response[2]{};
    if (!read_exact_until(client.get(), response, sizeof(response))) {
        std::cerr << "client read failed\n";
        return 1;
    }

    std::cout << "io_uring accept direct packed="
              << packed_read
              << " response=" << response[0] << response[1] << '\n';
    return 0;
#else
    std::cout << "io_uring accept direct example is Linux-only\n";
    return 0;
#endif
}
