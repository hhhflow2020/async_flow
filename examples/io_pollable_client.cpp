#include <iostream>

#include "support/io_pollable_client_peer.hpp"
#include "support/io_pollable_client_task.hpp"

#if defined(__linux__)
#include <sys/socket.h>
#include <unistd.h>
#endif

int main() {
#if defined(__linux__)
    using namespace io_pollable_client_example;

    client_async::init();
    if (!client_async::io_backend_available(ClientThreads::IO_0)) {
        std::cout << "epoll backend unavailable\n";
        client_async::shutdown();
        return 0;
    }

    int fds[2]{-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
        std::cout << "socketpair failed\n";
        client_async::shutdown();
        return 1;
    }

    const bool started = client_async::start_task<PollableClientTask>(fds[0]);
    AF_ASSERT(started);
    if (!started) {
        ::close(fds[0]);
        ::close(fds[1]);
        client_async::shutdown();
        return 1;
    }

    echo_peer_once(fds[1]);
    client_async::shutdown();

    ::close(fds[0]);
    ::close(fds[1]);
    return 0;
#else
    std::cout << "pollable client example is Linux-only\n";
    return 0;
#endif
}
