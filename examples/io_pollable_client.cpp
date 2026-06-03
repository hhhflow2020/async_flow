#include <iostream>

#include "support/io_pollable_client_peer.hpp"
#include "support/io_pollable_client_task.hpp"

int main() {
    using namespace io_pollable_client_example;

    client_async::init();
    if (!client_async::io_backend_available(ClientThreads::IO_0)) {
        std::cout << "IO backend unavailable\n";
        client_async::shutdown();
        return 0;
    }
    std::cout << "pollable client backend="
              << af::runtime_io_backend_name<client_async>(ClientThreads::IO_0) << '\n';

    PollableSocketPair sockets{};
    if (!sockets.create()) {
        std::cout << "socketpair failed\n";
        client_async::shutdown();
        return 1;
    }

    const bool started = client_async::start_task<PollableClientTask>(sockets.client.get());
    AF_ASSERT(started);
    if (!started) {
        client_async::shutdown();
        return 1;
    }

    echo_peer_once(sockets.peer.get());
    client_async::shutdown();

    return 0;
}
