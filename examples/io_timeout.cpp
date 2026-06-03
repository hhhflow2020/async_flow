#include <cerrno>
#include <chrono>
#include <iostream>

#include "support/io_timeout_read_task.hpp"
#include "support/io_timeout_socket_pair.hpp"

int main() {
    using namespace io_timeout_example;

    async::init();
    if (!async::io_backend_available(AppThreads::IO_0)) {
        std::cout << "IO backend unavailable\n";
        async::shutdown();
        return 0;
    }
    std::cout << "IO timeout backend=" << af::runtime_io_backend_name<async>(AppThreads::IO_0)
              << '\n';

    TimeoutSocketPair sockets{};
    if (!sockets.create()) {
        std::cerr << "socketpair failed\n";
        async::shutdown();
        return 1;
    }

    int error = 0;
    if (!async::start_task<ReadWithTimeoutTask>(sockets.reader.get(), std::chrono::milliseconds(5),
                                                &error)) {
        std::cerr << "timeout task did not start\n";
        async::shutdown();
        return 1;
    }

    async::shutdown();
    std::cout << "read timeout error=" << error << '\n';
    return error == ETIMEDOUT ? 0 : 1;
}
