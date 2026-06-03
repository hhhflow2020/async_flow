#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

#include "support/io_epoll_read_task.hpp"
#include "support/io_epoll_socket_pair.hpp"

namespace {

bool wait_until(std::atomic<int> &value, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

} // namespace

int main() {
    using namespace io_epoll_example;

    if constexpr (!af::supports_epoll) {
        std::cout << "epoll IO example is Linux-only\n";
        return 0;
    }

    async::init();
    if (!async::io_backend_available(AppThreads::IO_0)) {
        std::cout << "epoll backend unavailable\n";
        async::shutdown();
        return 0;
    }

    EpollSocketPair sockets{};
    if (!sockets.create()) {
        std::cout << "socketpair failed\n";
        async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    const bool started = async::start_task<ReadOneByteTask>(sockets.reader.get(), &armed);
    AF_ASSERT(started);

    if (!started || !wait_until(armed, 1)) {
        std::cout << "read task did not arm\n";
        async::shutdown();
        return 1;
    }

    const char value = 'A';
    if (!sockets.write_byte(value)) {
        std::cout << "write byte failed\n";
        async::shutdown();
        return 1;
    }

    async::shutdown();
    return 0;
}
