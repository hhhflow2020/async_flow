#include <atomic>
#include <iostream>

#include "support/io_uring_recv_multishot_socket_pair.hpp"
#include "support/io_uring_recv_multishot_task.hpp"

int main() {
#if defined(__linux__)
    using namespace io_uring_recv_multishot_example;

    recv_async::init();
    if (!recv_async::io_uring_backend_available(RecvThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        recv_async::shutdown();
        return 0;
    }

    RecvMultishotSocketPair sockets{};
    if (!sockets.create()) {
        std::cout << "socketpair failed\n";
        recv_async::shutdown();
        return 1;
    }

    std::atomic<int> armed{0};
    int packed_read = 0;
    std::atomic<int> error{0};
    const bool started = recv_async::start_task<RecvMultishotTask>(sockets.receiver.get(), &armed,
                                                                   &packed_read, &error);
    AF_ASSERT(started);

    if (!started || !wait_until_armed_or_error(armed, error)) {
        std::cout << "io_uring recv_multishot arm timed out\n";
        recv_async::shutdown();
        return 1;
    }
    if (armed.load(std::memory_order_acquire) == 0) {
        const int task_error = error.load(std::memory_order_acquire);
        if (task_error != 0) {
            std::cout << "io_uring recv_multishot unsupported error=" << task_error << '\n';
            recv_async::shutdown();
            return 0;
        }
        std::cout << "io_uring recv_multishot arm timed out\n";
        recv_async::shutdown();
        return 1;
    }

    const char payload[] = {'M', 'R'};
    if (!write_payload_once(sockets.sender.get(), payload, sizeof(payload))) {
        std::cout << "write payload failed\n";
        recv_async::shutdown();
        return 1;
    }

    recv_async::shutdown();

    const int task_error = error.load(std::memory_order_acquire);
    if (task_error != 0) {
        std::cout << "io_uring recv_multishot unsupported error=" << task_error << '\n';
        return 0;
    }

    const int packed = packed_read;
    std::cout << "io_uring recv_multishot bytes=" << static_cast<char>((packed >> 8) & 0xff)
              << static_cast<char>(packed & 0xff) << '\n';
    return 0;
#else
    std::cout << "io_uring recv_multishot example is Linux-only\n";
    return 0;
#endif
}
