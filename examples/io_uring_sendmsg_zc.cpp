#include <iostream>

#include "support/io_uring_sendmsg_zc_socket_pair.hpp"
#include "support/io_uring_sendmsg_zc_task.hpp"

int main() {
    if constexpr (!af::supports_io_uring) {
        std::cout << "sendmsg_zc example is Linux-only\n";
        return 0;
    }

    using namespace io_uring_sendmsg_zc_example;

    SendmsgZcSocketPair sockets{};
    if (!sockets.create()) {
        std::cerr << "socketpair failed\n";
        return 1;
    }

    sendmsg_zc_async::init();
    const bool has_uring = sendmsg_zc_async::io_uring_backend_available(SendmsgZcThreads::IO_0);
    std::size_t bytes_sent{0};
    const bool started = sendmsg_zc_async::start_task<SendmsgZcTask>(
        sockets.sender.get(), sendmsg_zc_first, sendmsg_zc_first_size, sendmsg_zc_second,
        sendmsg_zc_second_size, &bytes_sent);
    if (!started) {
        std::cerr << "sendmsg_zc task did not start\n";
        sendmsg_zc_async::shutdown();
        return 1;
    }
    sendmsg_zc_async::shutdown();

    char received[sendmsg_zc_payload_size]{};
    if (!sockets.read_payload_exact(received, sendmsg_zc_payload_size) ||
        !payload_matches(received)) {
        std::cerr << "payload mismatch\n";
        return 1;
    }

    std::cout << "sendmsg_zc bytes=" << bytes_sent
              << " io_uring=" << (has_uring ? "available" : "fallback") << '\n';
    return 0;
}
