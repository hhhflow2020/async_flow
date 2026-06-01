#include <array>
#include <iostream>

#include "support/io_uring_multishot_accept_sockets.hpp"
#include "support/io_uring_multishot_accept_task.hpp"

int main() {
#if defined(__linux__)
    using namespace io_uring_multishot_accept_example;

    accept_async::init();
    if (!accept_async::io_uring_backend_available(AcceptThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        accept_async::shutdown();
        return 0;
    }

    MultishotAcceptListener listener{};
    if (!listener.listen()) {
        std::cout << "listen failed\n";
        accept_async::shutdown();
        return 1;
    }

    constexpr int target_accepts = 2;
    std::array<af::UniqueFd, target_accepts> clients{};
    if (!connect_accept_clients(listener.address, listener.address_size, clients)) {
        std::cout << "client connect failed\n";
        accept_async::shutdown();
        return 1;
    }

    MultishotAcceptResult result{};
    const bool started =
        accept_async::start_task<MultishotAcceptTask>(listener.fd.get(), target_accepts, &result);
    AF_ASSERT(started);
    if (!started) {
        std::cout << "io_uring multishot accept task start failed\n";
        accept_async::shutdown();
        return 1;
    }
    accept_async::shutdown();

    if (result.error != 0) {
        if (multishot_accept_unsupported_error(result.error)) {
            std::cout << "io_uring multishot accept unsupported error=" << result.error << '\n';
            return 0;
        }
        std::cout << "io_uring multishot accept failed error=" << result.error << '\n';
        return 1;
    }

    std::cout << "io_uring multishot accepted=" << result.accepted_count << '\n';
    return 0;
#else
    std::cout << "io_uring multishot accept example is Linux-only\n";
    return 0;
#endif
}
