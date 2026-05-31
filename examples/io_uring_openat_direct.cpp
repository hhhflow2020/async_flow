#include <iostream>

#include "support/io_uring_openat_direct_path.hpp"
#include "support/io_uring_openat_direct_task.hpp"

int main() {
#if defined(__linux__)
    using namespace io_uring_openat_direct_example;

    direct_open_async::init();
    if (!direct_open_async::io_uring_backend_available(DirectOpenThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        direct_open_async::shutdown();
        return 0;
    }

    DirectOpenTempPath temp_path{};
    if (!temp_path.create()) {
        std::cout << "mkstemp failed\n";
        direct_open_async::shutdown();
        return 1;
    }

    DirectOpenRoundTripResult result{};
    const bool started = direct_open_async::start_task<DirectOpenRoundTripTask>(
        temp_path.path,
        &result);
    AF_ASSERT(started);

    if (!started) {
        std::cout << "io_uring openat direct task did not start\n";
        direct_open_async::shutdown();
        return 1;
    }
    direct_open_async::shutdown();

    if (result.error != 0) {
        std::cout << "io_uring openat direct "
                  << (unsupported_direct_open_error(result.error) ? "unsupported" : "failed")
                  << " error=" << result.error << '\n';
        return unsupported_direct_open_error(result.error) ? 0 : 1;
    }

    std::cout << "io_uring openat direct byte=" << result.byte_read << '\n';
    return 0;
#else
    std::cout << "io_uring openat direct example is Linux-only\n";
    return 0;
#endif
}
