#include <iostream>

#include "support/io_uring_openat_path.hpp"
#include "support/io_uring_openat_task.hpp"

int main() {
    if constexpr (!af::supports_io_uring) {
        std::cout << "io_uring openat example is Linux-only\n";
        return 0;
    }

    using namespace io_uring_openat_example;

    openat_async::init();
    if (!openat_async::io_uring_backend_available(OpenAtThreads::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        openat_async::shutdown();
        return 0;
    }

    OpenAtTempPath temp_path{};
    if (!temp_path.create()) {
        std::cerr << "temp path creation failed\n";
        openat_async::shutdown();
        return 1;
    }

    char byte_read{0};
    const bool started = openat_async::start_task<OpenAtRoundTripTask>(temp_path.path, &byte_read);
    if (!started) {
        std::cerr << "io_uring openat task did not start\n";
        openat_async::shutdown();
        return 1;
    }

    openat_async::shutdown();
    std::cout << "io_uring openat byte=" << byte_read << '\n';
    return 0;
}
