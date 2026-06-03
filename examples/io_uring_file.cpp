#include <iostream>

#include "support/io_uring_file_task.hpp"
#include "support/io_uring_file_temp_file.hpp"

int main() {
    if constexpr (!af::supports_io_uring) {
        std::cout << "io_uring file example is Linux-only\n";
        return 0;
    }

    using namespace io_uring_file_example;

    file_async::init();
    if (!file_async::io_uring_backend_available(FileThreads::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        file_async::shutdown();
        return 0;
    }

    TempFile file{};
    if (!file.create()) {
        std::cout << "mkstemp failed\n";
        file_async::shutdown();
        return 1;
    }

    char byte_read{0};
    const bool started = file_async::start_task<FileRoundTripTask>(file.fd.get(), &byte_read);
    AF_ASSERT(started);

    if (!started) {
        std::cout << "io_uring file task did not start\n";
        file_async::shutdown();
        return 1;
    }

    file_async::shutdown();
    std::cout << "io_uring file byte=" << byte_read << '\n';
    return 0;
}
