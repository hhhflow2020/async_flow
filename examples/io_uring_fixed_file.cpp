#include <iostream>

#include "support/io_uring_fixed_file_task.hpp"
#include "support/io_uring_fixed_file_temp_files.hpp"

int main() {
#if defined(__linux__)
    using namespace io_uring_fixed_file_example;

    fixed_file_async::init();
    if (!fixed_file_async::io_uring_backend_available(FixedFileThreads::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        fixed_file_async::shutdown();
        return 0;
    }

    FixedFileTempFiles temp_files{};
    if (!temp_files.create()) {
        std::cout << "temporary file setup failed\n";
        fixed_file_async::shutdown();
        return 1;
    }

    int error = 0;
    char byte_read = 0;
    int vectored_read = 0;
    char updated_byte_read = 0;
    const bool started = fixed_file_async::start_task<FixedFileRoundTripTask>(
        temp_files.file.get(), temp_files.updated_file.get(), &error, &byte_read, &vectored_read,
        &updated_byte_read);
    AF_ASSERT(started);

    if (!started) {
        std::cout << "io_uring fixed file task start failed\n";
        temp_files.cleanup();
        fixed_file_async::shutdown();
        return 1;
    }

    fixed_file_async::shutdown();

    if (error != 0) {
        std::cout << "io_uring fixed file failed error=" << error << '\n';
        temp_files.cleanup();
        return 1;
    }

    const int vectored = vectored_read;
    std::cout << "io_uring fixed file byte=" << byte_read
              << " vectored=" << static_cast<char>((vectored >> 8) & 0xff)
              << static_cast<char>(vectored & 0xff) << " updated=" << updated_byte_read << '\n';
    temp_files.cleanup();
    return 0;
#else
    std::cout << "io_uring fixed file example is Linux-only\n";
    return 0;
#endif
}
