#include <iostream>

#include "support/io_uring_fixed_buffer_task.hpp"
#include "support/io_uring_fixed_buffer_temp_file.hpp"

int main() {
    if constexpr (!af::supports_io_uring) {
        std::cout << "io_uring fixed buffer example is Linux-only\n";
        return 0;
    }

    using namespace io_uring_fixed_buffer_example;

    fixed_async::init();
    if (!fixed_async::io_uring_backend_available(FixedBufferThreads::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        fixed_async::shutdown();
        return 0;
    }

    FixedBufferTempFile temp_file{};
    if (!temp_file.create()) {
        std::cout << "temporary file setup failed\n";
        fixed_async::shutdown();
        return 1;
    }

    FixedBufferRoundTripResult result{};
    const bool started =
        fixed_async::start_task<FixedBufferRoundTripTask>(temp_file.file.get(), &result);
    AF_ASSERT(started);

    if (!started) {
        std::cout << "io_uring fixed buffer task did not start\n";
        temp_file.cleanup();
        fixed_async::shutdown();
        return 1;
    }

    fixed_async::shutdown();

    if (result.error != 0) {
        std::cout << "io_uring fixed buffer failed error=" << result.error << '\n';
        temp_file.cleanup();
        return 1;
    }

    std::cout << "io_uring fixed buffer byte=" << result.byte_read << '\n';
    temp_file.cleanup();
    return 0;
}
