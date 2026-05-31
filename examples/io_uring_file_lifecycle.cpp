#include <cstdint>
#include <iostream>

#include "support/io_uring_file_lifecycle_paths.hpp"
#include "support/io_uring_file_lifecycle_task.hpp"

int main() {
#if defined(__linux__)
    using namespace io_uring_file_lifecycle_example;

    lifecycle_async::init();
    if (!lifecycle_async::io_uring_backend_available(LifecycleThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        lifecycle_async::shutdown();
        return 0;
    }

    FileLifecyclePaths paths{};
    if (!paths.create()) {
        std::cerr << "path formatting failed\n";
        lifecycle_async::shutdown();
        return 1;
    }

    std::uint64_t observed_size = 0;
    const bool started = lifecycle_async::start_task<FileLifecycleTask>(
        paths.path.data(),
        paths.renamed_path.data(),
        &observed_size);
    if (!started) {
        std::cerr << "io_uring lifecycle task start failed\n";
        paths.cleanup();
        lifecycle_async::shutdown();
        return 1;
    }

    lifecycle_async::shutdown();
    std::cout << "io_uring lifecycle size=" << observed_size << '\n';
    return 0;
#else
    std::cout << "io_uring lifecycle example is Linux-only\n";
    return 0;
#endif
}
