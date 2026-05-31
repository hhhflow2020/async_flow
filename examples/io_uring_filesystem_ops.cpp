#include <cerrno>
#include <iostream>

#include "support/io_uring_filesystem_ops_paths.hpp"
#include "support/io_uring_filesystem_ops_task.hpp"

int main() {
#if !defined(__linux__)
    std::cout << "io_uring filesystem ops example is Linux-only\n";
    return 0;
#else
    using namespace io_uring_filesystem_ops_example;

    fs_async::init();
    if (!fs_async::io_uring_backend_available(FsThread::IO_0)) {
        std::cout << "io_uring backend unavailable\n";
        fs_async::shutdown();
        return 0;
    }

    FsExamplePaths paths{};
    if (!paths.create()) {
        std::cerr << "failed to create filesystem example paths\n";
        fs_async::shutdown();
        return 1;
    }

    FsResult result{};
    const bool scheduled = fs_async::start_task<FilesystemOpsTask>(
        paths.dir_path,
        paths.file_path,
        paths.hardlink_path,
        paths.symlink_path,
        &result);
    if (!scheduled) {
        std::cerr << "failed to schedule filesystem task\n";
        fs_async::shutdown();
        paths.cleanup();
        return 1;
    }

    fs_async::wait_for_idle();
    fs_async::shutdown();
    paths.cleanup();

    if (result.error == EINVAL || result.error == EOPNOTSUPP || result.error == ENOSYS) {
        std::cout << "kernel does not support one of the requested io_uring fs opcodes\n";
        return 0;
    }
    if (result.error != 0) {
        std::cerr << "filesystem task failed: " << result.error << "\n";
        return 1;
    }

    std::cout << "filesystem lifecycle complete, statx size=" << result.observed_size << "\n";
    return result.observed_size == 1U ? 0 : 1;
#endif
}
