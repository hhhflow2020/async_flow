#pragma once

#include <cstdio>
#include <cstdlib>

#include "io_uring_filesystem_ops_runtime.hpp"

#if defined(__linux__)
#include <unistd.h>
#endif

namespace io_uring_filesystem_ops_example {

struct FsExamplePaths {
    char dir_path[64]{"/tmp/asyncflow-fsops-example-XXXXXX"};
    char file_path[sizeof(dir_path) + 8]{};
    char hardlink_path[sizeof(dir_path) + 12]{};
    char symlink_path[sizeof(dir_path) + 12]{};

    bool create() {
#if defined(__linux__)
        if (::mkdtemp(dir_path) == nullptr) {
            return false;
        }
        static_cast<void>(::rmdir(dir_path));
        std::snprintf(file_path, sizeof(file_path), "%s/file", dir_path);
        std::snprintf(hardlink_path, sizeof(hardlink_path), "%s/hard", dir_path);
        std::snprintf(symlink_path, sizeof(symlink_path), "%s/sym", dir_path);
        return true;
#else
        return false;
#endif
    }

    void cleanup() noexcept {
#if defined(__linux__)
        static_cast<void>(::unlink(file_path));
        static_cast<void>(::unlink(hardlink_path));
        static_cast<void>(::unlink(symlink_path));
        static_cast<void>(::rmdir(dir_path));
#endif
    }
};

} // namespace io_uring_filesystem_ops_example
