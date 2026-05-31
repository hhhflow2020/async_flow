#pragma once

#include <array>
#include <cstdio>

#include "io_uring_file_lifecycle_runtime.hpp"

#if defined(__linux__)
#include <unistd.h>
#endif

namespace io_uring_file_lifecycle_example {

struct FileLifecyclePaths {
    std::array<char, 160> path{};
    std::array<char, 176> renamed_path{};

    bool create() {
#if defined(__linux__)
        const int path_written = std::snprintf(
            path.data(),
            path.size(),
            "/tmp/asyncflow-lifecycle-%ld",
            static_cast<long>(::getpid()));
        const int rename_written =
            std::snprintf(renamed_path.data(), renamed_path.size(), "%s.renamed", path.data());
        if (path_written < 0 ||
            rename_written < 0 ||
            static_cast<std::size_t>(path_written) >= path.size() ||
            static_cast<std::size_t>(rename_written) >= renamed_path.size()) {
            return false;
        }
        cleanup();
        return true;
#else
        return false;
#endif
    }

    void cleanup() noexcept {
#if defined(__linux__)
        static_cast<void>(::unlink(path.data()));
        static_cast<void>(::unlink(renamed_path.data()));
#endif
    }
};

} // namespace io_uring_file_lifecycle_example
