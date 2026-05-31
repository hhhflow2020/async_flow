#pragma once

#include "io_uring_openat_direct_runtime.hpp"

#if defined(__linux__)
#include <unistd.h>

namespace io_uring_openat_direct_example {

struct DirectOpenTempPath {
    char path[40]{"/tmp/asyncflow-openat-direct-XXXXXX"};
    bool created{false};

    bool create() noexcept {
        int seed = ::mkstemp(path);
        if (seed < 0) {
            return false;
        }
        ::close(seed);
        static_cast<void>(::unlink(path));
        created = true;
        return true;
    }

    void cleanup() noexcept {
        if (created) {
            static_cast<void>(::unlink(path));
            created = false;
        }
    }

    ~DirectOpenTempPath() {
        cleanup();
    }
};

} // namespace io_uring_openat_direct_example

#endif
