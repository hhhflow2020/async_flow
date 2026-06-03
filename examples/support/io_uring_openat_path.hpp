#pragma once

#include "io_uring_openat_runtime.hpp"

#if defined(__linux__)
#include <unistd.h>
#endif

namespace io_uring_openat_example {

#if defined(__linux__)

struct OpenAtTempPath {
    char path[40]{"/tmp/asyncflow-openat-XXXXXX"};
    bool created{false};

    [[nodiscard]] bool create() noexcept {
        const int seed = ::mkstemp(path);
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

    ~OpenAtTempPath() {
        cleanup();
    }
};

#else

struct OpenAtTempPath {
    char path[40]{};

    [[nodiscard]] bool create() noexcept {
        return false;
    }

    void cleanup() noexcept {}

    ~OpenAtTempPath() {
        cleanup();
    }
};

#endif

} // namespace io_uring_openat_example
