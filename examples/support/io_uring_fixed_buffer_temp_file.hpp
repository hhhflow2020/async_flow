#pragma once

#include <cstdlib>

#include "io_uring_fixed_buffer_runtime.hpp"

#if defined(__linux__)
#include <unistd.h>
#endif

namespace io_uring_fixed_buffer_example {

#if defined(__linux__)

struct FixedBufferTempFile {
    char path[48]{"/tmp/asyncflow-fixed-buffer-XXXXXX"};
    af::UniqueFd file{};

    [[nodiscard]] bool create() noexcept {
        const int fd = ::mkstemp(path);
        if (fd < 0) {
            return false;
        }
        file.reset(fd);
        return true;
    }

    void cleanup() noexcept {
        file.reset();
        static_cast<void>(::unlink(path));
    }
};

#else

struct FixedBufferTempFile {
    af::UniqueFd file{};

    [[nodiscard]] bool create() noexcept {
        return false;
    }

    void cleanup() noexcept {}
};

#endif

} // namespace io_uring_fixed_buffer_example
