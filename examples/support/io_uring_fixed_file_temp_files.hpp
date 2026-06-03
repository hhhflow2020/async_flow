#pragma once

#include <cstdlib>

#include "io_uring_fixed_file_runtime.hpp"

#if defined(__linux__)

namespace io_uring_fixed_file_example {

struct FixedFileTempFiles {
    char path[64]{"/tmp/asyncflow-fixed-file-XXXXXX"};
    char updated_path[72]{"/tmp/asyncflow-fixed-file-update-XXXXXX"};
    af::UniqueFd file{};
    af::UniqueFd updated_file{};

    bool create() {
        const int fd = ::mkstemp(path);
        if (fd < 0) {
            return false;
        }
        file.reset(fd);

        const int updated_fd = ::mkstemp(updated_path);
        if (updated_fd < 0) {
            cleanup();
            return false;
        }
        updated_file.reset(updated_fd);

        const char updated_payload = 'U';
        if (::write(updated_file.get(), &updated_payload, sizeof(updated_payload)) !=
            static_cast<ssize_t>(sizeof(updated_payload))) {
            cleanup();
            return false;
        }
        return true;
    }

    void cleanup() noexcept {
        updated_file.reset();
        file.reset();
        static_cast<void>(::unlink(updated_path));
        static_cast<void>(::unlink(path));
    }
};

} // namespace io_uring_fixed_file_example

#else

namespace io_uring_fixed_file_example {

struct FixedFileTempFiles {
    af::UniqueFd file{};
    af::UniqueFd updated_file{};

    bool create() noexcept {
        return false;
    }

    void cleanup() noexcept {}
};

} // namespace io_uring_fixed_file_example

#endif
