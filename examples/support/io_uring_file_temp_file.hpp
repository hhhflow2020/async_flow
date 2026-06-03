#pragma once

#include <array>
#include <cstdio>

#include "io_uring_file_runtime.hpp"

#if defined(__linux__)
#include <unistd.h>
#endif

namespace io_uring_file_example {

struct TempFile {
    ~TempFile() {
        cleanup();
    }

    [[nodiscard]] bool create() noexcept {
#if defined(__linux__)
        const int written =
            std::snprintf(path.data(), path.size(), "%s", "/tmp/asyncflow-file-XXXXXX");
        if (written < 0 || static_cast<std::size_t>(written) >= path.size()) {
            return false;
        }
        const int opened = ::mkstemp(path.data());
        if (opened < 0) {
            path[0] = '\0';
            return false;
        }
        fd.reset(opened);
        return true;
#else
        return false;
#endif
    }

    void cleanup() noexcept {
#if defined(__linux__)
        fd.reset();
        if (path[0] != '\0') {
            static_cast<void>(::unlink(path.data()));
            path[0] = '\0';
        }
#endif
    }

    std::array<char, 32> path{};
    af::UniqueFd fd{};
};

} // namespace io_uring_file_example
