#pragma once

#include <cstdio>

#include "io_sendfile_static_runtime.hpp"

#if defined(__linux__)
#include <fcntl.h>
#include <unistd.h>

namespace io_sendfile_static_example {

struct SendfileStaticFile {
    af::UniqueFd fd{};

    bool create() noexcept {
        char path[96]{};
        const int path_written =
            std::snprintf(path, sizeof(path), "/tmp/asyncflow-sendfile-static-%ld",
                          static_cast<long>(::getpid()));
        if (path_written < 0 || static_cast<std::size_t>(path_written) >= sizeof(path)) {
            return false;
        }

        fd.reset(::open(path, O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC, 0600));
        static_cast<void>(::unlink(path));
        if (!fd) {
            return false;
        }

        const ssize_t written = ::write(fd.get(), sendfile_payload, sendfile_payload_size);
        return written == static_cast<ssize_t>(sendfile_payload_size);
    }
};

} // namespace io_sendfile_static_example

#else

namespace io_sendfile_static_example {

struct SendfileStaticFile {
    af::UniqueFd fd{};

    bool create() noexcept {
        return false;
    }
};

} // namespace io_sendfile_static_example

#endif
