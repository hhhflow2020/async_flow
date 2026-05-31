#pragma once

#include "io_uring_recv_multishot_runtime.hpp"

#if defined(__linux__)

namespace io_uring_recv_multishot_example {

struct RecvMultishotSocketPair {
    af::UniqueFd receiver{};
    af::UniqueFd sender{};

    bool create() {
        int fds[2]{-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds) != 0) {
            return false;
        }
        receiver.reset(fds[0]);
        sender.reset(fds[1]);
        return true;
    }
};

inline bool write_payload_once(int fd, const char* payload, std::size_t size) {
    return ::write(fd, payload, size) == static_cast<ssize_t>(size);
}

} // namespace io_uring_recv_multishot_example

#endif
