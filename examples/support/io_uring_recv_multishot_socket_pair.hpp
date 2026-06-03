#pragma once

#include "io_uring_recv_multishot_runtime.hpp"
#include "posix_socket_flags.hpp"

#if defined(__linux__)

namespace io_uring_recv_multishot_example {

struct RecvMultishotSocketPair {
    af::UniqueFd receiver{};
    af::UniqueFd sender{};

    bool create() {
        int fds[2]{-1, -1};
        if (!asyncflow_examples::socket_pair_with_flags(AF_UNIX, SOCK_STREAM, 0, fds)) {
            return false;
        }
        receiver.reset(fds[0]);
        sender.reset(fds[1]);
        return true;
    }
};

inline bool write_payload_once(int fd, const char *payload, std::size_t size) {
    return ::write(fd, payload, size) == static_cast<ssize_t>(size);
}

} // namespace io_uring_recv_multishot_example

#else

namespace io_uring_recv_multishot_example {

struct RecvMultishotSocketPair {
    af::UniqueFd receiver{};
    af::UniqueFd sender{};

    bool create() noexcept {
        return false;
    }
};

inline bool write_payload_once(int fd, const char *payload, std::size_t size) {
    static_cast<void>(fd);
    static_cast<void>(payload);
    static_cast<void>(size);
    return false;
}

} // namespace io_uring_recv_multishot_example

#endif
