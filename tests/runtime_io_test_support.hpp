#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <limits>
#include <thread>
#include <type_traits>

#include <gtest/gtest.h>

#include "af/async_flow.hpp"

#include <sys/uio.h>

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__linux__)
#include <linux/openat2.h>
#endif

namespace {

#include "support/runtime_io_core.hpp"

#include "support/runtime_io_basic_tasks.hpp"

#include "support/runtime_io_basic_socket_tasks.hpp"

#include "support/runtime_io_stream_basic_tasks.hpp"

#include "support/runtime_io_stream_vectored_tasks.hpp"

#include "support/runtime_io_stream_zc_send_tasks.hpp"

#include "support/runtime_io_stream_send_zc_pending_tasks.hpp"

#include "support/runtime_io_wait_cancel_tasks.hpp"

void close_pair(int fds[2]) {
    if (fds[0] >= 0) {
        ::close(fds[0]);
    }
    if (fds[1] >= 0) {
        ::close(fds[1]);
    }
}

bool set_nonblocking_cloexec(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
        return false;
    }
    const int fd_flags = ::fcntl(fd, F_GETFD, 0);
    return fd_flags >= 0 && ::fcntl(fd, F_SETFD, fd_flags | FD_CLOEXEC) == 0;
}

bool create_socket_pair(int fds[2]) {
    fds[0] = -1;
    fds[1] = -1;
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        return false;
    }
    if (set_nonblocking_cloexec(fds[0]) && set_nonblocking_cloexec(fds[1])) {
        return true;
    }
    close_pair(fds);
    fds[0] = -1;
    fds[1] = -1;
    return false;
}

bool fill_until_blocked(int fd) {
    char data[4096]{};
    bool blocked = false;
    for (;;) {
        const ssize_t n = ::write(fd, data, sizeof(data));
        if (n > 0) {
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            blocked = true;
        }
        break;
    }
    return blocked;
}

void drain_available(int fd) {
    char data[4096]{};
    for (;;) {
        const ssize_t n = ::read(fd, data, sizeof(data));
        if (n > 0) {
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
}

bool read_exact_until(int fd, char *output, std::size_t size) {
    std::size_t offset = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (offset < size) {
        const ssize_t n = ::read(fd, output + offset, size - offset);
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0) {
            return false;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

bool write_exact_until(int fd, const char *input, std::size_t size) {
    std::size_t offset = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (offset < size) {
        const ssize_t n = ::write(fd, input + offset, size - offset);
        if (n > 0) {
            offset += static_cast<std::size_t>(n);
            continue;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != ENOTCONN) {
            return false;
        }
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

#include "support/runtime_io_udp_socket_helpers.hpp"

#if defined(__linux__)
#include "support/runtime_io_accept_tasks.hpp"

#include "support/runtime_io_stream_tasks.hpp"

#include "support/runtime_io_file_tasks.hpp"

#include "support/runtime_io_uring_socket_tasks.hpp"

#include "support/runtime_io_timer_event_tasks.hpp"

#include "support/runtime_io_socket_lifecycle_tasks.hpp"

bool create_tcp_listener(int &listener, sockaddr_in &address, socklen_t &address_size) {
    listener = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listener < 0) {
        return false;
    }

    address = sockaddr_in{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener, reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
        ::listen(listener, 16) != 0) {
        ::close(listener);
        listener = -1;
        return false;
    }

    address_size = sizeof(address);
    if (::getsockname(listener, reinterpret_cast<sockaddr *>(&address), &address_size) != 0) {
        ::close(listener);
        listener = -1;
        return false;
    }
    return true;
}

int accept_tcp_until_ready(int listener) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (;;) {
        const int fd = ::accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd >= 0) {
            return fd;
        }
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (std::chrono::steady_clock::now() > deadline) {
            return -1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void close_fd(int &fd) {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

#include "support/runtime_io_stream_transfer_helpers.hpp"
#endif

} // namespace
