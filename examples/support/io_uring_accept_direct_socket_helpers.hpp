#pragma once

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <thread>

#include "io_uring_accept_direct_runtime.hpp"
#include "posix_socket_flags.hpp"

namespace io_uring_accept_direct_example {

[[nodiscard]] inline bool unsupported_direct_accept_error(int error) noexcept {
    return error == EINVAL || error == EBADF
#ifdef ENOSYS
           || error == ENOSYS
#endif
#ifdef ENXIO
           || error == ENXIO
#endif
#ifdef EOPNOTSUPP
           || error == EOPNOTSUPP
#endif
        ;
}

#if defined(__linux__)

inline bool create_loopback_listener(af::UniqueFd &listener, sockaddr_in &address) {
    listener.reset(asyncflow_examples::socket_with_flags(AF_INET, SOCK_STREAM, 0));
    if (!listener) {
        return false;
    }

    const int one = 1;
    static_cast<void>(::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)));

    address = sockaddr_in{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listener.get(), reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0 ||
        ::listen(listener.get(), 16) != 0) {
        return false;
    }

    socklen_t address_size = sizeof(address);
    return ::getsockname(listener.get(), reinterpret_cast<sockaddr *>(&address), &address_size) ==
           0;
}

inline bool write_exact_until(int fd, const char *input, std::size_t size) {
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

inline bool read_exact_until(int fd, char *output, std::size_t size) {
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

struct DirectAcceptLoopbackPeer {
    [[nodiscard]] bool create_listener() noexcept {
        return create_loopback_listener(listener, address_);
    }

    [[nodiscard]] bool connect_client() noexcept {
        client.reset(asyncflow_examples::socket_with_flags(AF_INET, SOCK_STREAM, 0));
        if (!client) {
            return false;
        }
        const int rc =
            ::connect(client.get(), reinterpret_cast<sockaddr *>(&address_), sizeof(address_));
        return rc == 0 || errno == EINPROGRESS;
    }

    [[nodiscard]] bool write_request(const char *request, std::size_t size) noexcept {
        return client && write_exact_until(client.get(), request, size);
    }

    [[nodiscard]] bool read_response(char *response, std::size_t size) noexcept {
        return client && read_exact_until(client.get(), response, size);
    }

    af::UniqueFd listener{};
    af::UniqueFd client{};

private:
    sockaddr_in address_{};
};

#else

struct DirectAcceptLoopbackPeer {
    [[nodiscard]] bool create_listener() noexcept {
        return false;
    }

    [[nodiscard]] bool connect_client() noexcept {
        return false;
    }

    [[nodiscard]] bool write_request(const char *request, std::size_t size) noexcept {
        static_cast<void>(request);
        static_cast<void>(size);
        return false;
    }

    [[nodiscard]] bool read_response(char *response, std::size_t size) noexcept {
        static_cast<void>(response);
        static_cast<void>(size);
        return false;
    }

    af::UniqueFd listener{};
    af::UniqueFd client{};
};

#endif

} // namespace io_uring_accept_direct_example
