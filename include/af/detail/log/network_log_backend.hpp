#pragma once

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include "af/detail/log/log_backend.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace af {

struct UdpLogBackendConfig {
    std::string host;
    std::uint16_t port{0};
    std::size_t max_datagram_size{1400};
};

struct TcpLogBackendConfig {
    std::string host;
    std::uint16_t port{0};
    std::chrono::milliseconds reconnect_interval{std::chrono::milliseconds(500)};
};

namespace detail {

#if !defined(_WIN32)
[[nodiscard]] inline int log_send_flags() noexcept {
#if defined(MSG_NOSIGNAL)
    return MSG_NOSIGNAL;
#else
    return 0;
#endif
}

inline void close_log_socket(int &fd) noexcept {
    if (fd >= 0) {
        static_cast<void>(::close(fd));
        fd = -1;
    }
}

inline void set_log_socket_nonblocking(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        static_cast<void>(::fcntl(fd, F_SETFL, flags | O_NONBLOCK));
    }
}

[[nodiscard]] inline std::string log_port_string(std::uint16_t port) {
    return std::to_string(static_cast<unsigned>(port));
}
#endif

} // namespace detail

class UdpLogBackend final : public LogBackend {
public:
    explicit UdpLogBackend(UdpLogBackendConfig config) : config_(std::move(config)) {}

    ~UdpLogBackend() override {
#if !defined(_WIN32)
        detail::close_log_socket(fd_);
#endif
    }

    void write_batch(std::span<detail::LogRecord *const> records) noexcept override {
#if defined(_WIN32)
        static_cast<void>(records);
#else
        if (records.empty() || !ensure_socket()) {
            return;
        }
        for (detail::LogRecord *record : records) {
            std::string_view message = record->message();
            const std::size_t size = std::min(message.size(), config_.max_datagram_size);
            send_best_effort(message.data(), size);
        }
#endif
    }

private:
#if !defined(_WIN32)
    [[nodiscard]] bool ensure_socket() noexcept {
        if (fd_ >= 0) {
            return true;
        }

        addrinfo hints{};
        hints.ai_socktype = SOCK_DGRAM;
        hints.ai_family = AF_UNSPEC;
        const std::string port = detail::log_port_string(config_.port);

        addrinfo *result = nullptr;
        if (::getaddrinfo(config_.host.c_str(), port.c_str(), &hints, &result) != 0) {
            return false;
        }

        for (addrinfo *entry = result; entry != nullptr; entry = entry->ai_next) {
            int candidate = ::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
            if (candidate < 0) {
                continue;
            }
            detail::set_log_socket_nonblocking(candidate);
            if (::connect(candidate, entry->ai_addr, entry->ai_addrlen) == 0) {
                fd_ = candidate;
                break;
            }
            static_cast<void>(::close(candidate));
        }

        ::freeaddrinfo(result);
        return fd_ >= 0;
    }

    void send_best_effort(const char *data, std::size_t size) noexcept {
        while (size != 0U) {
            const auto sent = ::send(fd_, data, size, detail::log_send_flags());
            if (sent >= 0) {
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                detail::close_log_socket(fd_);
            }
            return;
        }
    }
#endif

    UdpLogBackendConfig config_;
#if !defined(_WIN32)
    int fd_{-1};
#endif
};

class TcpLogBackend final : public LogBackend {
public:
    explicit TcpLogBackend(TcpLogBackendConfig config) : config_(std::move(config)) {}

    ~TcpLogBackend() override {
#if !defined(_WIN32)
        detail::close_log_socket(fd_);
#endif
    }

    void write_batch(std::span<detail::LogRecord *const> records) noexcept override {
#if defined(_WIN32)
        static_cast<void>(records);
#else
        if (records.empty() || !ensure_socket()) {
            return;
        }
        for (detail::LogRecord *record : records) {
            send_best_effort(record->message());
            if (fd_ < 0) {
                return;
            }
        }
#endif
    }

private:
#if !defined(_WIN32)
    [[nodiscard]] bool ensure_socket() noexcept {
        if (fd_ >= 0) {
            return true;
        }

        const auto now = std::chrono::steady_clock::now();
        if (now < next_connect_time_) {
            return false;
        }
        next_connect_time_ = now + config_.reconnect_interval;

        addrinfo hints{};
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_family = AF_UNSPEC;
        const std::string port = detail::log_port_string(config_.port);

        addrinfo *result = nullptr;
        if (::getaddrinfo(config_.host.c_str(), port.c_str(), &hints, &result) != 0) {
            return false;
        }

        for (addrinfo *entry = result; entry != nullptr; entry = entry->ai_next) {
            int candidate = ::socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
            if (candidate < 0) {
                continue;
            }
            detail::set_log_socket_nonblocking(candidate);
            if (::connect(candidate, entry->ai_addr, entry->ai_addrlen) == 0 ||
                errno == EINPROGRESS) {
                fd_ = candidate;
                break;
            }
            static_cast<void>(::close(candidate));
        }

        ::freeaddrinfo(result);
        return fd_ >= 0;
    }

    void send_best_effort(std::string_view message) noexcept {
        const char *data = message.data();
        std::size_t size = message.size();
        while (size != 0U) {
            const auto sent = ::send(fd_, data, size, detail::log_send_flags());
            if (sent > 0) {
                data += sent;
                size -= static_cast<std::size_t>(sent);
                continue;
            }
            if (sent == 0) {
                detail::close_log_socket(fd_);
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            detail::close_log_socket(fd_);
            return;
        }
    }
#endif

    TcpLogBackendConfig config_;
#if !defined(_WIN32)
    int fd_{-1};
    std::chrono::steady_clock::time_point next_connect_time_{};
#endif
};

[[nodiscard]] inline std::unique_ptr<LogBackend> make_udp_log_backend(UdpLogBackendConfig config) {
    return std::make_unique<UdpLogBackend>(std::move(config));
}

[[nodiscard]] inline std::unique_ptr<LogBackend> make_tcp_log_backend(TcpLogBackendConfig config) {
    return std::make_unique<TcpLogBackend>(std::move(config));
}

} // namespace af
