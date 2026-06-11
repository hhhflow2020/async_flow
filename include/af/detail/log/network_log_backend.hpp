#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include "af/detail/log/log_backend.hpp"

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#if defined(__linux__)
#include <sys/syscall.h>
#endif
#include <sys/uio.h>
#include <unistd.h>

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

[[nodiscard]] inline bool set_log_socket_nonblocking(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

[[nodiscard]] inline std::string log_port_string(std::uint16_t port) {
    return std::to_string(static_cast<unsigned>(port));
}

#if defined(__linux__)
struct LogMmsgHeader {
    msghdr msg_hdr{};
    unsigned int msg_len{0};
};

[[nodiscard]] inline int log_sendmmsg(int fd, LogMmsgHeader *messages, unsigned int count,
                                      int flags) noexcept {
    return static_cast<int>(::syscall(SYS_sendmmsg, fd, messages, count, flags));
}
#endif

} // namespace detail

class UdpLogBackend final : public log_backend {
public:
    explicit UdpLogBackend(UdpLogBackendConfig config) : config_(std::move(config)) {}

    ~UdpLogBackend() override {
        detail::close_log_socket(fd_);
    }

    void write_batch(af::Span<detail::LogRecord *const> records) noexcept override {
        if (records.empty() || !ensure_socket()) {
            return;
        }
#if defined(__linux__)
        send_batch_best_effort(records);
#else
        for (detail::LogRecord *record : records) {
            std::string_view message = record->message();
            const std::size_t size = std::min(message.size(), config_.max_datagram_size);
            send_best_effort(message.data(), size);
        }
#endif
    }

private:
    static constexpr std::size_t max_message_count = 64;

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
            if (!detail::set_log_socket_nonblocking(candidate)) {
                static_cast<void>(::close(candidate));
                continue;
            }
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

#if defined(__linux__)
    void send_batch_best_effort(af::Span<detail::LogRecord *const> records) noexcept {
        std::size_t index = 0;
        while (index < records.size() && fd_ >= 0) {
            std::size_t count = 0;
            while (index < records.size() && count < messages_.size()) {
                std::string_view message = records[index]->message();
                ++index;
                const std::size_t size = std::min(message.size(), config_.max_datagram_size);
                if (size == 0U) {
                    continue;
                }

                iovecs_[count].iov_base = const_cast<char *>(message.data());
                iovecs_[count].iov_len = size;
                messages_[count] = {};
                messages_[count].msg_hdr.msg_iov = &iovecs_[count];
                messages_[count].msg_hdr.msg_iovlen = 1;
                ++count;
            }
            if (count == 0U || !sendmmsg_best_effort(messages_.data(), count)) {
                return;
            }
        }
    }

    [[nodiscard]] bool sendmmsg_best_effort(detail::LogMmsgHeader *messages,
                                            std::size_t count) noexcept {
        while (count != 0U && fd_ >= 0) {
            const int sent = detail::log_sendmmsg(fd_, messages, static_cast<unsigned int>(count),
                                                  detail::log_send_flags());
            if (sent > 0) {
                const auto sent_count = static_cast<std::size_t>(sent);
                if (sent_count >= count) {
                    return true;
                }
                messages += sent_count;
                count -= sent_count;
                continue;
            }
            if (sent == 0) {
                return false;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                detail::close_log_socket(fd_);
            }
            return false;
        }
        return count == 0U;
    }
#endif

    UdpLogBackendConfig config_;
    int fd_{-1};
#if defined(__linux__)
    std::array<iovec, max_message_count> iovecs_{};
    std::array<detail::LogMmsgHeader, max_message_count> messages_{};
#endif
};

class TcpLogBackend final : public log_backend {
public:
    explicit TcpLogBackend(TcpLogBackendConfig config) : config_(std::move(config)) {}

    ~TcpLogBackend() override {
        detail::close_log_socket(fd_);
    }

    void write_batch(af::Span<detail::LogRecord *const> records) noexcept override {
        if (records.empty() || !ensure_socket()) {
            return;
        }
        send_batch_best_effort(records);
    }

private:
    static constexpr std::size_t max_iov_count = 64;

    [[nodiscard]] bool ensure_socket() noexcept {
        if (fd_ >= 0) {
            return !connect_pending_ || finish_connect_if_ready();
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
            if (!detail::set_log_socket_nonblocking(candidate)) {
                static_cast<void>(::close(candidate));
                continue;
            }
            if (::connect(candidate, entry->ai_addr, entry->ai_addrlen) == 0) {
                fd_ = candidate;
                connect_pending_ = false;
                break;
            }
            if (errno == EINPROGRESS || errno == EALREADY || errno == EWOULDBLOCK) {
                fd_ = candidate;
                connect_pending_ = true;
                break;
            }
            static_cast<void>(::close(candidate));
        }

        ::freeaddrinfo(result);
        return fd_ >= 0 && (!connect_pending_ || finish_connect_if_ready());
    }

    [[nodiscard]] bool finish_connect_if_ready() noexcept {
        pollfd poll_fd{};
        poll_fd.fd = fd_;
        poll_fd.events = POLLOUT;
        const int ready = ::poll(&poll_fd, 1, 0);
        if (ready < 0) {
            if (errno == EINTR) {
                return false;
            }
            close_socket();
            return false;
        }
        if ((poll_fd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0 &&
            (poll_fd.revents & POLLOUT) == 0) {
            close_socket();
            return false;
        }
        if (ready == 0 || (poll_fd.revents & POLLOUT) == 0) {
            return false;
        }

        int error = 0;
        socklen_t error_size = sizeof(error);
        if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &error, &error_size) != 0 || error != 0) {
            close_socket();
            return false;
        }

        connect_pending_ = false;
        return true;
    }

    void send_batch_best_effort(af::Span<detail::LogRecord *const> records) noexcept {
        std::size_t index = 0;
        while (index < records.size() && fd_ >= 0) {
            std::size_t count = 0;
            while (index < records.size() && count < iovecs_.size()) {
                std::string_view message = records[index]->message();
                ++index;
                if (message.empty()) {
                    continue;
                }
                iovecs_[count].iov_base = const_cast<char *>(message.data());
                iovecs_[count].iov_len = message.size();
                ++count;
            }
            if (count != 0U) {
                sendmsg_all(iovecs_.data(), count);
            }
        }
    }

    void sendmsg_all(iovec *iovecs, std::size_t count) noexcept {
        while (count != 0U) {
            msghdr message{};
            message.msg_iov = iovecs;
            message.msg_iovlen = count;
            const auto sent = ::sendmsg(fd_, &message, detail::log_send_flags());
            if (sent > 0) {
                trim_iovecs(iovecs, count, static_cast<std::size_t>(sent));
                continue;
            }
            if (sent == 0) {
                close_socket();
                return;
            }
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            }
            close_socket();
            return;
        }
    }

    static void trim_iovecs(iovec *&iovecs, std::size_t &count, std::size_t sent) noexcept {
        while (count != 0U && sent >= iovecs[0].iov_len) {
            sent -= iovecs[0].iov_len;
            ++iovecs;
            --count;
        }
        if (count != 0U && sent != 0U) {
            auto *base = static_cast<char *>(iovecs[0].iov_base);
            iovecs[0].iov_base = base + sent;
            iovecs[0].iov_len -= sent;
        }
    }

    void close_socket() noexcept {
        detail::close_log_socket(fd_);
        connect_pending_ = false;
    }

    TcpLogBackendConfig config_;
    int fd_{-1};
    bool connect_pending_{false};
    std::chrono::steady_clock::time_point next_connect_time_{};
    std::array<iovec, max_iov_count> iovecs_{};
};

[[nodiscard]] inline std::unique_ptr<log_backend> make_udp_log_backend(UdpLogBackendConfig config) {
    return std::make_unique<UdpLogBackend>(std::move(config));
}

[[nodiscard]] inline std::unique_ptr<log_backend> make_tcp_log_backend(TcpLogBackendConfig config) {
    return std::make_unique<TcpLogBackend>(std::move(config));
}

} // namespace af
