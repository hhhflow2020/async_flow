#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "af/detail/net/reactor/net_io_channel.hpp"
#include "af/detail/net/socket_address.hpp"
#include "af/net/detail/tcp_socket_ops.hpp"
#include "af/net/detail/tcp_state.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace af::net::detail {

template <typename Runtime> class TcpListenerShard {
public:
    using Shard = TcpServerShard<Runtime>;
    using ListenerContext = TcpListenerContext<Runtime>;

    TcpListenerShard(Shard *shard, std::shared_ptr<ListenerContext> context) noexcept
        : shard_(shard), context_(std::move(context)) {
        listener_channel_.owner = this;
        listener_channel_.on_event = &TcpListenerShard::on_listener_event;
    }

    TcpListenerShard(const TcpListenerShard &) = delete;
    TcpListenerShard &operator=(const TcpListenerShard &) = delete;

    ~TcpListenerShard() {
        close();
    }

    [[nodiscard]] ListenerId id() const noexcept {
        return context_ == nullptr ? ListenerId{} : context_->id;
    }

    [[nodiscard]] const TcpListenerOptions &options() const noexcept {
        return context_->options;
    }

    [[nodiscard]] std::shared_ptr<ListenerContext> context() const noexcept {
        return context_;
    }

    [[nodiscard]] int open() noexcept {
        if (context_ == nullptr) {
            return EINVAL;
        }
        ::af::detail::SocketAddress bind_address{};
        int address_error = 0;
        if (!::af::detail::socket_address_from_endpoint(context_->endpoint, bind_address,
                                                        address_error)) {
            return address_error == 0 ? EINVAL : address_error;
        }
        const bool unix_listener = bind_address.family == AF_UNIX;
        const char *unix_path = unix_listener ? context_->endpoint.address.c_str() : "";

        listener_fd_ = ::socket(bind_address.family, SOCK_STREAM, 0);
        if (listener_fd_ < 0) {
            return errno;
        }
        if (!set_nonblocking(listener_fd_) || !set_cloexec(listener_fd_)) {
            const int error = errno == 0 ? EIO : errno;
            close();
            return error;
        }

        int one = 1;
        static_cast<void>(::setsockopt(listener_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)));
#if defined(SO_REUSEPORT)
        if (context_->options.reuse_port) {
            static_cast<void>(
                ::setsockopt(listener_fd_, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)));
        }
#else
        static_cast<void>(one);
#endif
        if (bind_address.family == AF_INET6) {
            const int v6_only = context_->options.ipv6_only ? 1 : 0;
            static_cast<void>(
                ::setsockopt(listener_fd_, IPPROTO_IPV6, IPV6_V6ONLY, &v6_only, sizeof(v6_only)));
        }
        if (unix_listener && context_->options.unlink_existing_unix_path) {
            static_cast<void>(::unlink(unix_path));
        }
        if (::bind(listener_fd_, reinterpret_cast<const sockaddr *>(&bind_address.storage),
                   bind_address.size) != 0) {
            const int error = errno;
            close();
            return error;
        }
        if (unix_listener) {
            try {
                bound_unix_path_ = context_->endpoint.address;
            } catch (...) {
                close_fd(listener_fd_);
                if (context_->options.unlink_unix_path_on_close) {
                    static_cast<void>(::unlink(unix_path));
                }
                return ENOMEM;
            }
        }
        if (::listen(listener_fd_, context_->options.backlog) != 0) {
            const int error = errno;
            close();
            return error;
        }

        listener_channel_.fd = listener_fd_;
        if (!register_channel()) {
            const int error = errno == 0 ? EIO : errno;
            close();
            return error;
        }
        return 0;
    }

    void close() noexcept {
        if (listener_fd_ >= 0) {
            unregister_channel();
        }
        close_fd(listener_fd_);
        if (!bound_unix_path_.empty() && context_ != nullptr &&
            context_->options.unlink_unix_path_on_close) {
            static_cast<void>(::unlink(bound_unix_path_.c_str()));
        }
        bound_unix_path_.clear();
    }

private:
    static void on_listener_event(void *owner, std::uint32_t events) noexcept {
        static_cast<TcpListenerShard *>(owner)->handle_listener(events);
    }

    [[nodiscard]] bool register_channel() noexcept;
    void unregister_channel() noexcept;
    [[nodiscard]] bool route_connection(int fd, const sockaddr *peer, socklen_t peer_size) noexcept;

    void handle_listener(std::uint32_t events) noexcept {
        if ((events & (af::detail::net_io_error | af::detail::net_io_hangup)) != 0U) {
            close();
            if (context_ != nullptr && context_->handler != nullptr) {
                context_->handler->on_listener_error(TcpListenerHandle{context_->id}, EIO);
            }
            return;
        }
        std::size_t accepted = 0;
        while (accepted < options().accept_budget) {
            sockaddr_storage peer{};
            socklen_t peer_size = sizeof(peer);
            int fd =
                accept_nonblocking(listener_fd_, reinterpret_cast<sockaddr *>(&peer), &peer_size);
            if (fd >= 0) {
                set_no_sigpipe(fd);
                if (context_ != nullptr && context_->endpoint.family != AddressFamily::Unix) {
                    if (context_->options.no_delay) {
                        static_cast<void>(set_tcp_no_delay(fd, true));
                    }
                    if (context_->options.keepalive) {
                        static_cast<void>(set_socket_keepalive(fd, true));
                    }
                }
                if (!route_connection(fd, reinterpret_cast<const sockaddr *>(&peer), peer_size)) {
                    close_fd(fd);
                    if (context_ != nullptr && context_->handler != nullptr) {
                        context_->handler->on_listener_error(TcpListenerHandle{context_->id},
                                                             ENOBUFS);
                    }
                }
                ++accepted;
                continue;
            }
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN || error == EWOULDBLOCK) {
                return;
            }
            if (context_ != nullptr && context_->handler != nullptr) {
                context_->handler->on_listener_error(TcpListenerHandle{context_->id}, error);
            }
            return;
        }
    }

    Shard *shard_{nullptr};
    std::shared_ptr<ListenerContext> context_;
    int listener_fd_{-1};
    std::string bound_unix_path_;
    af::detail::NetIoChannel listener_channel_{};
};

} // namespace af::net::detail
