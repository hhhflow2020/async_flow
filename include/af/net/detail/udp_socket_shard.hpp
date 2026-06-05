#pragma once

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "af/buffer/buffer.hpp"
#include "af/detail/config.hpp"
#include "af/detail/net/reactor/net_io_channel.hpp"
#include "af/detail/net/socket_address.hpp"
#include "af/net/detail/udp_handler.hpp"
#include "af/net/detail/udp_socket_ops.hpp"
#include "af/net/detail/udp_state.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/udp_types.hpp"

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

namespace af::net::detail {

template <typename Runtime> class UdpSendTask;

template <typename Runtime> class UdpSocketShard {
public:
    using State = UdpSocketState<Runtime>;
    using Thread = typename Runtime::Thread;
    using Context = UdpSocketContext<Runtime>;

    UdpSocketShard(std::weak_ptr<State> state, std::uint16_t shard_index, Thread thread)
        : state_(std::move(state)), shard_index_(shard_index), thread_(thread) {}

    UdpSocketShard(const UdpSocketShard &) = delete;
    UdpSocketShard &operator=(const UdpSocketShard &) = delete;

    ~UdpSocketShard() {
        udp_close_fd(fd_);
    }

    [[nodiscard]] Thread thread() const noexcept {
        return thread_;
    }

    [[nodiscard]] std::weak_ptr<State> weak_state() const noexcept {
        return state_;
    }

    [[nodiscard]] bool active() const noexcept {
        return fd_ >= 0;
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return generation_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool matches_generation(std::uint32_t generation) const noexcept {
        return generation != 0U && generation_.load(std::memory_order_acquire) == generation;
    }

    [[nodiscard]] bool resolve_peer_endpoint(const UdpEndpoint &endpoint,
                                             af::detail::SocketAddress &address) const noexcept {
        int address_error = 0;
        return af::detail::socket_address_from_endpoint(endpoint, address, address_error) &&
               address.family == socket_family_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool
    supports_peer_address(const af::detail::SocketAddress &address) const noexcept {
        return address.size != 0U &&
               address.family == socket_family_.load(std::memory_order_acquire);
    }

    [[nodiscard]] UdpSendResult send(af::Buffer buffer) noexcept {
        if (context_ == nullptr || !context_->connect_remote) {
            return UdpSendResult::Unsupported;
        }
        return send_impl(std::move(buffer), nullptr);
    }

    [[nodiscard]] UdpSendResult send(af::BufferView view) noexcept {
        if (context_ == nullptr || !context_->connect_remote) {
            return UdpSendResult::Unsupported;
        }
        return send_view_impl(view, nullptr);
    }

    [[nodiscard]] UdpSendResult send_to(af::Buffer buffer, const UdpEndpoint &endpoint) noexcept {
        af::detail::SocketAddress address{};
        if (!resolve_peer_endpoint(endpoint, address)) {
            return UdpSendResult::Unsupported;
        }
        return send_to_address(std::move(buffer), address);
    }

    [[nodiscard]] UdpSendResult send_to(af::Buffer buffer, const UdpPeer &peer) noexcept {
        return send_to_address(std::move(buffer), peer.socket_address());
    }

    [[nodiscard]] UdpSendResult send_to(af::BufferView view, const UdpEndpoint &endpoint) noexcept {
        af::detail::SocketAddress address{};
        if (!resolve_peer_endpoint(endpoint, address)) {
            return UdpSendResult::Unsupported;
        }
        return send_to_view_address(view, address);
    }

    [[nodiscard]] UdpSendResult send_to(af::BufferView view, const UdpPeer &peer) noexcept {
        return send_to_view_address(view, peer.socket_address());
    }

    [[nodiscard]] int start_on_owner(std::shared_ptr<Context> context) noexcept {
        if (context == nullptr || context->handler == nullptr) {
            return EINVAL;
        }
        if (fd_ >= 0) {
            return EALREADY;
        }
        advance_generation_on_owner();

        af::detail::SocketAddress local{};
        int address_error = 0;
        if (!af::detail::socket_address_from_endpoint(context->local_endpoint, local,
                                                      address_error)) {
            return address_error == 0 ? EINVAL : address_error;
        }

        int socket_fd = ::socket(local.family, SOCK_DGRAM, 0);
        if (socket_fd < 0) {
            return errno;
        }
        if (!udp_set_nonblocking(socket_fd) || !udp_set_cloexec(socket_fd)) {
            const int error = errno == 0 ? EIO : errno;
            udp_close_fd(socket_fd);
            return error;
        }

        int one = 1;
        if (local.family != AF_UNIX) {
            static_cast<void>(::setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)));
        }
#if defined(SO_REUSEPORT)
        if (context->options.reuse_port && local.family != AF_UNIX) {
            static_cast<void>(::setsockopt(socket_fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)));
        }
#endif
        if (local.family == AF_INET6) {
            const int v6_only = context->options.ipv6_only ? 1 : 0;
            static_cast<void>(
                ::setsockopt(socket_fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6_only, sizeof(v6_only)));
        }

        const bool unix_socket = local.family == AF_UNIX;
        if (unix_socket && context->options.unlink_existing_unix_path) {
            static_cast<void>(::unlink(context->local_endpoint.address.c_str()));
        }
        if (::bind(socket_fd, reinterpret_cast<const sockaddr *>(&local.storage), local.size) !=
            0) {
            const int error = errno;
            udp_close_fd(socket_fd);
            return error;
        }
        if (unix_socket) {
            try {
                bound_unix_path_ = context->local_endpoint.address;
                unlink_bound_unix_path_on_close_ = context->options.unlink_unix_path_on_close;
            } catch (...) {
                udp_close_fd(socket_fd);
                if (context->options.unlink_unix_path_on_close) {
                    static_cast<void>(::unlink(context->local_endpoint.address.c_str()));
                }
                return ENOMEM;
            }
        }

        if (context->connect_remote) {
            af::detail::SocketAddress remote{};
            if (!af::detail::socket_address_from_endpoint(context->remote_endpoint, remote,
                                                          address_error)) {
                udp_close_fd(socket_fd);
                cleanup_bound_unix_path();
                return address_error == 0 ? EINVAL : address_error;
            }
            if (::connect(socket_fd, reinterpret_cast<const sockaddr *>(&remote.storage),
                          remote.size) != 0) {
                const int error = errno;
                udp_close_fd(socket_fd);
                cleanup_bound_unix_path();
                return error;
            }
        }

        fd_ = socket_fd;
        socket_family_.store(local.family, std::memory_order_release);
        channel_.fd = fd_;
        channel_.owner = this;
        channel_.on_event = &UdpSocketShard::on_socket_event;
        if (!Runtime::net_register_channel(thread_, &channel_, af::detail::net_io_readable)) {
            const int error = errno == 0 ? EIO : errno;
            static_cast<void>(Runtime::net_unregister_channel(thread_, &channel_));
            udp_close_fd(fd_);
            cleanup_bound_unix_path();
            return error;
        }
        context_ = std::move(context);
        return 0;
    }

    void stop_on_owner() noexcept {
        if (fd_ >= 0) {
            static_cast<void>(Runtime::net_unregister_channel(thread_, &channel_));
        }
        udp_close_fd(fd_);
        socket_family_.store(AF_UNSPEC, std::memory_order_release);
        cleanup_bound_unix_path();
        context_.reset();
        read_buffer_.clear();
    }

private:
    template <typename RuntimeT> friend class ::af::net::UdpSocketRef;
    template <typename RuntimeT> friend class ::af::net::UdpSocketHandle;
    friend class UdpSendTask<Runtime>;

    static void on_socket_event(void *owner, std::uint32_t events) noexcept {
        static_cast<UdpSocketShard *>(owner)->handle_socket(events);
    }

    [[nodiscard]] UdpSocketHandle<Runtime> handle() const noexcept {
        return UdpSocketHandle<Runtime>(weak_state(), shard_index_, generation());
    }

    [[nodiscard]] std::string_view name() const noexcept {
        return context_ == nullptr ? std::string_view{} : std::string_view(context_->name);
    }

    void handle_socket(std::uint32_t events) noexcept {
        if ((events & af::detail::net_io_error) != 0U) {
            notify_error(EIO);
            return;
        }
        if ((events & af::detail::net_io_readable) != 0U) {
            read_available();
        }
    }

    void read_available() noexcept {
        if (fd_ < 0 || context_ == nullptr || context_->handler == nullptr) {
            return;
        }
        const std::size_t configured_receive_size = context_->options.receive_buffer_size == 0U
                                                        ? UdpSocketOptions{}.receive_buffer_size
                                                        : context_->options.receive_buffer_size;
        const std::size_t max_datagram_size = context_->options.max_datagram_size == 0U
                                                  ? UdpSocketOptions{}.max_datagram_size
                                                  : context_->options.max_datagram_size;
        const std::size_t buffer_size = std::max(configured_receive_size, max_datagram_size);
        if (read_buffer_.size() < buffer_size) {
            try {
                read_buffer_.resize(buffer_size);
            } catch (...) {
                notify_error(ENOMEM);
                return;
            }
        }

        const std::size_t budget = context_->options.read_budget_datagrams == 0U
                                       ? UdpSocketOptions{}.read_budget_datagrams
                                       : context_->options.read_budget_datagrams;
        std::size_t received = 0;
        while (fd_ >= 0 && received < budget) {
            sockaddr_storage peer{};
            iovec iov{};
            iov.iov_base = read_buffer_.data();
            iov.iov_len = read_buffer_.size();
            msghdr message{};
            message.msg_name = &peer;
            message.msg_namelen = sizeof(peer);
            message.msg_iov = &iov;
            message.msg_iovlen = 1;
            const ssize_t n = ::recvmsg(fd_, &message, 0);
            if (n >= 0) {
                const auto size = static_cast<std::size_t>(n);
                if (size > max_datagram_size || (message.msg_flags & MSG_TRUNC) == MSG_TRUNC) {
                    notify_error(EMSGSIZE);
                    ++received;
                    continue;
                }
                UdpPeer peer_endpoint(reinterpret_cast<const sockaddr *>(&peer),
                                      message.msg_namelen);
                context_->handler->on_datagram(UdpSocketRef<Runtime>(this),
                                               af::BufferView(read_buffer_.data(), size),
                                               peer_endpoint);
                ++received;
                continue;
            }
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN || error == EWOULDBLOCK) {
                return;
            }
            notify_error(error);
            return;
        }
    }

    [[nodiscard]] UdpSendResult send_to_address(af::Buffer buffer,
                                                const af::detail::SocketAddress &address) noexcept {
        if (!supports_peer_address(address)) {
            return UdpSendResult::Unsupported;
        }
        return send_impl(std::move(buffer), &address);
    }

    [[nodiscard]] UdpSendResult
    send_to_view_address(af::BufferView view, const af::detail::SocketAddress &address) noexcept {
        if (!supports_peer_address(address)) {
            return UdpSendResult::Unsupported;
        }
        return send_view_impl(view, &address);
    }

    [[nodiscard]] UdpSendResult send_impl(af::Buffer buffer,
                                          const af::detail::SocketAddress *address) noexcept {
        return send_view_impl(buffer.view(), address);
    }

    [[nodiscard]] UdpSendResult send_view_impl(af::BufferView view,
                                               const af::detail::SocketAddress *address) noexcept {
        if (fd_ < 0 || context_ == nullptr) {
            return UdpSendResult::Closed;
        }
        if (view.size() > context_->options.max_datagram_size) {
            return UdpSendResult::Backpressure;
        }

        const sockaddr *raw_address = nullptr;
        socklen_t raw_address_size = 0;
        if (address != nullptr) {
            if (!supports_peer_address(*address)) {
                return UdpSendResult::Unsupported;
            }
            raw_address = reinterpret_cast<const sockaddr *>(&address->storage);
            raw_address_size = address->size;
        } else if (!context_->connect_remote) {
            return UdpSendResult::Unsupported;
        }

        for (;;) {
            const ssize_t n = address == nullptr ? ::send(fd_, view.data(), view.size(), 0)
                                                 : ::sendto(fd_, view.data(), view.size(), 0,
                                                            raw_address, raw_address_size);
            if (n == static_cast<ssize_t>(view.size())) {
                return UdpSendResult::Accepted;
            }
            if (n >= 0) {
                return UdpSendResult::Backpressure;
            }
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN || error == EWOULDBLOCK || error == ENOBUFS) {
                return UdpSendResult::Backpressure;
            }
            if (error == EBADF || error == ENOTCONN) {
                return UdpSendResult::Closed;
            }
            notify_error(error);
            return UdpSendResult::Closed;
        }
    }

    void advance_generation_on_owner() noexcept {
        std::uint32_t next = generation_.load(std::memory_order_relaxed) + 1U;
        if (next == 0U) {
            next = 1U;
        }
        generation_.store(next, std::memory_order_release);
    }

    void notify_error(int error) noexcept {
        if (context_ != nullptr && context_->handler != nullptr) {
            context_->handler->on_error(handle(), error == 0 ? EIO : error);
        }
    }

    void cleanup_bound_unix_path() noexcept {
        if (!bound_unix_path_.empty() && unlink_bound_unix_path_on_close_) {
            static_cast<void>(::unlink(bound_unix_path_.c_str()));
        }
        bound_unix_path_.clear();
        unlink_bound_unix_path_on_close_ = false;
    }

    std::weak_ptr<State> state_;
    std::uint16_t shard_index_{0};
    Thread thread_;
    int fd_{-1};
    af::detail::NetIoChannel channel_{};
    std::atomic<int> socket_family_{AF_UNSPEC};
    alignas(af::detail::hardware_cache_line_size) std::atomic<std::uint32_t> generation_{0};
    std::shared_ptr<Context> context_;
    std::vector<std::byte> read_buffer_;
    std::string bound_unix_path_;
    bool unlink_bound_unix_path_on_close_{false};
};

} // namespace af::net::detail
