#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/async_runtime.hpp"
#include "af/buffer/buffer.hpp"
#include "af/detail/config.hpp"
#include "af/detail/net/reactor/net_io_channel.hpp"
#include "af/detail/net/socket_address.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/thread_kind.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

namespace af::net {

enum class UdpSendResult : std::uint8_t {
    Accepted,
    Queued,
    Backpressure,
    Closed,
    Unsupported,
};

struct UdpSocketOptions {
    bool reuse_port{true};
    bool ipv6_only{true};
    std::size_t read_budget_datagrams{64};
    std::size_t receive_buffer_size{64U * 1024U};
    std::size_t max_datagram_size{64U * 1024U};
    bool unlink_existing_unix_path{true};
    bool unlink_unix_path_on_close{true};
};

struct UdpSocketRuntimeConfig {};

template <typename Runtime> class UdpSocketHandle;
template <typename Runtime> class UdpSocketRef;

namespace detail {

template <typename Runtime> class UdpSocketShard;
template <typename Runtime> struct UdpSocketState;
template <typename Runtime> class UdpSendTask;

enum class UdpSendKind : std::uint8_t {
    Send,
    SendTo,
};

} // namespace detail

class UdpPeer {
public:
    UdpPeer() = default;

    UdpPeer(const sockaddr *address, socklen_t size) noexcept {
        assign(address, size);
    }

    [[nodiscard]] bool valid() const noexcept {
        return address_.size != 0U;
    }

    [[nodiscard]] int native_family() const noexcept {
        return address_.family;
    }

    [[nodiscard]] const sockaddr *native_address() const noexcept {
        return reinterpret_cast<const sockaddr *>(&address_.storage);
    }

    [[nodiscard]] socklen_t native_address_size() const noexcept {
        return address_.size;
    }

    [[nodiscard]] UdpEndpoint endpoint() const {
        return af::detail::endpoint_from_socket_address(native_address(), address_.size);
    }

private:
    template <typename Runtime> friend class UdpSocketHandle;
    template <typename Runtime> friend class UdpSocketRef;
    template <typename Runtime> friend class detail::UdpSocketShard;

    void assign(const sockaddr *address, socklen_t size) noexcept {
        address_ = af::detail::SocketAddress{};
        if (address == nullptr || size == 0U || size > sizeof(address_.storage)) {
            return;
        }
        std::memcpy(&address_.storage, address, size);
        address_.size = size;
        address_.family = address->sa_family;
    }

    [[nodiscard]] const af::detail::SocketAddress &socket_address() const noexcept {
        return address_;
    }

    af::detail::SocketAddress address_{};
};

namespace detail {

[[nodiscard]] inline bool udp_set_nonblocking(int fd) noexcept {
    const int current = ::fcntl(fd, F_GETFL, 0);
    return current >= 0 && ::fcntl(fd, F_SETFL, current | O_NONBLOCK) == 0;
}

[[nodiscard]] inline bool udp_set_cloexec(int fd) noexcept {
    const int current = ::fcntl(fd, F_GETFD, 0);
    return current >= 0 && ::fcntl(fd, F_SETFD, current | FD_CLOEXEC) == 0;
}

inline void udp_close_fd(int &fd) noexcept {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

template <typename Runtime> class UdpHandlerBase {
public:
    virtual ~UdpHandlerBase() = default;
    [[nodiscard]] virtual std::unique_ptr<UdpHandlerBase> clone() const = 0;
    virtual void on_datagram(UdpSocketRef<Runtime> socket, af::BufferView bytes,
                             const UdpPeer &peer) noexcept = 0;
    virtual void on_error(UdpSocketHandle<Runtime> socket, int error) noexcept = 0;
};

template <typename Handler, typename Runtime, typename = void>
struct UdpHandlerHasOnDatagramPeer : std::false_type {};
template <typename Handler, typename Runtime>
struct UdpHandlerHasOnDatagramPeer<
    Handler, Runtime,
    std::void_t<decltype(std::declval<Handler &>().on_datagram(
        std::declval<UdpSocketRef<Runtime>>(), std::declval<af::BufferView>(),
        std::declval<const UdpPeer &>()))>> : std::true_type {};

template <typename Handler, typename Runtime, typename = void>
struct UdpHandlerHasOnDatagramEndpoint : std::false_type {};
template <typename Handler, typename Runtime>
struct UdpHandlerHasOnDatagramEndpoint<
    Handler, Runtime,
    std::void_t<decltype(std::declval<Handler &>().on_datagram(
        std::declval<UdpSocketRef<Runtime>>(), std::declval<af::BufferView>(),
        std::declval<const UdpEndpoint &>()))>> : std::true_type {};

template <typename Handler, typename Runtime, typename = void>
struct UdpHandlerHasOnDatagram : std::false_type {};
template <typename Handler, typename Runtime>
struct UdpHandlerHasOnDatagram<
    Handler, Runtime,
    std::void_t<decltype(std::declval<Handler &>().on_datagram(
        std::declval<UdpSocketRef<Runtime>>(), std::declval<af::BufferView>()))>> : std::true_type {
};

template <typename Handler, typename Runtime, typename = void>
struct UdpHandlerHasOnError : std::false_type {};
template <typename Handler, typename Runtime>
struct UdpHandlerHasOnError<Handler, Runtime,
                            std::void_t<decltype(std::declval<Handler &>().on_error(
                                std::declval<UdpSocketHandle<Runtime>>(), std::declval<int>()))>>
    : std::true_type {};

template <typename Runtime, typename Handler>
class UdpHandlerModel final : public UdpHandlerBase<Runtime> {
public:
    explicit UdpHandlerModel(Handler handler) : handler_(std::move(handler)) {}

    [[nodiscard]] std::unique_ptr<UdpHandlerBase<Runtime>> clone() const override {
        return std::make_unique<UdpHandlerModel>(handler_);
    }

    void on_datagram(UdpSocketRef<Runtime> socket, af::BufferView bytes,
                     const UdpPeer &peer) noexcept override {
        if constexpr (UdpHandlerHasOnDatagramPeer<Handler, Runtime>::value) {
            try {
                handler_.on_datagram(socket, bytes, peer);
            } catch (...) {
                on_error(socket.handle(), EIO);
            }
        } else if constexpr (UdpHandlerHasOnDatagramEndpoint<Handler, Runtime>::value) {
            try {
                const UdpEndpoint endpoint = peer.endpoint();
                handler_.on_datagram(socket, bytes, endpoint);
            } catch (...) {
                on_error(socket.handle(), EIO);
            }
        } else if constexpr (UdpHandlerHasOnDatagram<Handler, Runtime>::value) {
            try {
                handler_.on_datagram(socket, bytes);
            } catch (...) {
                on_error(socket.handle(), EIO);
            }
        } else {
            static_cast<void>(socket);
            static_cast<void>(bytes);
            static_cast<void>(peer);
        }
    }

    void on_error(UdpSocketHandle<Runtime> socket, int error) noexcept override {
        if constexpr (UdpHandlerHasOnError<Handler, Runtime>::value) {
            try {
                handler_.on_error(socket, error);
            } catch (...) {
            }
        } else {
            static_cast<void>(socket);
            static_cast<void>(error);
        }
    }

private:
    Handler handler_;
};

template <typename Runtime> struct UdpSocketContext {
    std::string name;
    UdpEndpoint local_endpoint;
    UdpEndpoint remote_endpoint;
    UdpSocketOptions options;
    bool connect_remote{false};
    std::unique_ptr<UdpHandlerBase<Runtime>> handler;
};

template <typename Runtime> struct UdpSocketState {
    using Thread = typename Runtime::Thread;
    using Shard = UdpSocketShard<Runtime>;

    UdpSocketRuntimeConfig config;
    std::uint16_t control_thread_index{Runtime::invalid_thread_index};
    bool running{false};
    alignas(af::detail::hardware_cache_line_size) std::atomic<bool> accepting_send_tasks{false};
    std::vector<Thread> default_threads;
    std::vector<std::uint16_t> active_shards;
    std::vector<std::unique_ptr<Shard>> shards;
    std::array<std::atomic<std::uint16_t>, Runtime::thread_count> active_shard_snapshot{};
    alignas(af::detail::hardware_cache_line_size) std::atomic<std::uint16_t> active_shard_count{0};
};

template <typename Runtime>
void clear_udp_active_shard_snapshot(UdpSocketState<Runtime> &state) noexcept {
    state.active_shard_count.store(0U, std::memory_order_release);
}

template <typename Runtime>
void publish_udp_active_shard_snapshot(UdpSocketState<Runtime> &state,
                                       const std::vector<std::uint16_t> &shard_indexes) noexcept {
    const auto count = static_cast<std::uint16_t>(
        std::min<std::size_t>(shard_indexes.size(), Runtime::thread_count));
    for (std::uint16_t i = 0; i < count; ++i) {
        state.active_shard_snapshot[i].store(shard_indexes[i], std::memory_order_relaxed);
    }
    state.active_shard_count.store(count, std::memory_order_release);
}

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

template <typename Runtime> class UdpStartResultTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = UdpSocketState<Runtime>;

public:
    explicit UdpStartResultTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, int error) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        error_ = error;
        if (state_ == nullptr || state_->control_thread_index >= Runtime::thread_count) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(state_->control_thread_index));
    }

private:
    af::TaskResult run() override {
        if (state_ == nullptr || error_ == 0) {
            return this->done();
        }
        auto &active = state_->active_shards;
        auto out = active.begin();
        bool removed = false;
        for (auto it = active.begin(); it != active.end(); ++it) {
            if (*it == shard_index_) {
                removed = true;
                continue;
            }
            if (out != it) {
                *out = *it;
            }
            ++out;
        }
        if (removed) {
            active.erase(out, active.end());
            state_->running = !active.empty();
            if (state_->running) {
                publish_udp_active_shard_snapshot<Runtime>(*state_, active);
            } else {
                state_->accepting_send_tasks.store(false, std::memory_order_release);
                clear_udp_active_shard_snapshot<Runtime>(*state_);
            }
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    int error_{0};
};

template <typename Runtime>
void complete_udp_start_from_shard(std::shared_ptr<UdpSocketState<Runtime>> state,
                                   std::uint16_t shard_index, int error) noexcept {
    if (state == nullptr || state->control_thread_index >= Runtime::thread_count) {
        return;
    }
    if (Runtime::current_thread_index() == state->control_thread_index) {
        if (error == 0) {
            return;
        }
        auto &active = state->active_shards;
        auto out = active.begin();
        bool removed = false;
        for (auto it = active.begin(); it != active.end(); ++it) {
            if (*it == shard_index) {
                removed = true;
                continue;
            }
            if (out != it) {
                *out = *it;
            }
            ++out;
        }
        if (removed) {
            active.erase(out, active.end());
            state->running = !active.empty();
            if (state->running) {
                publish_udp_active_shard_snapshot<Runtime>(*state, active);
            } else {
                state->accepting_send_tasks.store(false, std::memory_order_release);
                clear_udp_active_shard_snapshot<Runtime>(*state);
            }
        }
        return;
    }
    try {
        static_cast<void>(Runtime::template start_task<UdpStartResultTask<Runtime>>(
            std::move(state), shard_index, error));
    } catch (...) {
    }
}

template <typename Runtime>
void notify_udp_start_error(const std::shared_ptr<UdpSocketState<Runtime>> &state,
                            std::uint16_t shard_index,
                            const std::shared_ptr<UdpSocketContext<Runtime>> &context,
                            int error) noexcept;

template <typename Runtime> class UdpStartShardTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = UdpSocketState<Runtime>;
    using Context = UdpSocketContext<Runtime>;

public:
    explicit UdpStartShardTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index,
               std::shared_ptr<Context> context) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        context_ = std::move(context);
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || context_ == nullptr) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

private:
    af::TaskResult run() override {
        int error = EINVAL;
        std::shared_ptr<Context> context = std::move(context_);
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr && context != nullptr) {
            error = state_->shards[shard_index_]->start_on_owner(context);
        }
        if (error != 0) {
            notify_udp_start_error<Runtime>(state_, shard_index_, context, error);
        }
        complete_udp_start_from_shard<Runtime>(state_, shard_index_, error);
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::shared_ptr<Context> context_;
};

template <typename Runtime> class UdpStopShardTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = UdpSocketState<Runtime>;

public:
    explicit UdpStopShardTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

private:
    af::TaskResult run() override {
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr) {
            state_->shards[shard_index_]->stop_on_owner();
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
};

template <typename Runtime> class UdpSendTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = UdpSocketState<Runtime>;

public:
    explicit UdpSendTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t generation,
               af::Buffer buffer) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        generation_ = generation;
        kind_ = UdpSendKind::Send;
        buffer_ = std::move(buffer);
        return schedule_on_owner();
    }

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t generation,
               af::Buffer buffer, af::detail::SocketAddress address) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        generation_ = generation;
        kind_ = UdpSendKind::SendTo;
        buffer_ = std::move(buffer);
        address_ = address;
        return schedule_on_owner();
    }

private:
    [[nodiscard]] bool schedule_on_owner() noexcept {
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || generation_ == 0U) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

    af::TaskResult run() override {
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            return this->done();
        }
        auto *shard = state_->shards[shard_index_].get();
        if (!shard->matches_generation(generation_)) {
            return this->done();
        }
        switch (kind_) {
        case UdpSendKind::Send:
            static_cast<void>(shard->send(std::move(buffer_)));
            break;
        case UdpSendKind::SendTo:
            static_cast<void>(shard->send_to_address(std::move(buffer_), address_));
            break;
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::uint32_t generation_{0};
    UdpSendKind kind_{UdpSendKind::Send};
    af::Buffer buffer_;
    af::detail::SocketAddress address_{};
};

} // namespace detail

template <typename Runtime> class UdpSocketHandle {
public:
    using State = detail::UdpSocketState<Runtime>;

    UdpSocketHandle() = default;

    UdpSocketHandle(std::weak_ptr<State> state, std::uint16_t shard_index,
                    std::uint32_t generation) noexcept
        : state_(std::move(state)), shard_index_(shard_index), generation_(generation) {}

    [[nodiscard]] std::uint16_t shard_index() const noexcept {
        return shard_index_;
    }

    [[nodiscard]] UdpSendResult send(af::Buffer buffer) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size() ||
            state->shards[shard_index_] == nullptr) {
            return UdpSendResult::Closed;
        }
        auto *shard = state->shards[shard_index_].get();
        if (!shard->matches_generation(generation_)) {
            return UdpSendResult::Closed;
        }
        if (Runtime::is_runtime_thread() && Runtime::current_thread_index() == shard_index_) {
            if (!shard->active()) {
                return UdpSendResult::Closed;
            }
            return shard->send(std::move(buffer));
        }
        if (!state->accepting_send_tasks.load(std::memory_order_acquire)) {
            return UdpSendResult::Closed;
        }
        return schedule_send(std::move(state), std::move(buffer));
    }

    [[nodiscard]] UdpSendResult send(af::BufferView view) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size() ||
            state->shards[shard_index_] == nullptr) {
            return UdpSendResult::Closed;
        }
        auto *shard = state->shards[shard_index_].get();
        if (!shard->matches_generation(generation_)) {
            return UdpSendResult::Closed;
        }
        if (Runtime::is_runtime_thread() && Runtime::current_thread_index() == shard_index_) {
            if (!shard->active()) {
                return UdpSendResult::Closed;
            }
            return shard->send(view);
        }
        if (!state->accepting_send_tasks.load(std::memory_order_acquire)) {
            return UdpSendResult::Closed;
        }
        try {
            return schedule_send(std::move(state), af::Buffer::copy(view));
        } catch (...) {
            return UdpSendResult::Backpressure;
        }
    }

    [[nodiscard]] UdpSendResult send_to(af::Buffer buffer, UdpEndpoint endpoint) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size() ||
            state->shards[shard_index_] == nullptr) {
            return UdpSendResult::Closed;
        }
        auto *shard = state->shards[shard_index_].get();
        if (!shard->matches_generation(generation_)) {
            return UdpSendResult::Closed;
        }
        if (Runtime::is_runtime_thread() && Runtime::current_thread_index() == shard_index_) {
            if (!shard->active()) {
                return UdpSendResult::Closed;
            }
            return shard->send_to(std::move(buffer), endpoint);
        }
        if (!state->accepting_send_tasks.load(std::memory_order_acquire)) {
            return UdpSendResult::Closed;
        }
        af::detail::SocketAddress address{};
        if (!shard->resolve_peer_endpoint(endpoint, address)) {
            return UdpSendResult::Unsupported;
        }
        return schedule_send_to(std::move(state), std::move(buffer), address);
    }

    [[nodiscard]] UdpSendResult send_to(af::BufferView view, UdpEndpoint endpoint) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size() ||
            state->shards[shard_index_] == nullptr) {
            return UdpSendResult::Closed;
        }
        auto *shard = state->shards[shard_index_].get();
        if (!shard->matches_generation(generation_)) {
            return UdpSendResult::Closed;
        }
        if (Runtime::is_runtime_thread() && Runtime::current_thread_index() == shard_index_) {
            if (!shard->active()) {
                return UdpSendResult::Closed;
            }
            return shard->send_to(view, endpoint);
        }
        if (!state->accepting_send_tasks.load(std::memory_order_acquire)) {
            return UdpSendResult::Closed;
        }
        af::detail::SocketAddress address{};
        if (!shard->resolve_peer_endpoint(endpoint, address)) {
            return UdpSendResult::Unsupported;
        }
        try {
            return schedule_send_to(std::move(state), af::Buffer::copy(view), address);
        } catch (...) {
            return UdpSendResult::Backpressure;
        }
    }

    [[nodiscard]] UdpSendResult send_to(af::Buffer buffer, const UdpPeer &peer) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size() ||
            state->shards[shard_index_] == nullptr) {
            return UdpSendResult::Closed;
        }
        auto *shard = state->shards[shard_index_].get();
        if (!shard->matches_generation(generation_)) {
            return UdpSendResult::Closed;
        }
        if (Runtime::is_runtime_thread() && Runtime::current_thread_index() == shard_index_) {
            if (!shard->active()) {
                return UdpSendResult::Closed;
            }
            return shard->send_to(std::move(buffer), peer);
        }
        if (!state->accepting_send_tasks.load(std::memory_order_acquire)) {
            return UdpSendResult::Closed;
        }
        const af::detail::SocketAddress &address = peer.socket_address();
        if (!shard->supports_peer_address(address)) {
            return UdpSendResult::Unsupported;
        }
        return schedule_send_to(std::move(state), std::move(buffer), address);
    }

    [[nodiscard]] UdpSendResult send_to(af::BufferView view, const UdpPeer &peer) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size() ||
            state->shards[shard_index_] == nullptr) {
            return UdpSendResult::Closed;
        }
        auto *shard = state->shards[shard_index_].get();
        if (!shard->matches_generation(generation_)) {
            return UdpSendResult::Closed;
        }
        if (Runtime::is_runtime_thread() && Runtime::current_thread_index() == shard_index_) {
            if (!shard->active()) {
                return UdpSendResult::Closed;
            }
            return shard->send_to(view, peer);
        }
        if (!state->accepting_send_tasks.load(std::memory_order_acquire)) {
            return UdpSendResult::Closed;
        }
        const af::detail::SocketAddress &address = peer.socket_address();
        if (!shard->supports_peer_address(address)) {
            return UdpSendResult::Unsupported;
        }
        try {
            return schedule_send_to(std::move(state), af::Buffer::copy(view), address);
        } catch (...) {
            return UdpSendResult::Backpressure;
        }
    }

private:
    [[nodiscard]] UdpSendResult schedule_send(std::shared_ptr<State> state,
                                              af::Buffer buffer) const {
        try {
            if (Runtime::template start_task<detail::UdpSendTask<Runtime>>(
                    std::move(state), shard_index_, generation_, std::move(buffer))) {
                return UdpSendResult::Queued;
            }
        } catch (...) {
        }
        return UdpSendResult::Backpressure;
    }

    [[nodiscard]] UdpSendResult schedule_send_to(std::shared_ptr<State> state, af::Buffer buffer,
                                                 af::detail::SocketAddress address) const {
        try {
            if (Runtime::template start_task<detail::UdpSendTask<Runtime>>(
                    std::move(state), shard_index_, generation_, std::move(buffer), address)) {
                return UdpSendResult::Queued;
            }
        } catch (...) {
        }
        return UdpSendResult::Backpressure;
    }

    std::weak_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::uint32_t generation_{0};
};

namespace detail {

template <typename Runtime>
void notify_udp_start_error(const std::shared_ptr<UdpSocketState<Runtime>> &state,
                            std::uint16_t shard_index,
                            const std::shared_ptr<UdpSocketContext<Runtime>> &context,
                            int error) noexcept {
    if (state == nullptr || context == nullptr || context->handler == nullptr ||
        shard_index >= state->shards.size() || state->shards[shard_index] == nullptr) {
        return;
    }
    context->handler->on_error(
        UdpSocketHandle<Runtime>(state, shard_index, state->shards[shard_index]->generation()),
        error == 0 ? EIO : error);
}

} // namespace detail

template <typename Runtime> class UdpSocketRef {
public:
    explicit UdpSocketRef(detail::UdpSocketShard<Runtime> *shard = nullptr) noexcept
        : shard_(shard) {}

    [[nodiscard]] bool valid() const noexcept {
        return shard_ != nullptr && shard_->active();
    }

    [[nodiscard]] UdpSocketHandle<Runtime> handle() const noexcept {
        return shard_ == nullptr ? UdpSocketHandle<Runtime>{} : shard_->handle();
    }

    [[nodiscard]] std::uint16_t shard_index() const noexcept {
        return shard_ == nullptr ? 0U : shard_->shard_index_;
    }

    [[nodiscard]] std::string_view name() const noexcept {
        return shard_ == nullptr ? std::string_view{} : shard_->name();
    }

    [[nodiscard]] UdpSendResult send(af::Buffer buffer) const noexcept {
        return shard_ == nullptr ? UdpSendResult::Closed : shard_->send(std::move(buffer));
    }

    [[nodiscard]] UdpSendResult send(af::BufferView view) const noexcept {
        return shard_ == nullptr ? UdpSendResult::Closed : shard_->send(view);
    }

    [[nodiscard]] UdpSendResult send_to(af::Buffer buffer,
                                        const UdpEndpoint &endpoint) const noexcept {
        return shard_ == nullptr ? UdpSendResult::Closed
                                 : shard_->send_to(std::move(buffer), endpoint);
    }

    [[nodiscard]] UdpSendResult send_to(af::BufferView view,
                                        const UdpEndpoint &endpoint) const noexcept {
        return shard_ == nullptr ? UdpSendResult::Closed : shard_->send_to(view, endpoint);
    }

    [[nodiscard]] UdpSendResult send_to(af::Buffer buffer, const UdpPeer &peer) const noexcept {
        return shard_ == nullptr ? UdpSendResult::Closed : shard_->send_to(std::move(buffer), peer);
    }

    [[nodiscard]] UdpSendResult send_to(af::BufferView view, const UdpPeer &peer) const noexcept {
        return shard_ == nullptr ? UdpSendResult::Closed : shard_->send_to(view, peer);
    }

private:
    detail::UdpSocketShard<Runtime> *shard_{nullptr};
};

template <typename Runtime> class UdpSocket {
public:
    using Thread = typename Runtime::Thread;
    using State = detail::UdpSocketState<Runtime>;

    struct Config {
        std::string name;
        UdpEndpoint local_endpoint = UdpEndpoint::any(0);
        UdpEndpoint remote_endpoint;
        std::vector<Thread> threads;
        UdpSocketOptions options;
        bool connect_remote{false};
    };

    UdpSocket() : UdpSocket(UdpSocketRuntimeConfig{}) {}

    explicit UdpSocket(UdpSocketRuntimeConfig config) : state_(std::make_shared<State>()) {
        state_->config = normalize_config(config);
        state_->control_thread_index = first_io_thread_index();
        init_shards();
    }

    explicit UdpSocket(std::vector<Thread> threads) : UdpSocket() {
        bind_threads(std::move(threads));
    }

    ~UdpSocket() {
        static_cast<void>(stop());
    }

    UdpSocket(const UdpSocket &) = delete;
    UdpSocket &operator=(const UdpSocket &) = delete;
    UdpSocket(UdpSocket &&) noexcept = default;
    UdpSocket &operator=(UdpSocket &&) noexcept = default;

    UdpSocket &bind_threads(std::vector<Thread> threads) {
        state_->default_threads = std::move(threads);
        return *this;
    }

    template <typename Group> UdpSocket &bind_threads(Group) {
        return bind_threads(thread_list_from_group(Group{}));
    }

    template <typename Handler>
    [[nodiscard]] bool start(Config config, Handler handler = Handler{}) {
        static_assert(std::is_copy_constructible_v<Handler>,
                      "UDP socket handlers must be copy constructible");
        std::unique_ptr<detail::UdpHandlerBase<Runtime>> prototype;
        try {
            prototype =
                std::make_unique<detail::UdpHandlerModel<Runtime, Handler>>(std::move(handler));
        } catch (...) {
            return false;
        }
        return start_impl(std::move(config), std::move(prototype));
    }

    [[nodiscard]] bool stop() {
        auto state = state_;
        if (state == nullptr) {
            return true;
        }

        std::vector<std::uint16_t> shards;
        if (!state->running) {
            return true;
        }
        try {
            shards = state->active_shards;
        } catch (...) {
            return false;
        }
        state->running = false;
        state->accepting_send_tasks.store(false, std::memory_order_release);
        detail::clear_udp_active_shard_snapshot<Runtime>(*state);
        state->active_shards.clear();

        const auto stop_result = stop_shards(state, shards);
        if (!stop_result.ok) {
            publish_remaining_after_failed_stop(state, shards, stop_result.stopped_shards);
        }
        return stop_result.ok;
    }

    [[nodiscard]] UdpSocketHandle<Runtime> handle() const {
        const std::uint16_t count = state_->active_shard_count.load(std::memory_order_acquire);
        if (count == 0U) {
            return {};
        }
        static thread_local std::uint32_t next_handle_slot = 0;
        const std::uint32_t ticket = next_handle_slot++;
        const std::uint16_t shard =
            state_->active_shard_snapshot[ticket % count].load(std::memory_order_relaxed);
        if (shard >= state_->shards.size() || state_->shards[shard] == nullptr) {
            return {};
        }
        return UdpSocketHandle<Runtime>(state_, shard, state_->shards[shard]->generation());
    }

    [[nodiscard]] UdpSocketHandle<Runtime> handle_for_shard(std::uint16_t shard_index) const {
        const std::uint16_t count = state_->active_shard_count.load(std::memory_order_acquire);
        for (std::uint16_t i = 0; i < count; ++i) {
            const std::uint16_t active_shard =
                state_->active_shard_snapshot[i].load(std::memory_order_relaxed);
            if (active_shard == shard_index) {
                if (shard_index >= state_->shards.size() ||
                    state_->shards[shard_index] == nullptr) {
                    return {};
                }
                return UdpSocketHandle<Runtime>(state_, shard_index,
                                                state_->shards[shard_index]->generation());
            }
        }
        return {};
    }

    [[nodiscard]] std::vector<UdpSocketHandle<Runtime>> handles() const {
        const std::uint16_t count = state_->active_shard_count.load(std::memory_order_acquire);
        std::vector<UdpSocketHandle<Runtime>> result;
        try {
            result.reserve(count);
            for (std::uint16_t i = 0; i < count; ++i) {
                const std::uint16_t shard_index =
                    state_->active_shard_snapshot[i].load(std::memory_order_relaxed);
                if (shard_index < state_->shards.size() && state_->shards[shard_index] != nullptr) {
                    result.emplace_back(state_, shard_index,
                                        state_->shards[shard_index]->generation());
                }
            }
        } catch (...) {
            result.clear();
        }
        return result;
    }

private:
    static constexpr bool is_io_thread(Thread thread) noexcept {
        const af::thread_kind kind = Runtime::thread_kind(thread);
        return kind == af::thread_kind::io;
    }

    [[nodiscard]] static std::uint16_t first_io_thread_index() noexcept {
        for (std::uint16_t i = 0; i < Runtime::thread_count; ++i) {
            if (is_io_thread(Runtime::thread_from_index(i))) {
                return i;
            }
        }
        return Runtime::invalid_thread_index;
    }

    [[nodiscard]] static UdpSocketRuntimeConfig
    normalize_config(UdpSocketRuntimeConfig config) noexcept {
        return config;
    }

    template <typename Group>
    [[nodiscard]] static std::vector<Thread> thread_list_from_group(Group) {
        std::vector<Thread> result;
        result.reserve(Group::count);
        for (std::uint16_t i = 0; i < Group::count; ++i) {
            result.push_back(Group::at(i));
        }
        return result;
    }

    void init_shards() {
        state_->shards.reserve(Runtime::thread_count);
        for (std::uint16_t i = 0; i < Runtime::thread_count; ++i) {
            const Thread thread = Runtime::thread_from_index(i);
            state_->shards.push_back(
                std::make_unique<detail::UdpSocketShard<Runtime>>(state_, i, thread));
        }
    }

    [[nodiscard]] int validate_config(const Config &config) const noexcept {
        if (config.threads.empty()) {
            return EINVAL;
        }
        if (config.options.read_budget_datagrams == 0U ||
            config.options.receive_buffer_size == 0U || config.options.max_datagram_size == 0U) {
            return EINVAL;
        }
        af::detail::SocketAddress local_address{};
        int address_error = 0;
        if (!af::detail::socket_address_from_endpoint(config.local_endpoint, local_address,
                                                      address_error)) {
            return address_error == 0 ? EINVAL : address_error;
        }
        const bool unix_socket = config.local_endpoint.family == AddressFamily::Unix;
        if (config.threads.size() > 1U && (!config.options.reuse_port || unix_socket)) {
            return EINVAL;
        }
        if (unix_socket && config.local_endpoint.address.empty()) {
            return EINVAL;
        }
        if (config.connect_remote) {
            if (config.remote_endpoint.family == AddressFamily::Unix &&
                config.remote_endpoint.address.empty()) {
                return EINVAL;
            }
            af::detail::SocketAddress remote_address{};
            if (!af::detail::socket_address_from_endpoint(config.remote_endpoint, remote_address,
                                                          address_error)) {
                return address_error == 0 ? EINVAL : address_error;
            }
            if (remote_address.family != local_address.family) {
                return EINVAL;
            }
            if (remote_address.family != AF_UNIX && config.remote_endpoint.port == 0U) {
                return EINVAL;
            }
        }
        for (Thread thread : config.threads) {
            const std::uint16_t index = Runtime::thread_index(thread);
            if (index >= Runtime::thread_count || !is_io_thread(thread)) {
                return EINVAL;
            }
        }
        for (std::size_t i = 0; i < config.threads.size(); ++i) {
            for (std::size_t j = i + 1U; j < config.threads.size(); ++j) {
                if (Runtime::thread_index(config.threads[i]) ==
                    Runtime::thread_index(config.threads[j])) {
                    return EINVAL;
                }
            }
        }
        return 0;
    }

    struct StopResult {
        bool ok{false};
        std::vector<std::uint16_t> stopped_shards;
    };

    [[nodiscard]] static bool contains_shard(const std::vector<std::uint16_t> &shards,
                                             std::uint16_t shard) noexcept {
        for (const std::uint16_t candidate : shards) {
            if (candidate == shard) {
                return true;
            }
        }
        return false;
    }

    void publish_remaining_after_failed_stop(const std::shared_ptr<State> &state,
                                             const std::vector<std::uint16_t> &requested_shards,
                                             const std::vector<std::uint16_t> &stopped_shards) {
        std::vector<std::uint16_t> remaining;
        try {
            remaining.reserve(requested_shards.size());
            for (const std::uint16_t shard : requested_shards) {
                if (!contains_shard(stopped_shards, shard)) {
                    remaining.push_back(shard);
                }
            }
        } catch (...) {
            return;
        }

        state->active_shards = std::move(remaining);
        state->running = !state->active_shards.empty();
        if (state->running) {
            state->accepting_send_tasks.store(true, std::memory_order_release);
            detail::publish_udp_active_shard_snapshot<Runtime>(*state, state->active_shards);
        } else {
            state->accepting_send_tasks.store(false, std::memory_order_release);
            detail::clear_udp_active_shard_snapshot<Runtime>(*state);
        }
    }

    [[nodiscard]] bool start_impl(Config config,
                                  std::unique_ptr<detail::UdpHandlerBase<Runtime>> prototype) {
        if (prototype == nullptr) {
            return false;
        }

        std::vector<std::uint16_t> shard_indexes;
        struct PendingStart {
            std::uint16_t shard_index{0};
            std::shared_ptr<detail::UdpSocketContext<Runtime>> context;
        };
        std::vector<PendingStart> pending;
        if (state_->running) {
            return false;
        }
        if (config.threads.empty()) {
            config.threads = state_->default_threads;
        }
        if (validate_config(config) != 0) {
            return false;
        }
        try {
            shard_indexes.reserve(config.threads.size());
            pending.reserve(config.threads.size());
            for (Thread thread : config.threads) {
                const std::uint16_t shard_index = Runtime::thread_index(thread);
                auto context = std::make_shared<detail::UdpSocketContext<Runtime>>();
                context->name = config.name;
                context->local_endpoint = config.local_endpoint;
                context->remote_endpoint = config.remote_endpoint;
                context->options = config.options;
                context->connect_remote = config.connect_remote;
                context->handler = prototype->clone();
                shard_indexes.push_back(shard_index);
                pending.push_back(PendingStart{shard_index, std::move(context)});
            }
        } catch (...) {
            return false;
        }

        state_->running = true;
        state_->accepting_send_tasks.store(true, std::memory_order_release);
        state_->active_shards = shard_indexes;
        detail::publish_udp_active_shard_snapshot<Runtime>(*state_, state_->active_shards);

        bool scheduled_all = true;
        std::vector<std::uint16_t> scheduled_shards;
        try {
            scheduled_shards.reserve(pending.size());
        } catch (...) {
            state_->running = false;
            state_->accepting_send_tasks.store(false, std::memory_order_release);
            detail::clear_udp_active_shard_snapshot<Runtime>(*state_);
            state_->active_shards.clear();
            return false;
        }
        for (auto &entry : pending) {
            bool scheduled = false;
            try {
                scheduled = Runtime::template start_task<detail::UdpStartShardTask<Runtime>>(
                    state_, entry.shard_index, std::move(entry.context));
            } catch (...) {
                scheduled = false;
            }
            if (!scheduled) {
                scheduled_all = false;
                continue;
            }
            scheduled_shards.push_back(entry.shard_index);
        }

        if (!scheduled_all) {
            state_->running = false;
            state_->accepting_send_tasks.store(false, std::memory_order_release);
            detail::clear_udp_active_shard_snapshot<Runtime>(*state_);
            state_->active_shards.clear();
            static_cast<void>(stop_shards(state_, scheduled_shards));
            return false;
        }
        return true;
    }

    [[nodiscard]] StopResult stop_shards(std::shared_ptr<State> state,
                                         const std::vector<std::uint16_t> &shards) {
        if (shards.empty()) {
            return StopResult{true, {}};
        }
        std::vector<std::uint16_t> scheduled_shards;
        try {
            scheduled_shards.reserve(shards.size());
        } catch (...) {
            return StopResult{false, {}};
        }
        bool ok = true;
        for (const std::uint16_t shard_index : shards) {
            bool scheduled = false;
            try {
                scheduled = Runtime::template start_task<detail::UdpStopShardTask<Runtime>>(
                    state, shard_index);
            } catch (...) {
                scheduled = false;
            }
            if (!scheduled) {
                ok = false;
                continue;
            }
            scheduled_shards.push_back(shard_index);
        }
        return StopResult{ok, std::move(scheduled_shards)};
    }

    std::shared_ptr<State> state_;
};

using udp_send_result = UdpSendResult;
using udp_socket_options = UdpSocketOptions;
using udp_socket_runtime_config = UdpSocketRuntimeConfig;
using udp_peer = UdpPeer;

template <typename Runtime> using udp_socket_handle = UdpSocketHandle<Runtime>;
template <typename Runtime> using udp_socket_ref = UdpSocketRef<Runtime>;
template <typename Runtime> using udp_socket = UdpSocket<Runtime>;

} // namespace af::net
