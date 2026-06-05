#pragma once

#include <algorithm>
#include <atomic>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/async_runtime.hpp"
#include "af/buffer/buffer.hpp"
#include "af/detail/config.hpp"
#include "af/net/detail/tcp_handler.hpp"
#include "af/detail/net/reactor/net_io_channel.hpp"
#include "af/detail/net/socket_address.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"
#include "af/thread_kind.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

namespace af::net {

namespace detail {

template <typename Runtime> class TcpConnection;
template <typename Runtime> class TcpServerShard;
template <typename Runtime> class TcpListenerShard;
template <typename Runtime> struct TcpListenerContext;
template <typename Runtime> struct TcpListenerEntry;
template <typename Runtime> struct TcpServerState;
template <typename Runtime> class TcpAdoptConnectionTask;
template <typename Runtime> class TcpConnectionCommandTask;

enum class TcpConnectionCommandKind : std::uint8_t {
    Send,
    Close,
    CloseAfterFlush,
    ShutdownWrite,
    PauseRead,
    ResumeRead,
    SetNoDelay,
    SetKeepAlive,
};

template <typename Runtime> struct TcpListenerContext {
    ListenerId id{};
    std::string name;
    TcpEndpoint endpoint;
    TcpListenerOptions options;
    std::vector<std::uint16_t> target_shards;
    std::uint32_t next_target_shard{0};
    std::unique_ptr<TcpHandlerBase<Runtime>> handler;
};

template <typename Runtime> struct TcpListenerEntry {
    using Thread = typename Runtime::Thread;

    ListenerId id{};
    std::string name;
    TcpEndpoint endpoint;
    TcpListenerOptions options;
    AcceptStrategy accept_strategy{AcceptStrategy::Auto};
    std::vector<Thread> threads;
    std::vector<std::uint16_t> active_shards;
    std::vector<std::uint16_t> starting_shards;
    std::vector<std::uint16_t> started_shards;
    std::unique_ptr<TcpHandlerBase<Runtime>> handler_prototype;
    ListenerState state{ListenerState::Configured};
    std::size_t pending_start_shards{0};
    int start_error{0};
};

template <typename Runtime> struct TcpServerState {
    using Thread = typename Runtime::Thread;
    using Shard = TcpServerShard<Runtime>;
    using ListenerEntry = TcpListenerEntry<Runtime>;

    TcpServerConfig config;
    std::uint16_t control_thread_index{Runtime::invalid_thread_index};
    bool running{false};
    alignas(af::detail::hardware_cache_line_size) std::atomic<bool> accepting_connection_tasks{
        false};
    std::vector<Thread> default_threads;
    std::vector<std::unique_ptr<Shard>> shards;
    std::vector<std::unique_ptr<ListenerEntry>> listeners;
    std::vector<std::uint32_t> listener_generations;
    std::vector<std::uint32_t> free_listener_slots;
};

[[nodiscard]] inline bool set_nonblocking(int fd) noexcept {
    const int current = ::fcntl(fd, F_GETFL, 0);
    return current >= 0 && ::fcntl(fd, F_SETFL, current | O_NONBLOCK) == 0;
}

[[nodiscard]] inline bool set_cloexec(int fd) noexcept {
    const int current = ::fcntl(fd, F_GETFD, 0);
    return current >= 0 && ::fcntl(fd, F_SETFD, current | FD_CLOEXEC) == 0;
}

[[nodiscard]] inline int send_no_signal_flags() noexcept {
#if defined(MSG_NOSIGNAL)
    return MSG_NOSIGNAL;
#else
    return 0;
#endif
}

[[nodiscard]] inline int accept_nonblocking(int listener_fd, sockaddr *address,
                                            socklen_t *address_size) noexcept {
#if defined(__linux__)
    return ::accept4(listener_fd, address, address_size, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
    const int fd = ::accept(listener_fd, address, address_size);
    if (fd >= 0 && (!set_nonblocking(fd) || !set_cloexec(fd))) {
        const int error = errno == 0 ? EIO : errno;
        ::close(fd);
        errno = error;
        return -1;
    }
    return fd;
#endif
}

inline void set_no_sigpipe(int fd) noexcept {
#if defined(SO_NOSIGPIPE)
    int one = 1;
    static_cast<void>(::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)));
#else
    static_cast<void>(fd);
#endif
}

[[nodiscard]] inline bool set_tcp_no_delay(int fd, bool enabled) noexcept {
    int value = enabled ? 1 : 0;
    return ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)) == 0;
}

[[nodiscard]] inline bool set_socket_keepalive(int fd, bool enabled) noexcept {
    int value = enabled ? 1 : 0;
    return ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &value, sizeof(value)) == 0;
}

inline void close_fd(int &fd) noexcept {
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
}

template <typename Runtime> class TcpConnection {
public:
    using State = TcpServerState<Runtime>;
    using Shard = TcpServerShard<Runtime>;
    using Thread = typename Runtime::Thread;
    using ListenerContext = TcpListenerContext<Runtime>;

    TcpConnection(Shard *shard, std::shared_ptr<ListenerContext> listener, int fd,
                  std::uint32_t slot, std::uint32_t generation, TcpEndpoint local_endpoint,
                  TcpEndpoint peer_endpoint) noexcept
        : shard_(shard), listener_(std::move(listener)), fd_(fd), slot_(slot),
          generation_(generation), local_endpoint_(std::move(local_endpoint)),
          peer_endpoint_(std::move(peer_endpoint)) {
        channel_.fd = fd_;
        channel_.owner = this;
        channel_.on_event = &TcpConnection::on_channel_event;
    }

    TcpConnection(const TcpConnection &) = delete;
    TcpConnection &operator=(const TcpConnection &) = delete;

    ~TcpConnection() {
        close_now(CloseReason::Local);
    }

    [[nodiscard]] bool start() noexcept {
        return Runtime::net_register_channel(owner_thread(), &channel_,
                                             af::detail::net_io_readable);
    }

    [[nodiscard]] bool alive() const noexcept {
        return fd_ >= 0;
    }

    [[nodiscard]] std::uint32_t slot() const noexcept {
        return slot_;
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return generation_;
    }

    [[nodiscard]] ListenerId listener_id() const noexcept {
        return listener_ == nullptr ? ListenerId{} : listener_->id;
    }

    [[nodiscard]] std::string_view listener_name() const noexcept {
        return listener_ == nullptr ? std::string_view{} : std::string_view(listener_->name);
    }

    [[nodiscard]] const TcpEndpoint &local_endpoint() const noexcept {
        return local_endpoint_;
    }

    [[nodiscard]] const TcpEndpoint &peer_endpoint() const noexcept {
        return peer_endpoint_;
    }

    [[nodiscard]] std::size_t queued_bytes() const noexcept {
        return queued_bytes_;
    }

    [[nodiscard]] TcpConnectionHandle<Runtime> handle() const noexcept;

    [[nodiscard]] SendResult send(af::Buffer buffer) noexcept {
        if (!alive() || write_shutdown_requested_ || write_shutdown_done_) {
            return SendResult::Closed;
        }
        if (buffer.empty()) {
            return SendResult::Accepted;
        }
        const std::size_t buffer_size = buffer.size();
        const std::size_t high_watermark = listener_options().output_high_watermark;
        if (queued_bytes_ >= high_watermark || buffer_size > high_watermark - queued_bytes_) {
            return SendResult::Backpressure;
        }
        try {
            output_.push_back(std::move(buffer));
        } catch (...) {
            return SendResult::Backpressure;
        }
        queued_bytes_ += buffer_size;
        flush_output();
        return alive() ? SendResult::Accepted : SendResult::Closed;
    }

    [[nodiscard]] SendResult send(af::BufferView view) noexcept {
        if (!alive() || write_shutdown_requested_ || write_shutdown_done_) {
            return SendResult::Closed;
        }
        if (view.empty()) {
            return SendResult::Accepted;
        }
        const std::size_t high_watermark = listener_options().output_high_watermark;
        if (queued_bytes_ >= high_watermark || view.size() > high_watermark - queued_bytes_) {
            return SendResult::Backpressure;
        }
        if (output_.empty()) {
            for (;;) {
                const ssize_t n = ::send(fd_, view.data(), view.size(), send_no_signal_flags());
                if (n == static_cast<ssize_t>(view.size())) {
                    return SendResult::Accepted;
                }
                if (n > 0) {
                    const auto written = static_cast<std::size_t>(n);
                    view = af::BufferView(view.data() + written, view.size() - written);
                    break;
                }
                if (n == 0) {
                    break;
                }
                const int error = errno;
                if (error == EINTR) {
                    continue;
                }
                if (error == EAGAIN || error == EWOULDBLOCK) {
                    break;
                }
                close_now(CloseReason::Error);
                return SendResult::Closed;
            }
        }
        try {
            return send(af::Buffer::copy(view));
        } catch (...) {
            return SendResult::Backpressure;
        }
    }

    void close(CloseReason reason = CloseReason::Local) noexcept {
        close_now(reason);
    }

    void close_after_flush() noexcept {
        if (!alive()) {
            return;
        }
        read_paused_ = true;
        close_after_flush_ = true;
        flush_output();
    }

    [[nodiscard]] bool shutdown_write() noexcept {
        if (!alive() || write_shutdown_done_) {
            return false;
        }
        write_shutdown_requested_ = true;
        if (!output_.empty()) {
            update_interest();
            return true;
        }
        return shutdown_write_now();
    }

    void pause_read() noexcept {
        if (!alive()) {
            return;
        }
        read_paused_ = true;
        update_interest();
    }

    void resume_read() noexcept {
        if (!alive()) {
            return;
        }
        read_paused_ = false;
        update_interest();
    }

    [[nodiscard]] bool set_no_delay(bool enabled) noexcept {
        return alive() && tcp_socket_options_supported() && detail::set_tcp_no_delay(fd_, enabled);
    }

    [[nodiscard]] bool set_keepalive(bool enabled) noexcept {
        return alive() && tcp_socket_options_supported() &&
               detail::set_socket_keepalive(fd_, enabled);
    }

private:
    friend class TcpConnectionRef<Runtime>;

    static void on_channel_event(void *owner, std::uint32_t events) noexcept {
        auto *connection = static_cast<TcpConnection *>(owner);
        auto *shard = connection == nullptr ? nullptr : connection->shard_;
        if (connection != nullptr) {
            connection->on_event(events);
        }
        if (shard != nullptr) {
            shard->reap_retired_connections();
        }
    }

    [[nodiscard]] Thread owner_thread() const noexcept;
    [[nodiscard]] const TcpListenerOptions &listener_options() const noexcept;
    [[nodiscard]] std::weak_ptr<State> weak_state() const noexcept;

    [[nodiscard]] TcpHandlerBase<Runtime> *handler() noexcept {
        return listener_ == nullptr ? nullptr : listener_->handler.get();
    }

    [[nodiscard]] bool tcp_socket_options_supported() const noexcept {
        return listener_ != nullptr && listener_->endpoint.family != AddressFamily::Unix;
    }

    void on_event(std::uint32_t events) noexcept {
        if ((events & af::detail::net_io_error) != 0U) {
            close_now(CloseReason::Error);
            return;
        }
        if (!read_paused_ && (events & af::detail::net_io_readable) != 0U) {
            read_available();
        }
        if (alive() && (events & af::detail::net_io_writable) != 0U) {
            flush_output();
        }
        if (alive() && (events & af::detail::net_io_hangup) != 0U) {
            close_now(CloseReason::Peer);
        }
    }

    void read_available() noexcept {
        const std::size_t buffer_size = listener_options().read_buffer_size == 0U
                                            ? 16U * 1024U
                                            : listener_options().read_buffer_size;
        if (read_buffer_.size() < buffer_size) {
            try {
                read_buffer_.resize(buffer_size);
            } catch (...) {
                close_now(CloseReason::Error);
                return;
            }
        }
        std::size_t consumed = 0;
        while (alive() && !read_paused_ && consumed < listener_options().read_budget_bytes) {
            const ssize_t n = ::recv(fd_, read_buffer_.data(), read_buffer_.size(), 0);
            if (n > 0) {
                consumed += static_cast<std::size_t>(n);
                if (auto *h = handler(); h != nullptr) {
                    shard_->begin_user_callback();
                    h->on_read(TcpConnectionRef<Runtime>(this),
                               af::BufferView(read_buffer_.data(), static_cast<std::size_t>(n)));
                    shard_->end_user_callback();
                }
                continue;
            }
            if (n == 0) {
                close_now(CloseReason::Peer);
                return;
            }
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN || error == EWOULDBLOCK) {
                return;
            }
            close_now(CloseReason::Error);
            return;
        }
    }

    void flush_output() noexcept {
        std::size_t written_this_run = 0;
        const std::size_t write_budget = listener_options().write_budget_bytes;
        while (alive()) {
            if (output_.empty()) {
                break;
            }
            if (written_this_run >= write_budget) {
                update_interest();
                return;
            }
            const ssize_t n = flush_output_once(write_budget - written_this_run);
            if (n > 0) {
                const auto written = static_cast<std::size_t>(n);
                written_this_run += written;
                consume_output(written);
                continue;
            }
            if (n == 0) {
                break;
            }
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN || error == EWOULDBLOCK) {
                update_interest();
                return;
            }
            close_now(CloseReason::Error);
            return;
        }
        if (close_after_flush_) {
            close_now(CloseReason::Local);
            return;
        }
        if (write_shutdown_requested_ && !write_shutdown_done_) {
            static_cast<void>(shutdown_write_now());
            return;
        }
        update_interest();
    }

    [[nodiscard]] ssize_t flush_output_once(std::size_t max_bytes) noexcept {
        if (max_bytes == 0U) {
            return 0;
        }
        std::array<af::BufferView, 64> views{};
        std::array<iovec, 64> iov{};
        const std::size_t count = output_.fill_views(views);
        std::size_t send_count = 0;
        std::size_t remaining = max_bytes;
        for (std::size_t i = 0; i < count && remaining != 0U; ++i) {
            const std::size_t size = std::min(views[i].size(), remaining);
            if (size == 0U) {
                continue;
            }
            iov[send_count].iov_base = const_cast<std::byte *>(views[i].data());
            iov[send_count].iov_len = size;
            ++send_count;
            remaining -= size;
        }
        if (send_count == 0U) {
            return 0;
        }
        if (send_count == 1U) {
            return ::send(fd_, iov[0].iov_base, iov[0].iov_len, send_no_signal_flags());
        }

        msghdr message{};
        message.msg_iov = iov.data();
        message.msg_iovlen = static_cast<decltype(message.msg_iovlen)>(send_count);
        return ::sendmsg(fd_, &message, send_no_signal_flags());
    }

    void consume_output(std::size_t written) noexcept {
        const std::size_t consumed = std::min(written, queued_bytes_);
        queued_bytes_ -= consumed;
        output_.remove_prefix(consumed);
    }

    void update_interest() noexcept {
        if (!alive()) {
            return;
        }
        std::uint32_t events = 0;
        if (!read_paused_) {
            events |= af::detail::net_io_readable;
        }
        if (!output_.empty()) {
            events |= af::detail::net_io_writable;
        }
        if (events != channel_.interests) {
            static_cast<void>(Runtime::net_update_channel(owner_thread(), &channel_, events));
        }
    }

    void close_now(CloseReason reason) noexcept {
        if (fd_ < 0) {
            return;
        }
        static_cast<void>(Runtime::net_unregister_channel(owner_thread(), &channel_));
        detail::close_fd(fd_);
        if (auto *h = handler(); h != nullptr) {
            if (shard_ != nullptr) {
                shard_->begin_user_callback();
            }
            h->on_close(handle(), reason);
            if (shard_ != nullptr) {
                shard_->end_user_callback();
            }
        }
        if (shard_ != nullptr) {
            shard_->retire_connection(slot_, generation_);
        }
    }

    [[nodiscard]] bool shutdown_write_now() noexcept {
        if (!alive() || write_shutdown_done_) {
            return false;
        }
        if (::shutdown(fd_, SHUT_WR) != 0 && errno != ENOTCONN) {
            return false;
        }
        write_shutdown_done_ = true;
        update_interest();
        return true;
    }

    Shard *shard_{nullptr};
    std::shared_ptr<ListenerContext> listener_;
    int fd_{-1};
    std::uint32_t slot_{0};
    std::uint32_t generation_{0};
    TcpEndpoint local_endpoint_;
    TcpEndpoint peer_endpoint_;
    af::detail::NetIoChannel channel_{};
    af::BufferChain output_;
    std::vector<std::byte> read_buffer_;
    std::size_t queued_bytes_{0};
    bool read_paused_{false};
    bool close_after_flush_{false};
    bool write_shutdown_requested_{false};
    bool write_shutdown_done_{false};
};

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
                detail::close_fd(listener_fd_);
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
        detail::close_fd(listener_fd_);
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
                    detail::close_fd(fd);
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

template <typename Runtime> class TcpServerShard {
public:
    using State = TcpServerState<Runtime>;
    using Thread = typename Runtime::Thread;
    using Connection = TcpConnection<Runtime>;
    using ListenerContext = TcpListenerContext<Runtime>;
    using ListenerShard = TcpListenerShard<Runtime>;

    struct ConnectionSlot {
        std::uint32_t index{0};
        std::uint32_t generation{1};
    };

    TcpServerShard(std::weak_ptr<State> state, std::uint16_t shard_index, Thread thread)
        : state_(std::move(state)), shard_index_(shard_index), thread_(thread) {}

    TcpServerShard(const TcpServerShard &) = delete;
    TcpServerShard &operator=(const TcpServerShard &) = delete;

    ~TcpServerShard() = default;

    [[nodiscard]] Thread thread() const noexcept {
        return thread_;
    }

    [[nodiscard]] std::weak_ptr<State> weak_state() const noexcept {
        return state_;
    }

    [[nodiscard]] SendResult send_to(std::uint32_t slot, std::uint32_t generation,
                                     af::Buffer buffer) noexcept {
        Connection *connection = find(slot, generation);
        if (connection == nullptr) {
            return SendResult::Closed;
        }
        const SendResult result = connection->send(std::move(buffer));
        reap_retired_connections_if_safe();
        return result;
    }

    [[nodiscard]] SendResult send_to(std::uint32_t slot, std::uint32_t generation,
                                     af::BufferView view) noexcept {
        Connection *connection = find(slot, generation);
        if (connection == nullptr) {
            return SendResult::Closed;
        }
        const SendResult result = connection->send(view);
        reap_retired_connections_if_safe();
        return result;
    }

    [[nodiscard]] bool close_connection(std::uint32_t slot, std::uint32_t generation) noexcept {
        Connection *connection = find(slot, generation);
        if (connection == nullptr) {
            return false;
        }
        connection->close(CloseReason::Local);
        reap_retired_connections_if_safe();
        return true;
    }

    [[nodiscard]] bool close_connection_after_flush(std::uint32_t slot,
                                                    std::uint32_t generation) noexcept {
        Connection *connection = find(slot, generation);
        if (connection == nullptr) {
            return false;
        }
        connection->close_after_flush();
        reap_retired_connections_if_safe();
        return true;
    }

    [[nodiscard]] bool shutdown_connection_write(std::uint32_t slot,
                                                 std::uint32_t generation) noexcept {
        Connection *connection = find(slot, generation);
        if (connection == nullptr) {
            return false;
        }
        return connection->shutdown_write();
    }

    [[nodiscard]] bool pause_connection_read(std::uint32_t slot,
                                             std::uint32_t generation) noexcept {
        Connection *connection = find(slot, generation);
        if (connection == nullptr) {
            return false;
        }
        connection->pause_read();
        return true;
    }

    [[nodiscard]] bool resume_connection_read(std::uint32_t slot,
                                              std::uint32_t generation) noexcept {
        Connection *connection = find(slot, generation);
        if (connection == nullptr) {
            return false;
        }
        connection->resume_read();
        return true;
    }

    [[nodiscard]] bool set_connection_no_delay(std::uint32_t slot, std::uint32_t generation,
                                               bool enabled) noexcept {
        Connection *connection = find(slot, generation);
        if (connection == nullptr) {
            return false;
        }
        return connection->set_no_delay(enabled);
    }

    [[nodiscard]] bool set_connection_keepalive(std::uint32_t slot, std::uint32_t generation,
                                                bool enabled) noexcept {
        Connection *connection = find(slot, generation);
        if (connection == nullptr) {
            return false;
        }
        return connection->set_keepalive(enabled);
    }

    [[nodiscard]] int add_listener_on_owner(std::uint32_t listener_slot,
                                            std::shared_ptr<ListenerContext> context,
                                            bool open_listener) noexcept {
        try {
            if (listener_slot >= listeners_.size()) {
                listeners_.resize(static_cast<std::size_t>(listener_slot) + 1U);
            }
        } catch (...) {
            return ENOMEM;
        }
        if (listeners_[listener_slot] != nullptr) {
            return EALREADY;
        }
        std::unique_ptr<ListenerShard> listener;
        try {
            listener = std::make_unique<ListenerShard>(this, std::move(context));
        } catch (...) {
            return ENOMEM;
        }
        if (open_listener) {
            const int error = listener->open();
            if (error != 0) {
                return error;
            }
        }
        listeners_[listener_slot] = std::move(listener);
        return 0;
    }

    void remove_listener_on_owner(ListenerId id, RemoveListenerPolicy policy) noexcept {
        if (id.slot < listeners_.size()) {
            auto &listener = listeners_[id.slot];
            if (listener != nullptr && listener->id() == id) {
                listener->close();
                listener.reset();
            }
        }
        if (policy == RemoveListenerPolicy::CloseExistingConnections) {
            for (auto &connection : connections_) {
                if (connection != nullptr && connection->alive() &&
                    connection->listener_id() == id) {
                    connection->close(CloseReason::Local);
                }
            }
            reap_retired_connections();
        }
    }

    void close_listeners_on_owner() noexcept {
        for (auto &listener : listeners_) {
            if (listener != nullptr) {
                listener->close();
            }
        }
        listeners_.clear();
    }

    [[nodiscard]] std::size_t close_connections_after_flush_on_owner() noexcept {
        for (auto &connection : connections_) {
            if (connection != nullptr && connection->alive()) {
                connection->close_after_flush();
            }
        }
        reap_retired_connections();
        return alive_connection_count();
    }

    void force_close_connections_on_owner() noexcept {
        for (auto &connection : connections_) {
            if (connection != nullptr && connection->alive()) {
                connection->close(CloseReason::Local);
            }
        }
        reap_retired_connections();
    }

    void stop_on_owner() noexcept {
        close_listeners_on_owner();
        force_close_connections_on_owner();
    }

    [[nodiscard]] bool create_connection(std::shared_ptr<ListenerContext> context, int fd,
                                         const sockaddr *peer, socklen_t peer_size) noexcept {
        ConnectionSlot slot{};
        if (!try_acquire_connection_slot(slot)) {
            detail::close_fd(fd);
            notify_listener_error(context, ENOMEM);
            return false;
        }

        Connection *connection_ptr = nullptr;
        try {
            sockaddr_storage local{};
            socklen_t local_size = sizeof(local);
            TcpEndpoint local_endpoint{};
            if (::getsockname(fd, reinterpret_cast<sockaddr *>(&local), &local_size) == 0) {
                local_endpoint = ::af::detail::endpoint_from_socket_address(
                    reinterpret_cast<const sockaddr *>(&local), local_size);
            }
            TcpEndpoint peer_endpoint = ::af::detail::endpoint_from_socket_address(peer, peer_size);
            auto connection =
                std::make_unique<Connection>(this, context, fd, slot.index, slot.generation,
                                             std::move(local_endpoint), std::move(peer_endpoint));
            connection_ptr = connection.get();
            connections_[slot.index] = std::move(connection);
        } catch (...) {
            release_unused_connection_slot(slot);
            detail::close_fd(fd);
            notify_listener_error(context, ENOMEM);
            return false;
        }
        if (!connection_ptr->start()) {
            connection_ptr->close(CloseReason::Error);
            reap_retired_connections();
            return false;
        }
        auto ref = TcpConnectionRef<Runtime>(connection_ptr);
        if (auto *handler = ref.handler_for_dispatch(); handler != nullptr) {
            begin_user_callback();
            handler->on_accept(ref);
            end_user_callback();
            reap_retired_connections();
        }
        return true;
    }

    [[nodiscard]] bool route_connection(std::shared_ptr<ListenerContext> context, int fd,
                                        const sockaddr *peer, socklen_t peer_size) noexcept {
        if (context == nullptr || fd < 0 || peer == nullptr ||
            peer_size > sizeof(sockaddr_storage)) {
            return false;
        }

        std::uint16_t target_shard = shard_index_;
        const std::size_t target_count = context->target_shards.size();
        if (target_count != 0U) {
            const std::uint32_t cursor = context->next_target_shard++;
            target_shard = context->target_shards[cursor % target_count];
        }

        if (target_shard == shard_index_) {
            return create_connection(std::move(context), fd, peer, peer_size);
        }
        return schedule_adopt_connection(target_shard, context->id, fd, peer, peer_size);
    }

private:
    friend class TcpConnection<Runtime>;
    friend class TcpListenerShard<Runtime>;
    friend class TcpConnectionHandle<Runtime>;
    friend class TcpAdoptConnectionTask<Runtime>;
    friend class TcpConnectionCommandTask<Runtime>;

    [[nodiscard]] Connection *find(std::uint32_t slot, std::uint32_t generation) noexcept {
        if (slot >= connections_.size()) {
            return nullptr;
        }
        Connection *connection = connections_[slot].get();
        if (connection == nullptr || !connection->alive() ||
            connection->generation() != generation) {
            return nullptr;
        }
        return connection;
    }

    [[nodiscard]] bool try_acquire_connection_slot(ConnectionSlot &slot) noexcept {
        try {
            slot = acquire_connection_slot();
            return true;
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] ConnectionSlot acquire_connection_slot() {
        if (!free_connection_slots_.empty()) {
            const std::uint32_t slot = free_connection_slots_.back();
            free_connection_slots_.pop_back();
            std::uint32_t next_generation = generations_[slot] + 1U;
            if (next_generation == 0U) {
                next_generation = 1U;
            }
            generations_[slot] = next_generation;
            return ConnectionSlot{slot, next_generation};
        }

        const auto slot = static_cast<std::uint32_t>(connections_.size());
        connections_.reserve(static_cast<std::size_t>(slot) + 1U);
        if (slot >= generations_.size()) {
            generations_.reserve(static_cast<std::size_t>(slot) + 1U);
        }
        connections_.push_back(nullptr);
        if (slot >= generations_.size()) {
            generations_.push_back(1U);
            return ConnectionSlot{slot, 1U};
        }
        std::uint32_t next_generation = generations_[slot] + 1U;
        if (next_generation == 0U) {
            next_generation = 1U;
        }
        generations_[slot] = next_generation;
        return ConnectionSlot{slot, next_generation};
    }

    void release_unused_connection_slot(ConnectionSlot slot) noexcept {
        if (slot.index >= connections_.size() || slot.index >= generations_.size() ||
            generations_[slot.index] != slot.generation || connections_[slot.index] != nullptr) {
            return;
        }
        if (slot.index + 1U < connections_.size()) {
            try {
                free_connection_slots_.push_back(slot.index);
            } catch (...) {
            }
        }
        trim_empty_tail_slots();
    }

    void retire_connection(std::uint32_t slot, std::uint32_t generation) noexcept {
        try {
            retired_connection_slots_.push_back(ConnectionSlot{slot, generation});
        } catch (...) {
        }
    }

    void reap_retired_connections() noexcept {
        if (retired_connection_slots_.empty()) {
            return;
        }
        for (const ConnectionSlot retired : retired_connection_slots_) {
            if (retired.index >= connections_.size() || retired.index >= generations_.size() ||
                generations_[retired.index] != retired.generation) {
                continue;
            }
            auto &connection = connections_[retired.index];
            if (connection == nullptr || connection->alive() ||
                connection->generation() != retired.generation) {
                continue;
            }
            connection.reset();
            if (retired.index + 1U < connections_.size()) {
                try {
                    free_connection_slots_.push_back(retired.index);
                } catch (...) {
                }
            }
        }
        retired_connection_slots_.clear();
        trim_empty_tail_slots();
    }

    void reap_retired_connections_if_safe() noexcept {
        if (user_callback_depth_ == 0U) {
            reap_retired_connections();
        }
    }

    [[nodiscard]] std::size_t alive_connection_count() const noexcept {
        std::size_t count = 0;
        for (const auto &connection : connections_) {
            if (connection != nullptr && connection->alive()) {
                ++count;
            }
        }
        return count;
    }

    void begin_user_callback() noexcept {
        ++user_callback_depth_;
    }

    void end_user_callback() noexcept {
        if (user_callback_depth_ > 0U) {
            --user_callback_depth_;
        }
    }

    void trim_empty_tail_slots() noexcept {
        while (!connections_.empty() && connections_.back() == nullptr) {
            const auto tail = static_cast<std::uint32_t>(connections_.size() - 1U);
            erase_free_connection_slot(tail);
            connections_.pop_back();
        }
    }

    void erase_free_connection_slot(std::uint32_t slot) noexcept {
        for (auto it = free_connection_slots_.rbegin(); it != free_connection_slots_.rend(); ++it) {
            if (*it == slot) {
                free_connection_slots_.erase(std::next(it).base());
                return;
            }
        }
    }

    [[nodiscard]] std::shared_ptr<ListenerContext> find_listener_context(ListenerId id) noexcept {
        if (id.slot >= listeners_.size()) {
            return nullptr;
        }
        const auto &listener = listeners_[id.slot];
        if (listener == nullptr || listener->id() != id) {
            return nullptr;
        }
        return listener->context();
    }

    [[nodiscard]] bool schedule_adopt_connection(std::uint16_t target_shard, ListenerId listener_id,
                                                 int fd, const sockaddr *peer,
                                                 socklen_t peer_size) noexcept {
        auto state = state_.lock();
        if (state == nullptr || target_shard >= state->shards.size() ||
            state->shards[target_shard] == nullptr || peer == nullptr ||
            peer_size > sizeof(sockaddr_storage)) {
            return false;
        }
        try {
            return Runtime::template start_task<TcpAdoptConnectionTask<Runtime>>(
                std::move(state), target_shard, listener_id, fd, peer, peer_size);
        } catch (...) {
            return false;
        }
    }

    void adopt_connection(ListenerId listener_id, int fd, const sockaddr *peer,
                          socklen_t peer_size) noexcept {
        auto context = find_listener_context(listener_id);
        if (context == nullptr || fd < 0 || peer == nullptr ||
            peer_size > sizeof(sockaddr_storage)) {
            detail::close_fd(fd);
            return;
        }
        static_cast<void>(create_connection(std::move(context), fd, peer, peer_size));
    }

    static void notify_listener_error(const std::shared_ptr<ListenerContext> &context,
                                      int error) noexcept {
        if (context != nullptr && context->handler != nullptr) {
            context->handler->on_listener_error(TcpListenerHandle{context->id}, error);
        }
    }

    std::weak_ptr<State> state_;
    std::uint16_t shard_index_{0};
    Thread thread_;
    std::vector<std::unique_ptr<ListenerShard>> listeners_;
    std::vector<std::unique_ptr<Connection>> connections_;
    std::vector<std::uint32_t> generations_;
    std::vector<ConnectionSlot> retired_connection_slots_;
    std::vector<std::uint32_t> free_connection_slots_;
    std::uint32_t user_callback_depth_{0};
};

template <typename Runtime> bool TcpListenerShard<Runtime>::register_channel() noexcept {
    return Runtime::net_register_channel(shard_->thread(), &listener_channel_,
                                         af::detail::net_io_readable);
}

template <typename Runtime> void TcpListenerShard<Runtime>::unregister_channel() noexcept {
    static_cast<void>(Runtime::net_unregister_channel(shard_->thread(), &listener_channel_));
}

template <typename Runtime>
bool TcpListenerShard<Runtime>::route_connection(int fd, const sockaddr *peer,
                                                 socklen_t peer_size) noexcept {
    return shard_->route_connection(context_, fd, peer, peer_size);
}

template <typename Runtime>
typename Runtime::Thread TcpConnection<Runtime>::owner_thread() const noexcept {
    return shard_->thread();
}

template <typename Runtime>
const TcpListenerOptions &TcpConnection<Runtime>::listener_options() const noexcept {
    static const TcpListenerOptions fallback{};
    return listener_ == nullptr ? fallback : listener_->options;
}

template <typename Runtime>
std::weak_ptr<TcpServerState<Runtime>> TcpConnection<Runtime>::weak_state() const noexcept {
    return shard_->weak_state();
}

template <typename Runtime> class TcpListenerStartResultTask;

template <typename Runtime>
void complete_listener_start_from_shard(std::shared_ptr<TcpServerState<Runtime>> state,
                                        ListenerId listener_id, std::uint16_t shard_index,
                                        int error) noexcept;

template <typename Runtime> class TcpAddListenerTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;
    using HandlerPtr = std::unique_ptr<TcpHandlerBase<Runtime>>;
    using ListenerContext = TcpListenerContext<Runtime>;

public:
    explicit TcpAddListenerTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t listener_slot,
               ListenerId listener_id, std::string name, TcpEndpoint endpoint,
               TcpListenerOptions options, std::vector<std::uint16_t> target_shards,
               HandlerPtr handler, bool open_listener) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        listener_slot_ = listener_slot;
        listener_id_ = listener_id;
        name_ = std::move(name);
        endpoint_ = std::move(endpoint);
        options_ = options;
        target_shards_ = std::move(target_shards);
        handler_ = std::move(handler);
        open_listener_ = open_listener;
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || handler_ == nullptr) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

private:
    af::TaskResult run() override {
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr && handler_ != nullptr) {
            int error = EINVAL;
            try {
                auto context = std::make_shared<ListenerContext>();
                context->id = listener_id_;
                context->name = std::move(name_);
                context->endpoint = std::move(endpoint_);
                context->options = options_;
                context->target_shards = std::move(target_shards_);
                context->handler = std::move(handler_);
                error = state_->shards[shard_index_]->add_listener_on_owner(
                    listener_slot_, std::move(context), open_listener_);
            } catch (...) {
                error = ENOMEM;
            }
            complete_listener_start_from_shard<Runtime>(state_, listener_id_, shard_index_, error);
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::uint32_t listener_slot_{0};
    ListenerId listener_id_{};
    std::string name_;
    TcpEndpoint endpoint_;
    TcpListenerOptions options_;
    std::vector<std::uint16_t> target_shards_;
    HandlerPtr handler_;
    bool open_listener_{false};
};

template <typename Runtime> class TcpRemoveListenerTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpRemoveListenerTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, ListenerId listener_id,
               RemoveListenerPolicy policy) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        listener_id_ = listener_id;
        policy_ = policy;
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
            state_->shards[shard_index_]->remove_listener_on_owner(listener_id_, policy_);
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    ListenerId listener_id_{};
    RemoveListenerPolicy policy_{RemoveListenerPolicy::StopAcceptOnly};
};

template <typename Runtime> class TcpStopShardTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;
    using Clock = std::chrono::steady_clock;

public:
    explicit TcpStopShardTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index,
               std::chrono::milliseconds close_timeout) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        close_timeout_ = close_timeout.count() < 0 ? std::chrono::milliseconds(0) : close_timeout;
        deadline_ = Clock::now() + close_timeout_;
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
            auto *shard = state_->shards[shard_index_].get();
            if (!listeners_closed_) {
                shard->close_listeners_on_owner();
                listeners_closed_ = true;
            }

            const std::size_t remaining =
                close_timeout_.count() <= 0 ? 0U : shard->close_connections_after_flush_on_owner();
            const auto now = Clock::now();
            if (remaining == 0U || close_timeout_.count() <= 0 || now >= deadline_) {
                shard->force_close_connections_on_owner();
                return this->done();
            }

            auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(deadline_ - now);
            if (delay > std::chrono::milliseconds(1)) {
                delay = std::chrono::milliseconds(1);
            }
            return this->pending_after(Runtime::thread_from_index(shard_index_), delay);
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::chrono::milliseconds close_timeout_{0};
    Clock::time_point deadline_{};
    bool listeners_closed_{false};
};

template <typename Runtime> class TcpAdoptConnectionTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpAdoptConnectionTask(typename Base::FactoryToken token) : Base(token) {}

    ~TcpAdoptConnectionTask() override {
        detail::close_fd(fd_);
    }

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, ListenerId listener_id,
               int fd, const sockaddr *peer, socklen_t peer_size) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        listener_id_ = listener_id;
        fd_ = fd;
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || fd_ < 0 || peer == nullptr ||
            peer_size > sizeof(peer_)) {
            fd_ = -1;
            return false;
        }
        peer_size_ = peer_size;
        std::memcpy(&peer_, peer, peer_size_);
        if (!this->schedule(Runtime::thread_from_index(shard_index_))) {
            fd_ = -1;
            return false;
        }
        return true;
    }

private:
    af::TaskResult run() override {
        int fd = fd_;
        fd_ = -1;
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr && fd >= 0) {
            state_->shards[shard_index_]->adopt_connection(
                listener_id_, fd, reinterpret_cast<const sockaddr *>(&peer_), peer_size_);
            return this->done();
        }
        detail::close_fd(fd);
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    ListenerId listener_id_{};
    int fd_{-1};
    sockaddr_storage peer_{};
    socklen_t peer_size_{0};
};

template <typename Runtime> class TcpConnectionCommandTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpConnectionCommandTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t slot,
               std::uint32_t generation, af::Buffer buffer) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        slot_ = slot;
        generation_ = generation;
        kind_ = TcpConnectionCommandKind::Send;
        buffer_ = std::move(buffer);
        return schedule_on_owner();
    }

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t slot,
               std::uint32_t generation, TcpConnectionCommandKind kind, bool flag) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        slot_ = slot;
        generation_ = generation;
        kind_ = kind;
        flag_ = flag;
        return schedule_on_owner();
    }

private:
    [[nodiscard]] bool schedule_on_owner() noexcept {
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
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
        switch (kind_) {
        case TcpConnectionCommandKind::Send:
            static_cast<void>(shard->send_to(slot_, generation_, std::move(buffer_)));
            break;
        case TcpConnectionCommandKind::Close:
            static_cast<void>(shard->close_connection(slot_, generation_));
            break;
        case TcpConnectionCommandKind::CloseAfterFlush:
            static_cast<void>(shard->close_connection_after_flush(slot_, generation_));
            break;
        case TcpConnectionCommandKind::ShutdownWrite:
            static_cast<void>(shard->shutdown_connection_write(slot_, generation_));
            break;
        case TcpConnectionCommandKind::PauseRead:
            static_cast<void>(shard->pause_connection_read(slot_, generation_));
            break;
        case TcpConnectionCommandKind::ResumeRead:
            static_cast<void>(shard->resume_connection_read(slot_, generation_));
            break;
        case TcpConnectionCommandKind::SetNoDelay:
            static_cast<void>(shard->set_connection_no_delay(slot_, generation_, flag_));
            break;
        case TcpConnectionCommandKind::SetKeepAlive:
            static_cast<void>(shard->set_connection_keepalive(slot_, generation_, flag_));
            break;
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::uint32_t slot_{0};
    std::uint32_t generation_{0};
    TcpConnectionCommandKind kind_{TcpConnectionCommandKind::Close};
    bool flag_{false};
    af::Buffer buffer_;
};

[[nodiscard]] inline bool contains_shard_index(const std::vector<std::uint16_t> &shards,
                                               std::uint16_t shard) noexcept {
    for (const std::uint16_t candidate : shards) {
        if (candidate == shard) {
            return true;
        }
    }
    return false;
}

template <typename Runtime>
void schedule_remove_listener_from_shard(const std::shared_ptr<TcpServerState<Runtime>> &state,
                                         ListenerId id, std::uint16_t shard_index,
                                         RemoveListenerPolicy policy) noexcept {
    if (state == nullptr || shard_index >= state->shards.size() ||
        state->shards[shard_index] == nullptr) {
        return;
    }
    try {
        static_cast<void>(Runtime::template start_task<TcpRemoveListenerTask<Runtime>>(
            state, shard_index, id, policy));
    } catch (...) {
    }
}

template <typename Runtime>
void schedule_remove_listener_from_shards(const std::shared_ptr<TcpServerState<Runtime>> &state,
                                          ListenerId id, const std::vector<std::uint16_t> &shards,
                                          RemoveListenerPolicy policy) noexcept {
    for (const std::uint16_t shard_index : shards) {
        schedule_remove_listener_from_shard<Runtime>(state, id, shard_index, policy);
    }
}

template <typename Runtime>
void clear_listener_start_progress(TcpListenerEntry<Runtime> &entry) noexcept {
    entry.starting_shards.clear();
    entry.started_shards.clear();
    entry.pending_start_shards = 0U;
}

template <typename Runtime>
void handle_listener_start_result_on_control(const std::shared_ptr<TcpServerState<Runtime>> &state,
                                             ListenerId id, std::uint16_t shard_index,
                                             int error) noexcept {
    if (state == nullptr || id.slot >= state->listeners.size()) {
        return;
    }
    auto &entry_ptr = state->listeners[id.slot];
    if (entry_ptr == nullptr || entry_ptr->id != id) {
        if (error == 0) {
            schedule_remove_listener_from_shard<Runtime>(state, id, shard_index,
                                                         RemoveListenerPolicy::StopAcceptOnly);
        }
        return;
    }

    auto &entry = *entry_ptr;
    if (entry.state != ListenerState::Starting) {
        if (error == 0) {
            schedule_remove_listener_from_shard<Runtime>(state, id, shard_index,
                                                         RemoveListenerPolicy::StopAcceptOnly);
        }
        return;
    }

    if (error != 0) {
        const int reported_error = error == 0 ? EIO : error;
        std::vector<std::uint16_t> started;
        try {
            started = entry.started_shards;
        } catch (...) {
            started.clear();
        }
        entry.state = ListenerState::Failed;
        entry.active_shards.clear();
        entry.start_error = reported_error;
        clear_listener_start_progress(entry);
        if (entry.handler_prototype != nullptr) {
            entry.handler_prototype->on_listener_error(TcpListenerHandle{id}, reported_error);
        }
        schedule_remove_listener_from_shards<Runtime>(state, id, started,
                                                      RemoveListenerPolicy::StopAcceptOnly);
        return;
    }

    if (!contains_shard_index(entry.started_shards, shard_index)) {
        try {
            entry.started_shards.push_back(shard_index);
        } catch (...) {
            handle_listener_start_result_on_control<Runtime>(state, id, shard_index, ENOMEM);
            return;
        }
    }
    if (entry.pending_start_shards > 0U) {
        --entry.pending_start_shards;
    }
    if (entry.pending_start_shards != 0U) {
        return;
    }

    std::vector<std::uint16_t> installed;
    try {
        installed = entry.starting_shards;
    } catch (...) {
        std::vector<std::uint16_t> started;
        try {
            started = entry.started_shards;
        } catch (...) {
            started.clear();
        }
        entry.state = ListenerState::Failed;
        entry.active_shards.clear();
        clear_listener_start_progress(entry);
        if (entry.handler_prototype != nullptr) {
            entry.handler_prototype->on_listener_error(TcpListenerHandle{id}, ENOMEM);
        }
        schedule_remove_listener_from_shards<Runtime>(state, id, started,
                                                      RemoveListenerPolicy::StopAcceptOnly);
        return;
    }

    if (state->running) {
        entry.active_shards = std::move(installed);
        entry.state = ListenerState::Active;
        entry.start_error = 0;
        clear_listener_start_progress(entry);
        return;
    }

    std::vector<std::uint16_t> started = std::move(entry.started_shards);
    entry.state = ListenerState::Configured;
    entry.active_shards.clear();
    clear_listener_start_progress(entry);
    schedule_remove_listener_from_shards<Runtime>(state, id, started,
                                                  RemoveListenerPolicy::CloseExistingConnections);
}

template <typename Runtime> class TcpListenerStartResultTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpListenerStartResultTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, ListenerId listener_id, std::uint16_t shard_index,
               int error) {
        state_ = std::move(state);
        listener_id_ = listener_id;
        shard_index_ = shard_index;
        error_ = error;
        if (state_ == nullptr || state_->control_thread_index >= Runtime::thread_count) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(state_->control_thread_index));
    }

private:
    af::TaskResult run() override {
        handle_listener_start_result_on_control<Runtime>(state_, listener_id_, shard_index_,
                                                         error_);
        return this->done();
    }

    std::shared_ptr<State> state_;
    ListenerId listener_id_{};
    std::uint16_t shard_index_{0};
    int error_{0};
};

template <typename Runtime>
void complete_listener_start_from_shard(std::shared_ptr<TcpServerState<Runtime>> state,
                                        ListenerId listener_id, std::uint16_t shard_index,
                                        int error) noexcept {
    if (state == nullptr || state->control_thread_index >= Runtime::thread_count) {
        return;
    }
    if (Runtime::current_thread_index() == state->control_thread_index) {
        handle_listener_start_result_on_control<Runtime>(state, listener_id, shard_index, error);
        return;
    }
    try {
        static_cast<void>(Runtime::template start_task<TcpListenerStartResultTask<Runtime>>(
            std::move(state), listener_id, shard_index, error));
    } catch (...) {
    }
}

} // namespace detail

template <typename Runtime> class TcpConnectionHandle {
public:
    using State = detail::TcpServerState<Runtime>;
    using CommandKind = detail::TcpConnectionCommandKind;

    TcpConnectionHandle() = default;

    TcpConnectionHandle(std::weak_ptr<State> state, std::uint16_t shard_index, std::uint32_t slot,
                        std::uint32_t generation, ListenerId listener_id) noexcept
        : state_(std::move(state)), shard_index_(shard_index), slot_(slot), generation_(generation),
          listener_id_(listener_id) {}

    [[nodiscard]] std::uint32_t slot() const noexcept {
        return slot_;
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return generation_;
    }

    [[nodiscard]] ListenerId listener_id() const noexcept {
        return listener_id_;
    }

    [[nodiscard]] TcpListenerHandle listener() const noexcept {
        return TcpListenerHandle{listener_id_};
    }

    [[nodiscard]] SendResult send(af::Buffer buffer) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size() ||
            state->shards[shard_index_] == nullptr) {
            return SendResult::Closed;
        }
        auto *shard = state->shards[shard_index_].get();
        if (Runtime::is_runtime_thread() && Runtime::current_thread_index() == shard_index_) {
            return shard->send_to(slot_, generation_, std::move(buffer));
        }
        if (!state->accepting_connection_tasks.load(std::memory_order_acquire)) {
            return SendResult::Closed;
        }
        return schedule_send_on_owner(std::move(state), std::move(buffer));
    }

    [[nodiscard]] SendResult send(af::BufferView view) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size() ||
            state->shards[shard_index_] == nullptr) {
            return SendResult::Closed;
        }
        auto *shard = state->shards[shard_index_].get();
        if (Runtime::is_runtime_thread() && Runtime::current_thread_index() == shard_index_) {
            return shard->send_to(slot_, generation_, view);
        }
        if (!state->accepting_connection_tasks.load(std::memory_order_acquire)) {
            return SendResult::Closed;
        }
        try {
            return schedule_send_on_owner(std::move(state), af::Buffer::copy(view));
        } catch (...) {
            return SendResult::Backpressure;
        }
    }

    [[nodiscard]] bool close() const {
        return schedule_command(CommandKind::Close);
    }

    [[nodiscard]] bool close_after_flush() const {
        return schedule_command(CommandKind::CloseAfterFlush);
    }

    [[nodiscard]] bool shutdown_write() const {
        return schedule_command(CommandKind::ShutdownWrite);
    }

    [[nodiscard]] bool pause_read() const {
        return schedule_command(CommandKind::PauseRead);
    }

    [[nodiscard]] bool resume_read() const {
        return schedule_command(CommandKind::ResumeRead);
    }

    [[nodiscard]] bool set_no_delay(bool enabled) const {
        return schedule_command(CommandKind::SetNoDelay, enabled);
    }

    [[nodiscard]] bool set_keepalive(bool enabled) const {
        return schedule_command(CommandKind::SetKeepAlive, enabled);
    }

    [[nodiscard]] friend bool operator==(TcpConnectionHandle lhs,
                                         TcpConnectionHandle rhs) noexcept {
        return lhs.shard_index_ == rhs.shard_index_ && lhs.slot_ == rhs.slot_ &&
               lhs.generation_ == rhs.generation_;
    }

private:
    [[nodiscard]] SendResult schedule_send_on_owner(std::shared_ptr<State> state,
                                                    af::Buffer buffer) const {
        if (buffer.empty()) {
            return SendResult::Queued;
        }
        try {
            if (Runtime::template start_task<detail::TcpConnectionCommandTask<Runtime>>(
                    std::move(state), shard_index_, slot_, generation_, std::move(buffer))) {
                return SendResult::Queued;
            }
        } catch (...) {
        }
        return SendResult::Backpressure;
    }

    [[nodiscard]] bool schedule_command(CommandKind kind, bool flag = false) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size() ||
            state->shards[shard_index_] == nullptr) {
            return false;
        }
        auto *shard = state->shards[shard_index_].get();
        const bool on_owner_thread =
            Runtime::is_runtime_thread() && Runtime::current_thread_index() == shard_index_;
        if (on_owner_thread) {
            return dispatch_on_owner(*shard, kind, flag);
        }
        if (!state->accepting_connection_tasks.load(std::memory_order_acquire)) {
            return false;
        }
        try {
            return Runtime::template start_task<detail::TcpConnectionCommandTask<Runtime>>(
                std::move(state), shard_index_, slot_, generation_, kind, flag);
        } catch (...) {
            return false;
        }
    }

    [[nodiscard]] bool dispatch_on_owner(detail::TcpServerShard<Runtime> &shard, CommandKind kind,
                                         bool flag) const noexcept {
        switch (kind) {
        case CommandKind::Send:
            return false;
        case CommandKind::Close:
            return shard.close_connection(slot_, generation_);
        case CommandKind::CloseAfterFlush:
            return shard.close_connection_after_flush(slot_, generation_);
        case CommandKind::ShutdownWrite:
            return shard.shutdown_connection_write(slot_, generation_);
        case CommandKind::PauseRead:
            return shard.pause_connection_read(slot_, generation_);
        case CommandKind::ResumeRead:
            return shard.resume_connection_read(slot_, generation_);
        case CommandKind::SetNoDelay:
            return shard.set_connection_no_delay(slot_, generation_, flag);
        case CommandKind::SetKeepAlive:
            return shard.set_connection_keepalive(slot_, generation_, flag);
        }
        return false;
    }

    std::weak_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::uint32_t slot_{0};
    std::uint32_t generation_{0};
    ListenerId listener_id_{};
};

template <typename Runtime> class TcpConnectionRef {
public:
    explicit TcpConnectionRef(detail::TcpConnection<Runtime> *connection = nullptr) noexcept
        : connection_(connection) {}

    [[nodiscard]] bool valid() const noexcept {
        return connection_ != nullptr && connection_->alive();
    }

    [[nodiscard]] TcpConnectionHandle<Runtime> handle() const noexcept {
        return connection_ == nullptr ? TcpConnectionHandle<Runtime>{} : connection_->handle();
    }

    [[nodiscard]] std::uint32_t slot() const noexcept {
        return connection_ == nullptr ? 0U : connection_->slot();
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return connection_ == nullptr ? 0U : connection_->generation();
    }

    [[nodiscard]] ListenerId listener_id() const noexcept {
        return connection_ == nullptr ? ListenerId{} : connection_->listener_id();
    }

    [[nodiscard]] TcpListenerHandle listener() const noexcept {
        return TcpListenerHandle{listener_id()};
    }

    [[nodiscard]] std::string_view listener_name() const noexcept {
        return connection_ == nullptr ? std::string_view{} : connection_->listener_name();
    }

    [[nodiscard]] const TcpEndpoint &local_endpoint() const noexcept {
        static const TcpEndpoint empty{};
        return connection_ == nullptr ? empty : connection_->local_endpoint();
    }

    [[nodiscard]] const TcpEndpoint &peer_endpoint() const noexcept {
        static const TcpEndpoint empty{};
        return connection_ == nullptr ? empty : connection_->peer_endpoint();
    }

    [[nodiscard]] std::size_t queued_bytes() const noexcept {
        return connection_ == nullptr ? 0U : connection_->queued_bytes();
    }

    [[nodiscard]] SendResult send(af::Buffer buffer) const noexcept {
        return connection_ == nullptr ? SendResult::Closed : connection_->send(std::move(buffer));
    }

    [[nodiscard]] SendResult send(af::BufferView view) const noexcept {
        return connection_ == nullptr ? SendResult::Closed : connection_->send(view);
    }

    void close() const noexcept {
        if (connection_ != nullptr) {
            connection_->close();
        }
    }

    void close(CloseReason reason) const noexcept {
        if (connection_ != nullptr) {
            connection_->close(reason);
        }
    }

    void close_after_flush() const noexcept {
        if (connection_ != nullptr) {
            connection_->close_after_flush();
        }
    }

    [[nodiscard]] bool shutdown_write() const noexcept {
        return connection_ != nullptr && connection_->shutdown_write();
    }

    void pause_read() const noexcept {
        if (connection_ != nullptr) {
            connection_->pause_read();
        }
    }

    void resume_read() const noexcept {
        if (connection_ != nullptr) {
            connection_->resume_read();
        }
    }

    [[nodiscard]] bool set_no_delay(bool enabled) const noexcept {
        return connection_ != nullptr && connection_->set_no_delay(enabled);
    }

    [[nodiscard]] bool set_keepalive(bool enabled) const noexcept {
        return connection_ != nullptr && connection_->set_keepalive(enabled);
    }

private:
    template <typename RuntimeT> friend class detail::TcpServerShard;

    [[nodiscard]] detail::TcpHandlerBase<Runtime> *handler_for_dispatch() const noexcept {
        return connection_ == nullptr ? nullptr : connection_->handler();
    }

    detail::TcpConnection<Runtime> *connection_{nullptr};
};

namespace detail {

template <typename Runtime>
TcpConnectionHandle<Runtime> TcpConnection<Runtime>::handle() const noexcept {
    const auto index = static_cast<std::uint16_t>(Runtime::thread_index(owner_thread()));
    return TcpConnectionHandle<Runtime>(weak_state(), index, slot_, generation_, listener_id());
}

} // namespace detail

template <typename Runtime> class TcpServer {
public:
    using Thread = typename Runtime::Thread;
    using State = detail::TcpServerState<Runtime>;

    struct ListenerConfig {
        std::string name;
        TcpEndpoint endpoint;
        std::vector<Thread> threads;
        TcpListenerOptions options;
        AcceptStrategy accept_strategy{AcceptStrategy::Auto};
    };

    TcpServer() : TcpServer(TcpServerConfig{}) {}

    explicit TcpServer(TcpServerConfig config) : state_(std::make_shared<State>()) {
        state_->config = normalize_config(config);
        state_->control_thread_index = first_io_thread_index();
        init_shards();
    }

    explicit TcpServer(std::vector<Thread> threads) : TcpServer() {
        bind_threads(std::move(threads));
    }

    TcpServer(TcpServerConfig config, std::vector<Thread> threads) : TcpServer(config) {
        bind_threads(std::move(threads));
    }

    ~TcpServer() = default;

    TcpServer(const TcpServer &) = delete;
    TcpServer &operator=(const TcpServer &) = delete;
    TcpServer(TcpServer &&) noexcept = default;
    TcpServer &operator=(TcpServer &&) noexcept = default;

    TcpServer &bind_threads(std::vector<Thread> threads) {
        state_->default_threads = std::move(threads);
        return *this;
    }

    template <typename Group> TcpServer &bind_threads(Group) {
        return bind_threads(thread_list<Runtime>(Group{}));
    }

    template <typename Handler>
    [[nodiscard]] ListenerResult add_listener(ListenerConfig config, Handler handler = Handler{}) {
        static_assert(std::is_copy_constructible_v<Handler>,
                      "Tcp listener handlers must be copy constructible");
        std::unique_ptr<detail::TcpHandlerBase<Runtime>> prototype;
        try {
            prototype =
                std::make_unique<detail::TcpHandlerModel<Runtime, Handler>>(std::move(handler));
        } catch (...) {
            return ListenerResult::failure(ENOMEM);
        }
        return add_listener_impl(std::move(config), std::move(prototype));
    }

    template <typename Handler>
    [[nodiscard]] ListenerResult start_listener(ListenerConfig config,
                                                Handler handler = Handler{}) {
        return add_listener(std::move(config), std::move(handler));
    }

    [[nodiscard]] bool
    remove_listener(TcpListenerHandle listener,
                    RemoveListenerPolicy policy = RemoveListenerPolicy::StopAcceptOnly) {
        if (!listener.valid()) {
            return false;
        }
        std::vector<std::uint16_t> shards;
        if (listener.slot() >= state_->listeners.size()) {
            return false;
        }
        auto &entry = state_->listeners[listener.slot()];
        if (entry == nullptr || entry->id != listener.id ||
            entry->state == ListenerState::Removed) {
            return false;
        }
        try {
            shards = entry->active_shards;
            for (const std::uint16_t shard : entry->started_shards) {
                if (!detail::contains_shard_index(shards, shard)) {
                    shards.push_back(shard);
                }
            }
        } catch (...) {
            return false;
        }
        entry->state = ListenerState::Removed;
        entry->active_shards.clear();
        entry->starting_shards.clear();
        entry->started_shards.clear();
        entry->pending_start_shards = 0U;
        release_listener_slot(listener.slot());
        detail::schedule_remove_listener_from_shards<Runtime>(state_, listener.id, shards, policy);
        return true;
    }

    [[nodiscard]] bool start() {
        std::vector<std::uint32_t> listener_slots;
        if (state_->running) {
            return true;
        }
        try {
            listener_slots.reserve(state_->listeners.size());
        } catch (...) {
            return false;
        }
        state_->running = true;
        for (std::uint32_t i = 0; i < state_->listeners.size(); ++i) {
            if (state_->listeners[i] != nullptr &&
                (state_->listeners[i]->state == ListenerState::Configured ||
                 state_->listeners[i]->state == ListenerState::Failed)) {
                listener_slots.push_back(i);
            }
        }

        bool ok = true;
        state_->accepting_connection_tasks.store(true, std::memory_order_release);
        for (const std::uint32_t slot : listener_slots) {
            ok = start_listener_slot(slot).ok() && ok;
        }
        return ok;
    }

    [[nodiscard]] bool stop() {
        auto state = state_;
        if (state == nullptr) {
            return true;
        }
        if (!state->running) {
            return true;
        }

        std::vector<std::uint16_t> shards;
        try {
            shards.reserve(state->shards.size());
            for (std::uint16_t i = 0; i < state->shards.size(); ++i) {
                if (state->shards[i] != nullptr) {
                    shards.push_back(i);
                }
            }
        } catch (...) {
            return false;
        }

        state->running = false;
        state->accepting_connection_tasks.store(false, std::memory_order_release);
        for (auto &listener : state->listeners) {
            if (listener == nullptr || listener->state == ListenerState::Removed) {
                continue;
            }
            try {
                for (const std::uint16_t shard : listener->started_shards) {
                    if (!detail::contains_shard_index(listener->active_shards, shard)) {
                        listener->active_shards.push_back(shard);
                    }
                }
            } catch (...) {
                listener->active_shards.clear();
            }
            listener->state = ListenerState::Configured;
            listener->starting_shards.clear();
            listener->started_shards.clear();
            listener->pending_start_shards = 0U;
        }

        if (shards.empty()) {
            return true;
        }
        bool ok = true;
        for (const std::uint16_t shard_index : shards) {
            bool scheduled = false;
            try {
                scheduled = Runtime::template start_task<detail::TcpStopShardTask<Runtime>>(
                    state, shard_index, state->config.connection_close_timeout);
            } catch (...) {
                scheduled = false;
            }
            if (!scheduled) {
                ok = false;
            }
        }
        return ok;
    }

    [[nodiscard]] std::shared_ptr<State> state() const noexcept {
        return state_;
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

    [[nodiscard]] static TcpServerConfig normalize_config(TcpServerConfig config) noexcept {
        const TcpConnectionConfig defaults{};
        if (config.connection.read_buffer_size == 0U) {
            config.connection.read_buffer_size = defaults.read_buffer_size;
        }
        if (config.connection.read_budget_bytes == 0U) {
            config.connection.read_budget_bytes = defaults.read_budget_bytes;
        }
        if (config.connection.write_budget_bytes == 0U) {
            config.connection.write_budget_bytes = defaults.write_budget_bytes;
        }
        if (config.connection.output_high_watermark == 0U) {
            config.connection.output_high_watermark = defaults.output_high_watermark;
        }
        if (config.connection_close_timeout.count() < 0) {
            config.connection_close_timeout = std::chrono::milliseconds(0);
        }
        return config;
    }

    [[nodiscard]] TcpListenerOptions
    normalize_listener_options(TcpListenerOptions options) const noexcept {
        const TcpListenerOptions listener_defaults{};
        const TcpConnectionConfig &connection = state_->config.connection;
        if (options.read_buffer_size == listener_defaults.read_buffer_size) {
            options.read_buffer_size = connection.read_buffer_size;
        }
        if (options.read_budget_bytes == listener_defaults.read_budget_bytes) {
            options.read_budget_bytes = connection.read_budget_bytes;
        }
        if (options.write_budget_bytes == listener_defaults.write_budget_bytes) {
            options.write_budget_bytes = connection.write_budget_bytes;
        }
        if (options.output_high_watermark == listener_defaults.output_high_watermark) {
            options.output_high_watermark = connection.output_high_watermark;
        }
        if (options.no_delay == listener_defaults.no_delay) {
            options.no_delay = connection.no_delay;
        }
        if (options.keepalive == listener_defaults.keepalive) {
            options.keepalive = connection.keepalive;
        }
        return options;
    }

    void init_shards() {
        state_->shards.reserve(Runtime::thread_count);
        for (std::uint16_t i = 0; i < Runtime::thread_count; ++i) {
            const Thread thread = Runtime::thread_from_index(i);
            state_->shards.push_back(
                std::make_unique<detail::TcpServerShard<Runtime>>(state_, i, thread));
        }
    }

    [[nodiscard]] ListenerId acquire_listener_slot() {
        std::uint32_t slot = 0;
        if (!state_->free_listener_slots.empty()) {
            slot = state_->free_listener_slots.back();
            if (slot >= state_->listeners.size()) {
                state_->listeners.resize(static_cast<std::size_t>(slot) + 1U);
            }
            if (slot >= state_->listener_generations.size()) {
                state_->listener_generations.resize(static_cast<std::size_t>(slot) + 1U, 0U);
            }
            state_->free_listener_slots.pop_back();
        } else {
            slot = static_cast<std::uint32_t>(state_->listeners.size());
            state_->listeners.reserve(static_cast<std::size_t>(slot) + 1U);
            if (slot >= state_->listener_generations.size()) {
                state_->listener_generations.reserve(static_cast<std::size_t>(slot) + 1U);
            }
            state_->listeners.push_back(nullptr);
            if (slot >= state_->listener_generations.size()) {
                state_->listener_generations.push_back(0U);
            }
        }

        std::uint32_t next_generation = state_->listener_generations[slot] + 1U;
        if (next_generation == 0U) {
            next_generation = 1U;
        }
        state_->listener_generations[slot] = next_generation;
        return ListenerId{slot, next_generation};
    }

    void release_listener_slot(std::uint32_t slot) noexcept {
        if (slot >= state_->listeners.size()) {
            return;
        }
        state_->listeners[slot].reset();
        if (slot + 1U < state_->listeners.size() && !listener_slot_is_free(slot)) {
            try {
                state_->free_listener_slots.push_back(slot);
            } catch (...) {
            }
        }
        trim_empty_listener_tail();
    }

    [[nodiscard]] bool listener_slot_is_free(std::uint32_t slot) const noexcept {
        for (const std::uint32_t free_slot : state_->free_listener_slots) {
            if (free_slot == slot) {
                return true;
            }
        }
        return false;
    }

    void trim_empty_listener_tail() noexcept {
        while (!state_->listeners.empty() && state_->listeners.back() == nullptr) {
            const auto tail = static_cast<std::uint32_t>(state_->listeners.size() - 1U);
            erase_free_listener_slot(tail);
            state_->listeners.pop_back();
        }
    }

    void erase_free_listener_slot(std::uint32_t slot) noexcept {
        for (auto it = state_->free_listener_slots.rbegin();
             it != state_->free_listener_slots.rend(); ++it) {
            if (*it == slot) {
                state_->free_listener_slots.erase(std::next(it).base());
                return;
            }
        }
    }

    [[nodiscard]] ListenerResult
    add_listener_impl(ListenerConfig config,
                      std::unique_ptr<detail::TcpHandlerBase<Runtime>> prototype) {
        if (prototype == nullptr) {
            return ListenerResult::failure(EINVAL);
        }

        if (config.threads.empty()) {
            try {
                config.threads = state_->default_threads;
            } catch (...) {
                return ListenerResult::failure(ENOMEM);
            }
        }
        config.options = normalize_listener_options(config.options);

        const int validation_error = validate_config(config);
        if (validation_error != 0) {
            return ListenerResult::failure(validation_error);
        }

        std::unique_ptr<typename State::ListenerEntry> entry;
        try {
            entry = std::make_unique<typename State::ListenerEntry>();
            entry->name = std::move(config.name);
            entry->endpoint = std::move(config.endpoint);
            entry->options = config.options;
            entry->accept_strategy = config.accept_strategy;
            entry->threads = std::move(config.threads);
            entry->handler_prototype = std::move(prototype);
            entry->state = ListenerState::Configured;
        } catch (...) {
            return ListenerResult::failure(ENOMEM);
        }

        ListenerId id{};
        bool should_start = false;
        try {
            id = acquire_listener_slot();
            entry->id = id;
            should_start = state_->running;
            state_->listeners[id.slot] = std::move(entry);
        } catch (...) {
            if (id.valid()) {
                release_listener_slot(id.slot);
            }
            return ListenerResult::failure(ENOMEM);
        }

        if (!should_start) {
            return ListenerResult::success(TcpListenerHandle{id});
        }
        ListenerResult result = start_listener_slot(id.slot);
        if (!result.ok()) {
            if (id.slot < state_->listeners.size() && state_->listeners[id.slot] != nullptr &&
                state_->listeners[id.slot]->id == id) {
                release_listener_slot(id.slot);
            }
        }
        return result;
    }

    [[nodiscard]] int validate_config(const ListenerConfig &config) const noexcept {
        if (config.threads.empty()) {
            return EINVAL;
        }
        if (config.options.backlog <= 0 || config.options.accept_budget == 0U ||
            config.options.read_budget_bytes == 0U || config.options.read_buffer_size == 0U ||
            config.options.write_budget_bytes == 0U || config.options.output_high_watermark == 0U) {
            return EINVAL;
        }
        if (config.endpoint.family == AddressFamily::Unix &&
            config.accept_strategy != AcceptStrategy::SingleAcceptor &&
            config.threads.size() > 1U) {
            return EINVAL;
        }
        if (config.endpoint.family == AddressFamily::Unix &&
            config.accept_strategy == AcceptStrategy::ReusePortPerIoThread) {
            return EINVAL;
        }
        if (config.accept_strategy == AcceptStrategy::ReusePortPerIoThread &&
            !config.options.reuse_port) {
            return EINVAL;
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

    [[nodiscard]] std::vector<std::uint16_t>
    listener_open_shards(const typename State::ListenerEntry &entry) const {
        std::vector<std::uint16_t> shards;
        shards.reserve(entry.threads.size());
        const bool per_thread =
            entry.accept_strategy == AcceptStrategy::ReusePortPerIoThread ||
            (entry.accept_strategy == AcceptStrategy::Auto && entry.options.reuse_port);
        if (!per_thread) {
            shards.push_back(Runtime::thread_index(entry.threads.front()));
            return shards;
        }
        for (Thread thread : entry.threads) {
            shards.push_back(Runtime::thread_index(thread));
        }
        return shards;
    }

    [[nodiscard]] std::vector<std::uint16_t>
    listener_install_shards(const typename State::ListenerEntry &entry) const {
        std::vector<std::uint16_t> shards;
        shards.reserve(entry.threads.size());
        for (Thread thread : entry.threads) {
            shards.push_back(Runtime::thread_index(thread));
        }
        return shards;
    }

    [[nodiscard]] ListenerResult start_listener_slot(std::uint32_t slot) {
        ListenerId id{};
        std::string name;
        TcpEndpoint endpoint;
        TcpListenerOptions options;
        std::vector<std::uint16_t> install_shards;
        std::vector<std::uint16_t> open_shards;
        std::vector<std::unique_ptr<detail::TcpHandlerBase<Runtime>>> handlers;
        detail::TcpHandlerBase<Runtime> *handler_prototype = nullptr;
        if (slot >= state_->listeners.size() || state_->listeners[slot] == nullptr) {
            return ListenerResult::failure(EINVAL);
        }
        auto &entry = *state_->listeners[slot];
        if (entry.state == ListenerState::Active || entry.state == ListenerState::Starting) {
            return ListenerResult::success(TcpListenerHandle{entry.id});
        }
        if (!state_->running ||
            (entry.state != ListenerState::Configured && entry.state != ListenerState::Failed)) {
            return ListenerResult::failure(EINVAL);
        }
        id = entry.id;
        name = entry.name;
        endpoint = entry.endpoint;
        options = entry.options;
        handler_prototype = entry.handler_prototype.get();
        try {
            install_shards = listener_install_shards(entry);
            open_shards = listener_open_shards(entry);
        } catch (...) {
            mark_listener_failed(slot, ENOMEM);
            return ListenerResult::failure(ENOMEM);
        }
        if (install_shards.empty()) {
            mark_listener_failed(slot, EINVAL);
            return ListenerResult::failure(EINVAL);
        }

        if (handler_prototype == nullptr) {
            mark_listener_failed(slot, EINVAL);
            return ListenerResult::failure(EINVAL);
        }

        try {
            handlers.reserve(install_shards.size());
            for (std::size_t i = 0; i < install_shards.size(); ++i) {
                handlers.push_back(handler_prototype->clone());
            }
        } catch (...) {
            mark_listener_failed(slot, ENOMEM);
            return ListenerResult::failure(ENOMEM);
        }

        std::vector<std::vector<std::uint16_t>> target_shards_by_install;
        try {
            target_shards_by_install.reserve(install_shards.size());
            for (const std::uint16_t shard_index : install_shards) {
                target_shards_by_install.push_back(open_shards.size() == install_shards.size()
                                                       ? std::vector<std::uint16_t>{shard_index}
                                                       : install_shards);
            }
        } catch (...) {
            mark_listener_failed(slot, ENOMEM);
            return ListenerResult::failure(ENOMEM);
        }

        try {
            entry.state = ListenerState::Starting;
            entry.active_shards.clear();
            entry.starting_shards = install_shards;
            entry.started_shards.clear();
            entry.pending_start_shards = install_shards.size();
            entry.start_error = 0;
        } catch (...) {
            mark_listener_failed(slot, ENOMEM);
            return ListenerResult::failure(ENOMEM);
        }

        bool ok = true;
        for (std::size_t i = 0; i < install_shards.size(); ++i) {
            const std::uint16_t shard_index = install_shards[i];
            const bool open_listener = detail::contains_shard_index(open_shards, shard_index);
            bool scheduled = false;
            try {
                scheduled = Runtime::template start_task<detail::TcpAddListenerTask<Runtime>>(
                    state_, shard_index, slot, id, name, endpoint, options,
                    std::move(target_shards_by_install[i]), std::move(handlers[i]), open_listener);
            } catch (...) {
                scheduled = false;
            }
            if (!scheduled) {
                detail::handle_listener_start_result_on_control<Runtime>(state_, id, shard_index,
                                                                         EIO);
                ok = false;
                break;
            }
        }
        return ok ? ListenerResult::success(TcpListenerHandle{id}) : ListenerResult::failure(EIO);
    }

    void mark_listener_failed(std::uint32_t slot, int error) {
        if (slot < state_->listeners.size() && state_->listeners[slot] != nullptr) {
            state_->listeners[slot]->state = ListenerState::Failed;
            state_->listeners[slot]->active_shards.clear();
            state_->listeners[slot]->starting_shards.clear();
            state_->listeners[slot]->started_shards.clear();
            state_->listeners[slot]->pending_start_shards = 0U;
            state_->listeners[slot]->start_error = error == 0 ? EIO : error;
        }
    }

    std::shared_ptr<State> state_;
};

template <typename Runtime, typename Group>
[[nodiscard]] std::vector<typename Runtime::Thread> thread_list(Group) {
    std::vector<typename Runtime::Thread> result;
    result.reserve(Group::count);
    for (std::uint16_t i = 0; i < Group::count; ++i) {
        result.push_back(Group::at(i));
    }
    return result;
}

using send_result = SendResult;
using close_reason = CloseReason;
using accept_strategy = AcceptStrategy;
using listener_state = ListenerState;
using remove_listener_policy = RemoveListenerPolicy;
using tcp_listener_options = TcpListenerOptions;
using tcp_server_config = TcpServerConfig;
using listener_id = ListenerId;
using tcp_listener_handle = TcpListenerHandle;
using listener_result = ListenerResult;

template <typename Runtime> using tcp_connection_handle = TcpConnectionHandle<Runtime>;
template <typename Runtime> using tcp_connection_ref = TcpConnectionRef<Runtime>;
template <typename Runtime> using tcp_server = TcpServer<Runtime>;

} // namespace af::net
