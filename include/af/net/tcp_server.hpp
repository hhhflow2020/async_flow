#pragma once

#include <atomic>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "af/async_runtime.hpp"
#include "af/buffer/buffer.hpp"
#include "af/detail/net/reactor/net_io_channel.hpp"
#include "af/detail/net/socket_address.hpp"
#include "af/detail/queue/bounded_mpsc_queue.hpp"
#include "af/net/tcp_endpoint.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/eventfd.h>
#endif

namespace af::net {

enum class SendResult : std::uint8_t {
    Accepted,
    Backpressure,
    Closed,
    Unsupported,
};

enum class CloseReason : std::uint8_t {
    Local,
    Peer,
    Error,
};

struct TcpServerOptions {
    int backlog{4096};
    bool reuse_port{true};
    bool ipv6_only{true};
    std::size_t command_queue_capacity{4096};
    std::size_t accept_budget{128};
    std::size_t read_budget_bytes{256U * 1024U};
    std::size_t read_buffer_size{16U * 1024U};
    std::size_t output_high_watermark{4U * 1024U * 1024U};
};

template <typename Runtime, typename Handler> class TcpConnectionHandle;
template <typename Runtime, typename Handler> class TcpConnectionRef;

namespace detail {

template <typename Runtime, typename Handler> class TcpConnection;
template <typename Runtime, typename Handler> class TcpServerShard;

template <typename Runtime, typename Handler> struct TcpCommand {
    enum class Kind : std::uint8_t {
        Send,
        Close,
        CloseAfterFlush,
        ShutdownWrite,
        PauseRead,
        ResumeRead,
        SetNoDelay,
        SetKeepAlive,
    };

    Kind kind{Kind::Send};
    std::uint32_t slot{0};
    std::uint32_t generation{0};
    bool flag{false};
    af::Buffer buffer;
};

template <typename Runtime, typename Handler> struct TcpServerState {
    using Thread = typename Runtime::Thread;
    using Command = TcpCommand<Runtime, Handler>;
    using Shard = TcpServerShard<Runtime, Handler>;

    TcpEndpoint endpoint;
    TcpServerOptions options;
    std::vector<Thread> threads;
    std::vector<std::unique_ptr<Shard>> shards;
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
        ::close(fd);
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

template <typename Runtime, typename Handler> class TcpConnection {
public:
    using State = TcpServerState<Runtime, Handler>;
    using Shard = TcpServerShard<Runtime, Handler>;
    using Thread = typename Runtime::Thread;

    TcpConnection(Shard *shard, int fd, std::uint32_t slot, std::uint32_t generation,
                  TcpEndpoint local_endpoint, TcpEndpoint peer_endpoint) noexcept
        : shard_(shard), fd_(fd), slot_(slot), generation_(generation),
          local_endpoint_(std::move(local_endpoint)), peer_endpoint_(std::move(peer_endpoint)) {
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

    [[nodiscard]] const TcpEndpoint &local_endpoint() const noexcept {
        return local_endpoint_;
    }

    [[nodiscard]] const TcpEndpoint &peer_endpoint() const noexcept {
        return peer_endpoint_;
    }

    [[nodiscard]] std::size_t queued_bytes() const noexcept {
        return queued_bytes_;
    }

    [[nodiscard]] TcpConnectionHandle<Runtime, Handler> handle() const noexcept;

    [[nodiscard]] SendResult send(af::Buffer buffer) noexcept {
        if (!alive() || write_shutdown_requested_ || write_shutdown_done_) {
            return SendResult::Closed;
        }
        if (queued_bytes_ + buffer.size() > shard_options().output_high_watermark) {
            return SendResult::Backpressure;
        }
        queued_bytes_ += buffer.size();
        output_.push_back(std::move(buffer));
        flush_output();
        return alive() ? SendResult::Accepted : SendResult::Closed;
    }

    [[nodiscard]] SendResult send(af::BufferView view) noexcept {
        return send(af::Buffer::copy(view));
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
        if (output_.empty()) {
            close_now(CloseReason::Local);
            return;
        }
        update_interest();
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
        return alive() && detail::set_tcp_no_delay(fd_, enabled);
    }

    [[nodiscard]] bool set_keepalive(bool enabled) noexcept {
        return alive() && detail::set_socket_keepalive(fd_, enabled);
    }

private:
    static void on_channel_event(void *owner, std::uint32_t events) noexcept {
        static_cast<TcpConnection *>(owner)->on_event(events);
    }

    [[nodiscard]] Thread owner_thread() const noexcept;
    [[nodiscard]] const TcpServerOptions &shard_options() const noexcept;
    Handler &handler() noexcept;
    std::weak_ptr<State> weak_state() const noexcept;

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
        const std::size_t buffer_size =
            shard_options().read_buffer_size == 0U ? 16U * 1024U : shard_options().read_buffer_size;
        if (read_buffer_.size() < buffer_size) {
            read_buffer_.resize(buffer_size);
        }
        std::size_t consumed = 0;
        while (alive() && consumed < shard_options().read_budget_bytes) {
            const ssize_t n = ::recv(fd_, read_buffer_.data(), read_buffer_.size(), 0);
            if (n > 0) {
                consumed += static_cast<std::size_t>(n);
                TcpConnectionRef<Runtime, Handler> ref(this);
                if constexpr (requires(Handler h, TcpConnectionRef<Runtime, Handler> c,
                                       af::BufferView v) { h.on_read(c, v); }) {
                    handler().on_read(
                        ref, af::BufferView(read_buffer_.data(), static_cast<std::size_t>(n)));
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
        while (alive() && !output_.empty()) {
            af::Buffer &front = output_.front();
            const ssize_t n = ::send(fd_, front.data(), front.size(), send_no_signal_flags());
            if (n > 0) {
                const auto written = static_cast<std::size_t>(n);
                queued_bytes_ -= written;
                front.remove_prefix(written);
                if (front.empty()) {
                    output_.pop_front();
                }
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
        if constexpr (requires(Handler h, TcpConnectionHandle<Runtime, Handler> c, CloseReason r) {
                          h.on_close(c, r);
                      }) {
            handler().on_close(handle(), reason);
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
    int fd_{-1};
    std::uint32_t slot_{0};
    std::uint32_t generation_{0};
    TcpEndpoint local_endpoint_;
    TcpEndpoint peer_endpoint_;
    af::detail::NetIoChannel channel_{};
    std::deque<af::Buffer> output_;
    std::vector<std::byte> read_buffer_;
    std::size_t queued_bytes_{0};
    bool read_paused_{false};
    bool close_after_flush_{false};
    bool write_shutdown_requested_{false};
    bool write_shutdown_done_{false};
};

template <typename Runtime, typename Handler> class TcpServerShard {
public:
    using State = TcpServerState<Runtime, Handler>;
    using Thread = typename Runtime::Thread;
    using Command = TcpCommand<Runtime, Handler>;
    using Connection = TcpConnection<Runtime, Handler>;

    TcpServerShard(std::weak_ptr<State> state, std::size_t shard_index, Thread thread,
                   Handler handler, std::size_t command_queue_capacity)
        : state_(std::move(state)), shard_index_(shard_index), thread_(thread),
          handler_(std::move(handler)), commands_(command_queue_capacity) {}

    TcpServerShard(const TcpServerShard &) = delete;
    TcpServerShard &operator=(const TcpServerShard &) = delete;

    ~TcpServerShard() {
        detail::close_fd(listener_fd_);
        detail::close_fd(wake_fd_);
        detail::close_fd(wake_write_fd_);
    }

    [[nodiscard]] Thread thread() const noexcept {
        return thread_;
    }

    [[nodiscard]] Handler &handler() noexcept {
        return handler_;
    }

    [[nodiscard]] std::weak_ptr<State> weak_state() const noexcept {
        return state_;
    }

    [[nodiscard]] const TcpServerOptions &options() const noexcept {
        return state_.lock()->options;
    }

    [[nodiscard]] bool start_on_owner() noexcept {
        auto state = state_.lock();
        if (state == nullptr) {
            return false;
        }
        if (!open_listener(*state)) {
            return false;
        }
        if (!open_wake_channel()) {
            close_listener();
            return false;
        }
        return true;
    }

    [[nodiscard]] bool enqueue(Command *command) noexcept {
        if (command == nullptr || !commands_.try_push(command)) {
            return false;
        }
        wake();
        return true;
    }

    void send_to(std::uint32_t slot, std::uint32_t generation, af::Buffer buffer) noexcept {
        Connection *connection = find(slot, generation);
        if (connection == nullptr) {
            return;
        }
        static_cast<void>(connection->send(std::move(buffer)));
    }

    void close_connection(std::uint32_t slot, std::uint32_t generation) noexcept {
        Connection *connection = find(slot, generation);
        if (connection != nullptr) {
            connection->close(CloseReason::Local);
        }
    }

    void close_connection_after_flush(std::uint32_t slot, std::uint32_t generation) noexcept {
        Connection *connection = find(slot, generation);
        if (connection != nullptr) {
            connection->close_after_flush();
        }
    }

    void shutdown_connection_write(std::uint32_t slot, std::uint32_t generation) noexcept {
        Connection *connection = find(slot, generation);
        if (connection != nullptr) {
            static_cast<void>(connection->shutdown_write());
        }
    }

    void pause_connection_read(std::uint32_t slot, std::uint32_t generation) noexcept {
        Connection *connection = find(slot, generation);
        if (connection != nullptr) {
            connection->pause_read();
        }
    }

    void resume_connection_read(std::uint32_t slot, std::uint32_t generation) noexcept {
        Connection *connection = find(slot, generation);
        if (connection != nullptr) {
            connection->resume_read();
        }
    }

    void set_connection_no_delay(std::uint32_t slot, std::uint32_t generation,
                                 bool enabled) noexcept {
        Connection *connection = find(slot, generation);
        if (connection != nullptr) {
            static_cast<void>(connection->set_no_delay(enabled));
        }
    }

    void set_connection_keepalive(std::uint32_t slot, std::uint32_t generation,
                                  bool enabled) noexcept {
        Connection *connection = find(slot, generation);
        if (connection != nullptr) {
            static_cast<void>(connection->set_keepalive(enabled));
        }
    }

    void stop_on_owner() noexcept {
        close_listener();
        for (auto &connection : connections_) {
            if (connection != nullptr && connection->alive()) {
                connection->close(CloseReason::Local);
            }
        }
        close_wake_channel();
    }

private:
    friend class TcpConnection<Runtime, Handler>;

    static void on_listener_event(void *owner, std::uint32_t events) noexcept {
        static_cast<TcpServerShard *>(owner)->handle_listener(events);
    }

    static void on_wake_event(void *owner, std::uint32_t events) noexcept {
        static_cast<TcpServerShard *>(owner)->handle_wake(events);
    }

    [[nodiscard]] bool open_listener(const State &state) noexcept {
        ::af::detail::SocketAddress bind_address{};
        int address_error = 0;
        if (!::af::detail::socket_address_from_endpoint(state.endpoint, bind_address,
                                                        address_error)) {
            static_cast<void>(address_error);
            return false;
        }

        listener_fd_ = ::socket(bind_address.family, SOCK_STREAM, 0);
        if (listener_fd_ < 0) {
            return false;
        }
        if (!set_nonblocking(listener_fd_) || !set_cloexec(listener_fd_)) {
            close_listener();
            return false;
        }

        int one = 1;
        static_cast<void>(::setsockopt(listener_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)));
#if defined(SO_REUSEPORT)
        if (state.options.reuse_port) {
            static_cast<void>(
                ::setsockopt(listener_fd_, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)));
        }
#else
        static_cast<void>(one);
#endif
        if (bind_address.family == AF_INET6) {
            const int v6_only = state.options.ipv6_only ? 1 : 0;
            static_cast<void>(
                ::setsockopt(listener_fd_, IPPROTO_IPV6, IPV6_V6ONLY, &v6_only, sizeof(v6_only)));
        }
        if (::bind(listener_fd_, reinterpret_cast<const sockaddr *>(&bind_address.storage),
                   bind_address.size) != 0) {
            close_listener();
            return false;
        }
        if (::listen(listener_fd_, state.options.backlog) != 0) {
            close_listener();
            return false;
        }

        listener_channel_.fd = listener_fd_;
        listener_channel_.owner = this;
        listener_channel_.on_event = &TcpServerShard::on_listener_event;
        if (!Runtime::net_register_channel(thread_, &listener_channel_,
                                           af::detail::net_io_readable)) {
            close_listener();
            return false;
        }
        return true;
    }

    [[nodiscard]] bool open_wake_channel() noexcept {
#if defined(__linux__)
        wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wake_fd_ < 0) {
            return false;
        }
        wake_channel_.fd = wake_fd_;
        wake_channel_.owner = this;
        wake_channel_.on_event = &TcpServerShard::on_wake_event;
        return Runtime::net_register_channel(thread_, &wake_channel_, af::detail::net_io_readable);
#else
        int fds[2]{-1, -1};
        if (::pipe(fds) != 0) {
            return false;
        }
        wake_fd_ = fds[0];
        wake_write_fd_ = fds[1];
        if (!set_nonblocking(wake_fd_) || !set_cloexec(wake_fd_) ||
            !set_nonblocking(wake_write_fd_) || !set_cloexec(wake_write_fd_)) {
            detail::close_fd(wake_fd_);
            detail::close_fd(wake_write_fd_);
            return false;
        }
        wake_channel_.fd = wake_fd_;
        wake_channel_.owner = this;
        wake_channel_.on_event = &TcpServerShard::on_wake_event;
        return Runtime::net_register_channel(thread_, &wake_channel_, af::detail::net_io_readable);
#endif
    }

    void close_listener() noexcept {
        if (listener_fd_ >= 0) {
            static_cast<void>(Runtime::net_unregister_channel(thread_, &listener_channel_));
        }
        detail::close_fd(listener_fd_);
    }

    void close_wake_channel() noexcept {
        if (wake_fd_ >= 0) {
            static_cast<void>(Runtime::net_unregister_channel(thread_, &wake_channel_));
        }
        detail::close_fd(wake_fd_);
        detail::close_fd(wake_write_fd_);
        wake_pending_.store(false, std::memory_order_release);
    }

    void handle_listener(std::uint32_t events) noexcept {
        if ((events & (af::detail::net_io_error | af::detail::net_io_hangup)) != 0U) {
            close_listener();
            return;
        }
        std::size_t accepted = 0;
        while (accepted < options().accept_budget) {
            sockaddr_storage peer{};
            socklen_t peer_size = sizeof(peer);
            const int fd =
                accept_nonblocking(listener_fd_, reinterpret_cast<sockaddr *>(&peer), &peer_size);
            if (fd >= 0) {
                set_no_sigpipe(fd);
                static_cast<void>(set_tcp_no_delay(fd, true));
                create_connection(fd, reinterpret_cast<const sockaddr *>(&peer), peer_size);
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
            return;
        }
    }

    void handle_wake(std::uint32_t events) noexcept {
        static_cast<void>(events);
        drain_wake_fd();
        wake_pending_.store(false, std::memory_order_release);
        drain_commands();
    }

    void drain_wake_fd() noexcept {
#if defined(__linux__)
        std::uint64_t value = 0;
        while (::read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {
        }
#else
        std::array<std::byte, 256> buffer{};
        while (::read(wake_fd_, buffer.data(), buffer.size()) > 0) {
        }
#endif
    }

    void drain_commands() noexcept {
        std::array<Command *, 64> batch{};
        for (;;) {
            const std::size_t count = commands_.try_pop_many(batch.data(), batch.size());
            if (count == 0U) {
                return;
            }
            for (std::size_t i = 0; i < count; ++i) {
                std::unique_ptr<Command> command(batch[i]);
                switch (command->kind) {
                case Command::Kind::Send:
                    send_to(command->slot, command->generation, std::move(command->buffer));
                    break;
                case Command::Kind::Close:
                    close_connection(command->slot, command->generation);
                    break;
                case Command::Kind::CloseAfterFlush:
                    close_connection_after_flush(command->slot, command->generation);
                    break;
                case Command::Kind::ShutdownWrite:
                    shutdown_connection_write(command->slot, command->generation);
                    break;
                case Command::Kind::PauseRead:
                    pause_connection_read(command->slot, command->generation);
                    break;
                case Command::Kind::ResumeRead:
                    resume_connection_read(command->slot, command->generation);
                    break;
                case Command::Kind::SetNoDelay:
                    set_connection_no_delay(command->slot, command->generation, command->flag);
                    break;
                case Command::Kind::SetKeepAlive:
                    set_connection_keepalive(command->slot, command->generation, command->flag);
                    break;
                }
            }
        }
    }

    void wake() noexcept {
#if defined(__linux__)
        if (wake_fd_ < 0) {
            return;
        }
        bool expected = false;
        if (!wake_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
            return;
        }
        const std::uint64_t value = 1;
        if (::write(wake_fd_, &value, sizeof(value)) != static_cast<ssize_t>(sizeof(value))) {
            wake_pending_.store(false, std::memory_order_release);
        }
#else
        if (wake_write_fd_ < 0) {
            return;
        }
        bool expected = false;
        if (!wake_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
            return;
        }
        const std::byte value{1};
        if (::write(wake_write_fd_, &value, 1) != 1) {
            wake_pending_.store(false, std::memory_order_release);
        }
#endif
    }

    void create_connection(int fd, const sockaddr *peer, socklen_t peer_size) noexcept {
        const std::uint32_t slot = static_cast<std::uint32_t>(connections_.size());
        generations_.push_back(1U);
        sockaddr_storage local{};
        socklen_t local_size = sizeof(local);
        TcpEndpoint local_endpoint{};
        if (::getsockname(fd, reinterpret_cast<sockaddr *>(&local), &local_size) == 0) {
            local_endpoint = ::af::detail::endpoint_from_socket_address(
                reinterpret_cast<const sockaddr *>(&local), local_size);
        }
        TcpEndpoint peer_endpoint = ::af::detail::endpoint_from_socket_address(peer, peer_size);
        auto connection =
            std::make_unique<Connection>(this, fd, slot, generations_.back(),
                                         std::move(local_endpoint), std::move(peer_endpoint));
        Connection *connection_ptr = connection.get();
        connections_.push_back(std::move(connection));
        if (!connection_ptr->start()) {
            connection_ptr->close(CloseReason::Error);
            return;
        }
        if constexpr (requires(Handler h, TcpConnectionRef<Runtime, Handler> c) {
                          h.on_accept(c);
                      }) {
            handler_.on_accept(TcpConnectionRef<Runtime, Handler>(connection_ptr));
        }
    }

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

    std::weak_ptr<State> state_;
    std::size_t shard_index_{0};
    Thread thread_;
    Handler handler_;
    af::detail::BoundedMpscQueue<Command> commands_;
    int listener_fd_{-1};
    int wake_fd_{-1};
    int wake_write_fd_{-1};
    af::detail::NetIoChannel listener_channel_{};
    af::detail::NetIoChannel wake_channel_{};
    std::atomic<bool> wake_pending_{false};
    std::vector<std::unique_ptr<Connection>> connections_;
    std::vector<std::uint32_t> generations_;
};

template <typename Runtime, typename Handler>
typename Runtime::Thread TcpConnection<Runtime, Handler>::owner_thread() const noexcept {
    return shard_->thread();
}

template <typename Runtime, typename Handler>
const TcpServerOptions &TcpConnection<Runtime, Handler>::shard_options() const noexcept {
    return shard_->options();
}

template <typename Runtime, typename Handler>
Handler &TcpConnection<Runtime, Handler>::handler() noexcept {
    return shard_->handler();
}

template <typename Runtime, typename Handler>
std::weak_ptr<TcpServerState<Runtime, Handler>>
TcpConnection<Runtime, Handler>::weak_state() const noexcept {
    return shard_->weak_state();
}

} // namespace detail

template <typename Runtime, typename Handler> class TcpConnectionHandle {
public:
    using State = detail::TcpServerState<Runtime, Handler>;
    using Command = detail::TcpCommand<Runtime, Handler>;

    TcpConnectionHandle() = default;

    TcpConnectionHandle(std::weak_ptr<State> state, std::size_t shard_index, std::uint32_t slot,
                        std::uint32_t generation) noexcept
        : state_(std::move(state)), shard_index_(shard_index), slot_(slot),
          generation_(generation) {}

    [[nodiscard]] std::uint32_t slot() const noexcept {
        return slot_;
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return generation_;
    }

    [[nodiscard]] SendResult send(af::Buffer buffer) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size()) {
            return SendResult::Closed;
        }
        auto command = std::make_unique<Command>();
        command->kind = Command::Kind::Send;
        command->slot = slot_;
        command->generation = generation_;
        command->buffer = std::move(buffer);
        if (!state->shards[shard_index_]->enqueue(command.get())) {
            return SendResult::Backpressure;
        }
        static_cast<void>(command.release());
        return SendResult::Accepted;
    }

    [[nodiscard]] SendResult send(af::BufferView view) const {
        return send(af::Buffer::copy(view));
    }

    [[nodiscard]] bool close() const {
        return enqueue_command(Command::Kind::Close);
    }

    [[nodiscard]] bool close_after_flush() const {
        return enqueue_command(Command::Kind::CloseAfterFlush);
    }

    [[nodiscard]] bool shutdown_write() const {
        return enqueue_command(Command::Kind::ShutdownWrite);
    }

    [[nodiscard]] bool pause_read() const {
        return enqueue_command(Command::Kind::PauseRead);
    }

    [[nodiscard]] bool resume_read() const {
        return enqueue_command(Command::Kind::ResumeRead);
    }

    [[nodiscard]] bool set_no_delay(bool enabled) const {
        return enqueue_command(Command::Kind::SetNoDelay, enabled);
    }

    [[nodiscard]] bool set_keepalive(bool enabled) const {
        return enqueue_command(Command::Kind::SetKeepAlive, enabled);
    }

    [[nodiscard]] friend bool operator==(TcpConnectionHandle lhs,
                                         TcpConnectionHandle rhs) noexcept {
        return lhs.shard_index_ == rhs.shard_index_ && lhs.slot_ == rhs.slot_ &&
               lhs.generation_ == rhs.generation_;
    }

private:
    [[nodiscard]] bool enqueue_command(typename Command::Kind kind, bool flag = false) const {
        auto state = state_.lock();
        if (state == nullptr || shard_index_ >= state->shards.size()) {
            return false;
        }
        auto command = std::make_unique<Command>();
        command->kind = kind;
        command->slot = slot_;
        command->generation = generation_;
        command->flag = flag;
        if (!state->shards[shard_index_]->enqueue(command.get())) {
            return false;
        }
        static_cast<void>(command.release());
        return true;
    }

    std::weak_ptr<State> state_;
    std::size_t shard_index_{0};
    std::uint32_t slot_{0};
    std::uint32_t generation_{0};
};

template <typename Runtime, typename Handler> class TcpConnectionRef {
public:
    explicit TcpConnectionRef(
        detail::TcpConnection<Runtime, Handler> *connection = nullptr) noexcept
        : connection_(connection) {}

    [[nodiscard]] bool valid() const noexcept {
        return connection_ != nullptr && connection_->alive();
    }

    [[nodiscard]] TcpConnectionHandle<Runtime, Handler> handle() const noexcept {
        return connection_->handle();
    }

    [[nodiscard]] std::uint32_t slot() const noexcept {
        return connection_ == nullptr ? 0U : connection_->slot();
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return connection_ == nullptr ? 0U : connection_->generation();
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
    detail::TcpConnection<Runtime, Handler> *connection_{nullptr};
};

namespace detail {

template <typename Runtime, typename Handler>
TcpConnectionHandle<Runtime, Handler> TcpConnection<Runtime, Handler>::handle() const noexcept {
    return TcpConnectionHandle<Runtime, Handler>(weak_state(), shard_->shard_index_, slot_,
                                                 generation_);
}

template <typename Runtime, typename Handler>
class TcpServerStartTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime, Handler>;

public:
    explicit TcpServerStartTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::size_t shard_index) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        if (state_ == nullptr || shard_index_ >= state_->threads.size()) {
            return false;
        }
        return this->schedule(state_->threads[shard_index_]);
    }

private:
    af::TaskResult run() override {
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            return this->failed();
        }
        return state_->shards[shard_index_]->start_on_owner() ? this->done() : this->failed();
    }

    std::shared_ptr<State> state_;
    std::size_t shard_index_{0};
};

template <typename Runtime, typename Handler> class TcpServerStopTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime, Handler>;

public:
    explicit TcpServerStopTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::size_t shard_index) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        if (state_ == nullptr || shard_index_ >= state_->threads.size()) {
            return false;
        }
        return this->schedule(state_->threads[shard_index_]);
    }

private:
    af::TaskResult run() override {
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            return this->failed();
        }
        state_->shards[shard_index_]->stop_on_owner();
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::size_t shard_index_{0};
};

} // namespace detail

template <typename Runtime, typename Handler> class TcpServer {
public:
    using Thread = typename Runtime::Thread;
    using State = detail::TcpServerState<Runtime, Handler>;

    struct Config {
        std::vector<Thread> threads;
        TcpEndpoint endpoint;
        TcpServerOptions options;
    };

    explicit TcpServer(Config config, Handler handler = Handler{})
        : state_(std::make_shared<State>()) {
        state_->endpoint = std::move(config.endpoint);
        state_->options = config.options;
        state_->threads = std::move(config.threads);
        state_->shards.reserve(state_->threads.size());
        for (std::size_t i = 0; i < state_->threads.size(); ++i) {
            state_->shards.push_back(std::make_unique<detail::TcpServerShard<Runtime, Handler>>(
                state_, i, state_->threads[i], handler, state_->options.command_queue_capacity));
        }
    }

    ~TcpServer() {
        static_cast<void>(stop());
    }

    TcpServer(const TcpServer &) = delete;
    TcpServer &operator=(const TcpServer &) = delete;
    TcpServer(TcpServer &&) noexcept = default;
    TcpServer &operator=(TcpServer &&) noexcept = default;

    [[nodiscard]] bool start() {
        if (state_ == nullptr || state_->threads.empty()) {
            return false;
        }
        bool ok = true;
        for (std::size_t i = 0; i < state_->threads.size(); ++i) {
            ok = Runtime::template start_task<detail::TcpServerStartTask<Runtime, Handler>>(state_,
                                                                                            i) &&
                 ok;
        }
        return ok;
    }

    [[nodiscard]] bool stop() {
        auto state = state_;
        if (state == nullptr) {
            return true;
        }
        bool ok = true;
        for (std::size_t i = 0; i < state->threads.size(); ++i) {
            ok = Runtime::template start_task<detail::TcpServerStopTask<Runtime, Handler>>(state,
                                                                                           i) &&
                 ok;
        }
        if (ok) {
            state_.reset();
        }
        return ok;
    }

    [[nodiscard]] std::shared_ptr<State> state() const noexcept {
        return state_;
    }

private:
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

} // namespace af::net
