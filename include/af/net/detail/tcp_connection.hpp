#pragma once

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "af/buffer/buffer.hpp"
#include "af/detail/net/reactor/net_io_channel.hpp"
#include "af/net/detail/tcp_handler.hpp"
#include "af/net/detail/tcp_socket_ops.hpp"
#include "af/net/detail/tcp_state.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"

#include <sys/socket.h>
#include <sys/uio.h>

namespace af::net::detail {

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

} // namespace af::net::detail
