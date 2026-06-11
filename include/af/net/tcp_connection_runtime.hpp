#pragma once

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "af/buffer/buffer.hpp"
#include "af/detail/config.hpp"
#include "af/net/detail/tcp_socket_ops.hpp"
#include "af/net/tcp_connection_handle.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"
#include "af/runtime.hpp"

#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

namespace af::net {

class tcp_connection {
public:
    tcp_connection(af::runtime &owner, af::thread_ref owner_thread, int fd, std::uint32_t slot,
                   std::uint32_t generation, tcp_endpoint local_endpoint,
                   tcp_endpoint peer_endpoint, tcp_connection_config config,
                   tcp_connection_callbacks callbacks, detail::tcp_connection_lifecycle lifecycle,
                   std::weak_ptr<detail::tcp_connection_handle_state> handle_state) noexcept
        : owner_(&owner), owner_thread_(owner_thread), fd_(fd), slot_(slot),
          generation_(generation), local_endpoint_(std::move(local_endpoint)),
          peer_endpoint_(std::move(peer_endpoint)), config_(normalize_config(config)),
          callbacks_(callbacks), lifecycle_(lifecycle), handle_state_(std::move(handle_state)) {
        source_.fd = fd_;
        source_.interests = af::reactor_readable;
        source_.owner = this;
        source_.on_event = &tcp_connection::on_reactor_event;
    }

    tcp_connection(const tcp_connection &) = delete;
    tcp_connection &operator=(const tcp_connection &) = delete;

    ~tcp_connection() {
        if (alive()) {
            AF_ASSERT(on_owner_thread() &&
                      "tcp_connection must be closed on the owner reactor thread before "
                      "destruction");
            close_without_callback();
        }
    }

    [[nodiscard]] bool start() noexcept {
        if (!on_owner_thread() || fd_ < 0) {
            return false;
        }
        if (!configure_socket()) {
            close_without_callback();
            return false;
        }
        if (!owner_->register_reactor_source(owner_thread_, &source_)) {
            close_without_callback();
            return false;
        }
        registered_ = true;
        return true;
    }

    [[nodiscard]] bool alive() const noexcept {
        return fd_ >= 0;
    }

    [[nodiscard]] tcp_connection_handle handle() const noexcept {
        return tcp_connection_handle(handle_state_, owner_thread_, slot_, generation_);
    }

    [[nodiscard]] af::thread_ref owner_thread() const noexcept {
        return owner_thread_;
    }

    [[nodiscard]] std::uint32_t slot() const noexcept {
        return slot_;
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return generation_;
    }

    [[nodiscard]] const tcp_endpoint &local_endpoint() const noexcept {
        return local_endpoint_;
    }

    [[nodiscard]] const tcp_endpoint &peer_endpoint() const noexcept {
        return peer_endpoint_;
    }

    [[nodiscard]] std::size_t queued_bytes() const noexcept {
        return queued_bytes_;
    }

    [[nodiscard]] send_result send(af::buffer buffer) noexcept {
        if (!alive()) {
            return send_result::closed;
        }
        if (buffer.empty()) {
            return send_result::accepted;
        }
        const std::size_t buffer_size = buffer.size();
        if (would_exceed_high_watermark(buffer_size)) {
            return send_result::backpressure;
        }
        try {
            output_.push_back(std::move(buffer));
        } catch (...) {
            return send_result::backpressure;
        }
        queued_bytes_ += buffer_size;
        flush_output();
        return alive() ? send_result::accepted : send_result::closed;
    }

    [[nodiscard]] send_result send(af::buffer_view view) noexcept {
        if (!alive()) {
            return send_result::closed;
        }
        if (view.empty()) {
            return send_result::accepted;
        }
        if (would_exceed_high_watermark(view.size())) {
            return send_result::backpressure;
        }
        if (output_.empty()) {
            for (;;) {
                const ssize_t n = detail::socket_send(fd_, view.data(), view.size(),
                                                      detail::send_no_signal_flags());
                if (n == static_cast<ssize_t>(view.size())) {
                    return send_result::accepted;
                }
                if (n > 0) {
                    const std::size_t written = static_cast<std::size_t>(n);
                    view = af::buffer_view(view.data() + written, view.size() - written);
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
                close(close_reason::error);
                return send_result::closed;
            }
        }
        try {
            return send(af::buffer::copy(view));
        } catch (...) {
            return send_result::backpressure;
        }
    }

    void close(close_reason reason = close_reason::local) noexcept {
        close_now(reason);
    }

    [[nodiscard]] bool close_after_flush() noexcept {
        if (!alive()) {
            return true;
        }
        close_after_flush_ = true;
        read_paused_ = true;
        flush_output();
        return !alive();
    }

    [[nodiscard]] bool pause_read() noexcept {
        if (!alive()) {
            return false;
        }
        if (read_paused_) {
            return true;
        }
        read_paused_ = true;
        update_interest();
        return alive();
    }

    [[nodiscard]] bool resume_read() noexcept {
        if (!alive()) {
            return false;
        }
        if (!read_paused_) {
            return true;
        }
        read_paused_ = false;
        update_interest();
        return alive();
    }

    [[nodiscard]] bool set_no_delay(bool enabled) noexcept {
        if (!alive() || local_endpoint_.family == address_family::unix_domain ||
            peer_endpoint_.family == address_family::unix_domain) {
            return false;
        }
        if (!detail::set_tcp_no_delay(fd_, enabled)) {
            return false;
        }
        config_.no_delay = enabled;
        return true;
    }

    [[nodiscard]] bool set_keepalive(bool enabled) noexcept {
        if (!alive() || local_endpoint_.family == address_family::unix_domain ||
            peer_endpoint_.family == address_family::unix_domain) {
            return false;
        }
        if (!detail::set_socket_keepalive(fd_, enabled)) {
            return false;
        }
        config_.keepalive = enabled;
        return true;
    }

private:
    friend class tcp_connection_ref;

    void begin_user_callback() noexcept {
        if (lifecycle_.on_callback_begin != nullptr) {
            lifecycle_.on_callback_begin(lifecycle_.owner);
        }
    }

    void end_user_callback() noexcept {
        if (lifecycle_.on_callback_end != nullptr) {
            lifecycle_.on_callback_end(lifecycle_.owner);
        }
    }

    [[nodiscard]] static tcp_connection_config
    normalize_config(tcp_connection_config config) noexcept {
        const tcp_connection_config defaults;
        if (config.read_buffer_size == 0U) {
            config.read_buffer_size = defaults.read_buffer_size;
        }
        if (config.read_budget_bytes == 0U) {
            config.read_budget_bytes = defaults.read_budget_bytes;
        }
        if (config.write_budget_bytes == 0U) {
            config.write_budget_bytes = defaults.write_budget_bytes;
        }
        if (config.output_high_watermark == 0U) {
            config.output_high_watermark = defaults.output_high_watermark;
        }
        return config;
    }

    [[nodiscard]] bool on_owner_thread() const noexcept {
        return owner_ != nullptr && af::runtime::current() == owner_ &&
               af::runtime::current_thread_index() == owner_thread_.index;
    }

    [[nodiscard]] bool configure_socket() noexcept {
        if (!detail::set_nonblocking(fd_) || !detail::set_cloexec(fd_)) {
            return false;
        }
        detail::set_no_sigpipe(fd_);
        if (local_endpoint_.family == address_family::unix_domain ||
            peer_endpoint_.family == address_family::unix_domain) {
            return true;
        }
        return detail::set_tcp_no_delay(fd_, config_.no_delay) &&
               detail::set_socket_keepalive(fd_, config_.keepalive);
    }

    [[nodiscard]] bool would_exceed_high_watermark(std::size_t size) const noexcept {
        const std::size_t high_watermark = config_.output_high_watermark;
        return queued_bytes_ >= high_watermark || size > high_watermark - queued_bytes_;
    }

    static void on_reactor_event(void *owner, af::fd_event_source &source,
                                 std::uint32_t events) noexcept {
        static_cast<void>(source);
        auto *connection = static_cast<tcp_connection *>(owner);
        if (connection == nullptr) {
            return;
        }
        connection->on_event(events);
        if (!connection->alive() && connection->lifecycle_.on_inactive != nullptr) {
            connection->lifecycle_.on_inactive(connection->lifecycle_.owner, *connection);
        }
    }

    void on_event(std::uint32_t events) noexcept {
        if ((events & af::reactor_error) != 0U) {
            close(close_reason::error);
            return;
        }
        if ((events & af::reactor_readable) != 0U) {
            read_available();
        }
        if (alive() && (events & af::reactor_writable) != 0U) {
            flush_output();
        }
        if (alive() && (events & af::reactor_hangup) != 0U) {
            close(close_reason::peer);
        }
    }

    void read_available() noexcept {
        if (read_buffer_.size() < config_.read_buffer_size) {
            try {
                read_buffer_.resize(config_.read_buffer_size);
            } catch (...) {
                close(close_reason::error);
                return;
            }
        }

        std::size_t consumed = 0;
        while (alive() && !read_paused_ && consumed < config_.read_budget_bytes) {
            const std::size_t remaining_budget = config_.read_budget_bytes - consumed;
            const std::size_t read_size = std::min(read_buffer_.size(), remaining_budget);
            const ssize_t n = ::recv(fd_, read_buffer_.data(), read_size, 0);
            if (n > 0) {
                const std::size_t size = static_cast<std::size_t>(n);
                consumed += size;
                if (callbacks_.on_read != nullptr) {
                    begin_user_callback();
                    callbacks_.on_read(callbacks_.owner, tcp_connection_ref(this),
                                       af::buffer_view(read_buffer_.data(), size));
                    end_user_callback();
                }
                continue;
            }
            if (n == 0) {
                close(close_reason::peer);
                return;
            }
            const int error = errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN || error == EWOULDBLOCK) {
                return;
            }
            close(close_reason::error);
            return;
        }
    }

    void flush_output() noexcept {
        std::size_t written_this_run = 0;
        while (alive()) {
            if (output_.empty()) {
                break;
            }
            if (written_this_run >= config_.write_budget_bytes) {
                update_interest();
                return;
            }
            const ssize_t n = flush_output_once(config_.write_budget_bytes - written_this_run);
            if (n > 0) {
                const std::size_t written = static_cast<std::size_t>(n);
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
            close(close_reason::error);
            return;
        }
        if (close_after_flush_ && output_.empty()) {
            close(close_reason::local);
            return;
        }
        update_interest();
    }

    [[nodiscard]] ssize_t flush_output_once(std::size_t max_bytes) noexcept {
        if (max_bytes == 0U) {
            return 0;
        }
        std::array<af::buffer_view, 64> views{};
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
            return detail::socket_send(fd_, iov[0].iov_base, iov[0].iov_len,
                                       detail::send_no_signal_flags());
        }

        msghdr message{};
        message.msg_iov = iov.data();
        message.msg_iovlen = static_cast<decltype(message.msg_iovlen)>(send_count);
        return detail::socket_sendmsg(fd_, &message, detail::send_no_signal_flags());
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
        std::uint32_t interests = read_paused_ ? 0U : af::reactor_readable;
        if (!output_.empty()) {
            interests |= af::reactor_writable;
        }
        if (registered_ && interests == source_.interests) {
            return;
        }
        source_.interests = interests;
        if (interests == 0U) {
            if (registered_) {
                registered_ = !owner_->unregister_reactor_source(owner_thread_, &source_);
            }
            return;
        }
        if (registered_) {
            if (!owner_->update_reactor_source(owner_thread_, &source_)) {
                close(close_reason::error);
            }
            return;
        }
        if (owner_->register_reactor_source(owner_thread_, &source_)) {
            registered_ = true;
            return;
        }
        close(close_reason::error);
    }

    void close_now(close_reason reason) noexcept {
        if (fd_ < 0) {
            return;
        }
        if (registered_) {
            static_cast<void>(owner_->unregister_reactor_source(owner_thread_, &source_));
            registered_ = false;
        }
        detail::close_fd(fd_);
        if (callbacks_.on_close != nullptr) {
            begin_user_callback();
            callbacks_.on_close(callbacks_.owner, tcp_connection_ref(this), reason);
            end_user_callback();
        }
    }

    void close_without_callback() noexcept {
        if (registered_ && on_owner_thread()) {
            static_cast<void>(owner_->unregister_reactor_source(owner_thread_, &source_));
            registered_ = false;
        }
        detail::close_fd(fd_);
    }

    af::runtime *owner_{nullptr};
    af::thread_ref owner_thread_{};
    int fd_{-1};
    std::uint32_t slot_{0};
    std::uint32_t generation_{0};
    tcp_endpoint local_endpoint_;
    tcp_endpoint peer_endpoint_;
    tcp_connection_config config_;
    tcp_connection_callbacks callbacks_{};
    detail::tcp_connection_lifecycle lifecycle_{};
    std::weak_ptr<detail::tcp_connection_handle_state> handle_state_;
    af::fd_event_source source_{};
    af::buffer_chain output_;
    std::vector<std::byte> read_buffer_;
    std::size_t queued_bytes_{0};
    bool registered_{false};
    bool close_after_flush_{false};
    bool read_paused_{false};
};

inline bool tcp_connection_ref::valid() const noexcept {
    return connection_ != nullptr && connection_->alive();
}

inline tcp_connection_handle tcp_connection_ref::handle() const noexcept {
    return connection_ == nullptr ? tcp_connection_handle{} : connection_->handle();
}

inline af::thread_ref tcp_connection_ref::owner_thread() const noexcept {
    return connection_ == nullptr ? af::thread_ref{} : connection_->owner_thread();
}

inline std::uint32_t tcp_connection_ref::slot() const noexcept {
    return connection_ == nullptr ? 0U : connection_->slot();
}

inline std::uint32_t tcp_connection_ref::generation() const noexcept {
    return connection_ == nullptr ? 0U : connection_->generation();
}

inline const tcp_endpoint &tcp_connection_ref::local_endpoint() const noexcept {
    static const tcp_endpoint empty{};
    return connection_ == nullptr ? empty : connection_->local_endpoint();
}

inline const tcp_endpoint &tcp_connection_ref::peer_endpoint() const noexcept {
    static const tcp_endpoint empty{};
    return connection_ == nullptr ? empty : connection_->peer_endpoint();
}

inline std::size_t tcp_connection_ref::queued_bytes() const noexcept {
    return connection_ == nullptr ? 0U : connection_->queued_bytes();
}

inline send_result tcp_connection_ref::send(af::buffer buffer) const noexcept {
    return connection_ == nullptr ? send_result::closed : connection_->send(std::move(buffer));
}

inline send_result tcp_connection_ref::send(af::buffer_view view) const noexcept {
    return connection_ == nullptr ? send_result::closed : connection_->send(view);
}

inline bool tcp_connection_ref::pause_read() const noexcept {
    return connection_ != nullptr && connection_->pause_read();
}

inline bool tcp_connection_ref::resume_read() const noexcept {
    return connection_ != nullptr && connection_->resume_read();
}

inline bool tcp_connection_ref::set_no_delay(bool enabled) const noexcept {
    return connection_ != nullptr && connection_->set_no_delay(enabled);
}

inline bool tcp_connection_ref::set_keepalive(bool enabled) const noexcept {
    return connection_ != nullptr && connection_->set_keepalive(enabled);
}

inline void tcp_connection_ref::close(close_reason reason) const noexcept {
    if (connection_ != nullptr) {
        connection_->close(reason);
    }
}

inline void tcp_connection_ref::close_after_flush() const noexcept {
    if (connection_ != nullptr) {
        static_cast<void>(connection_->close_after_flush());
    }
}

} // namespace af::net
