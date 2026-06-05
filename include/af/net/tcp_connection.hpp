#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

#include "af/buffer/buffer.hpp"
#include "af/net/detail/tcp_control_tasks.hpp"
#include "af/net/detail/tcp_handler.hpp"
#include "af/net/detail/tcp_server_shard.hpp"
#include "af/net/detail/tcp_state.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"

namespace af::net {

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

} // namespace af::net
