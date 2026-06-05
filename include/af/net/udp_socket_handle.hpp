#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

#include "af/buffer/buffer.hpp"
#include "af/detail/net/socket_address.hpp"
#include "af/net/detail/udp_control_tasks.hpp"
#include "af/net/detail/udp_socket_shard.hpp"
#include "af/net/detail/udp_state.hpp"
#include "af/net/udp_types.hpp"

namespace af::net {

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

template <typename Runtime> using udp_socket_handle = UdpSocketHandle<Runtime>;
template <typename Runtime> using udp_socket_ref = UdpSocketRef<Runtime>;

} // namespace af::net
