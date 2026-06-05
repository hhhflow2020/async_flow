#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <utility>

#include "af/detail/net/socket_address.hpp"
#include "af/net/detail/tcp_socket_ops.hpp"
#include "af/net/tcp_endpoint.hpp"

namespace af::net::detail {

template <typename Runtime>
TcpServerShard<Runtime>::TcpServerShard(std::weak_ptr<State> state, std::uint16_t shard_index,
                                        Thread thread)
    : state_(std::move(state)), shard_index_(shard_index), thread_(thread) {}

template <typename Runtime>
typename TcpServerShard<Runtime>::Thread TcpServerShard<Runtime>::thread() const noexcept {
    return thread_;
}

template <typename Runtime>
std::weak_ptr<typename TcpServerShard<Runtime>::State>
TcpServerShard<Runtime>::weak_state() const noexcept {
    return state_;
}

template <typename Runtime>
SendResult TcpServerShard<Runtime>::send_to(std::uint32_t slot, std::uint32_t generation,
                                            af::Buffer buffer) noexcept {
    Connection *connection = find(slot, generation);
    if (connection == nullptr) {
        return SendResult::Closed;
    }
    const SendResult result = connection->send(std::move(buffer));
    reap_retired_connections_if_safe();
    return result;
}

template <typename Runtime>
SendResult TcpServerShard<Runtime>::send_to(std::uint32_t slot, std::uint32_t generation,
                                            af::BufferView view) noexcept {
    Connection *connection = find(slot, generation);
    if (connection == nullptr) {
        return SendResult::Closed;
    }
    const SendResult result = connection->send(view);
    reap_retired_connections_if_safe();
    return result;
}

template <typename Runtime>
bool TcpServerShard<Runtime>::close_connection(std::uint32_t slot,
                                               std::uint32_t generation) noexcept {
    Connection *connection = find(slot, generation);
    if (connection == nullptr) {
        return false;
    }
    connection->close(CloseReason::Local);
    reap_retired_connections_if_safe();
    return true;
}

template <typename Runtime>
bool TcpServerShard<Runtime>::close_connection_after_flush(std::uint32_t slot,
                                                           std::uint32_t generation) noexcept {
    Connection *connection = find(slot, generation);
    if (connection == nullptr) {
        return false;
    }
    connection->close_after_flush();
    reap_retired_connections_if_safe();
    return true;
}

template <typename Runtime>
bool TcpServerShard<Runtime>::shutdown_connection_write(std::uint32_t slot,
                                                        std::uint32_t generation) noexcept {
    Connection *connection = find(slot, generation);
    if (connection == nullptr) {
        return false;
    }
    return connection->shutdown_write();
}

template <typename Runtime>
bool TcpServerShard<Runtime>::pause_connection_read(std::uint32_t slot,
                                                    std::uint32_t generation) noexcept {
    Connection *connection = find(slot, generation);
    if (connection == nullptr) {
        return false;
    }
    connection->pause_read();
    return true;
}

template <typename Runtime>
bool TcpServerShard<Runtime>::resume_connection_read(std::uint32_t slot,
                                                     std::uint32_t generation) noexcept {
    Connection *connection = find(slot, generation);
    if (connection == nullptr) {
        return false;
    }
    connection->resume_read();
    return true;
}

template <typename Runtime>
bool TcpServerShard<Runtime>::set_connection_no_delay(std::uint32_t slot, std::uint32_t generation,
                                                      bool enabled) noexcept {
    Connection *connection = find(slot, generation);
    if (connection == nullptr) {
        return false;
    }
    return connection->set_no_delay(enabled);
}

template <typename Runtime>
bool TcpServerShard<Runtime>::set_connection_keepalive(std::uint32_t slot, std::uint32_t generation,
                                                       bool enabled) noexcept {
    Connection *connection = find(slot, generation);
    if (connection == nullptr) {
        return false;
    }
    return connection->set_keepalive(enabled);
}

template <typename Runtime>
int TcpServerShard<Runtime>::add_listener_on_owner(std::uint32_t listener_slot,
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

template <typename Runtime>
void TcpServerShard<Runtime>::remove_listener_on_owner(ListenerId id,
                                                       RemoveListenerPolicy policy) noexcept {
    if (id.slot < listeners_.size()) {
        auto &listener = listeners_[id.slot];
        if (listener != nullptr && listener->id() == id) {
            listener->close();
            listener.reset();
        }
    }
    if (policy == RemoveListenerPolicy::CloseExistingConnections) {
        for (auto &connection : connections_) {
            if (connection != nullptr && connection->alive() && connection->listener_id() == id) {
                connection->close(CloseReason::Local);
            }
        }
        reap_retired_connections();
    }
}

template <typename Runtime> void TcpServerShard<Runtime>::close_listeners_on_owner() noexcept {
    for (auto &listener : listeners_) {
        if (listener != nullptr) {
            listener->close();
        }
    }
    listeners_.clear();
}

template <typename Runtime>
std::size_t TcpServerShard<Runtime>::close_connections_after_flush_on_owner() noexcept {
    for (auto &connection : connections_) {
        if (connection != nullptr && connection->alive()) {
            connection->close_after_flush();
        }
    }
    reap_retired_connections();
    return alive_connection_count();
}

template <typename Runtime>
void TcpServerShard<Runtime>::force_close_connections_on_owner() noexcept {
    for (auto &connection : connections_) {
        if (connection != nullptr && connection->alive()) {
            connection->close(CloseReason::Local);
        }
    }
    reap_retired_connections();
}

template <typename Runtime> void TcpServerShard<Runtime>::stop_on_owner() noexcept {
    close_listeners_on_owner();
    force_close_connections_on_owner();
}

template <typename Runtime>
bool TcpServerShard<Runtime>::create_connection(std::shared_ptr<ListenerContext> context, int fd,
                                                const sockaddr *peer,
                                                socklen_t peer_size) noexcept {
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

template <typename Runtime>
bool TcpServerShard<Runtime>::route_connection(std::shared_ptr<ListenerContext> context, int fd,
                                               const sockaddr *peer, socklen_t peer_size) noexcept {
    if (context == nullptr || fd < 0 || peer == nullptr || peer_size > sizeof(sockaddr_storage)) {
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

template <typename Runtime>
typename TcpServerShard<Runtime>::Connection *
TcpServerShard<Runtime>::find(std::uint32_t slot, std::uint32_t generation) noexcept {
    if (slot >= connections_.size()) {
        return nullptr;
    }
    Connection *connection = connections_[slot].get();
    if (connection == nullptr || !connection->alive() || connection->generation() != generation) {
        return nullptr;
    }
    return connection;
}

template <typename Runtime>
bool TcpServerShard<Runtime>::try_acquire_connection_slot(ConnectionSlot &slot) noexcept {
    try {
        slot = acquire_connection_slot();
        return true;
    } catch (...) {
        return false;
    }
}

template <typename Runtime>
typename TcpServerShard<Runtime>::ConnectionSlot
TcpServerShard<Runtime>::acquire_connection_slot() {
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

template <typename Runtime>
void TcpServerShard<Runtime>::release_unused_connection_slot(ConnectionSlot slot) noexcept {
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

template <typename Runtime>
void TcpServerShard<Runtime>::retire_connection(std::uint32_t slot,
                                                std::uint32_t generation) noexcept {
    try {
        retired_connection_slots_.push_back(ConnectionSlot{slot, generation});
    } catch (...) {
    }
}

template <typename Runtime> void TcpServerShard<Runtime>::reap_retired_connections() noexcept {
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

template <typename Runtime>
void TcpServerShard<Runtime>::reap_retired_connections_if_safe() noexcept {
    if (user_callback_depth_ == 0U) {
        reap_retired_connections();
    }
}

template <typename Runtime>
std::size_t TcpServerShard<Runtime>::alive_connection_count() const noexcept {
    std::size_t count = 0;
    for (const auto &connection : connections_) {
        if (connection != nullptr && connection->alive()) {
            ++count;
        }
    }
    return count;
}

template <typename Runtime> void TcpServerShard<Runtime>::begin_user_callback() noexcept {
    ++user_callback_depth_;
}

template <typename Runtime> void TcpServerShard<Runtime>::end_user_callback() noexcept {
    if (user_callback_depth_ > 0U) {
        --user_callback_depth_;
    }
}

template <typename Runtime> void TcpServerShard<Runtime>::trim_empty_tail_slots() noexcept {
    while (!connections_.empty() && connections_.back() == nullptr) {
        const auto tail = static_cast<std::uint32_t>(connections_.size() - 1U);
        erase_free_connection_slot(tail);
        connections_.pop_back();
    }
}

template <typename Runtime>
void TcpServerShard<Runtime>::erase_free_connection_slot(std::uint32_t slot) noexcept {
    for (auto it = free_connection_slots_.rbegin(); it != free_connection_slots_.rend(); ++it) {
        if (*it == slot) {
            free_connection_slots_.erase(std::next(it).base());
            return;
        }
    }
}

template <typename Runtime>
std::shared_ptr<typename TcpServerShard<Runtime>::ListenerContext>
TcpServerShard<Runtime>::find_listener_context(ListenerId id) noexcept {
    if (id.slot >= listeners_.size()) {
        return nullptr;
    }
    const auto &listener = listeners_[id.slot];
    if (listener == nullptr || listener->id() != id) {
        return nullptr;
    }
    return listener->context();
}

template <typename Runtime>
bool TcpServerShard<Runtime>::schedule_adopt_connection(std::uint16_t target_shard,
                                                        ListenerId listener_id, int fd,
                                                        const sockaddr *peer,
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

template <typename Runtime>
void TcpServerShard<Runtime>::adopt_connection(ListenerId listener_id, int fd, const sockaddr *peer,
                                               socklen_t peer_size) noexcept {
    auto context = find_listener_context(listener_id);
    if (context == nullptr || fd < 0 || peer == nullptr || peer_size > sizeof(sockaddr_storage)) {
        detail::close_fd(fd);
        return;
    }
    static_cast<void>(create_connection(std::move(context), fd, peer, peer_size));
}

template <typename Runtime>
void TcpServerShard<Runtime>::notify_listener_error(const std::shared_ptr<ListenerContext> &context,
                                                    int error) noexcept {
    if (context != nullptr && context->handler != nullptr) {
        context->handler->on_listener_error(TcpListenerHandle{context->id}, error);
    }
}

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

} // namespace af::net::detail
