#pragma once

#include <cerrno>
#include <utility>

#include "af/net/detail/udp_socket_shard.hpp"

namespace af::net {

inline udp_socket::udp_socket(af::runtime &owner)
    : state_(std::make_shared<detail::runtime_udp_state>(owner)) {
    state_->shards.reserve(owner.thread_count());
    for (af::runtime::thread_index i = 0; i < owner.thread_count(); ++i) {
        state_->shards.push_back(
            std::make_shared<detail::runtime_udp_shard>(state_, af::thread_ref(i)));
    }
}

inline udp_socket::~udp_socket() {
    if (state_ != nullptr) {
        state_->accepting_operations.store(false, std::memory_order_release);
    }
    if (running_) {
        AF_ASSERT(on_runtime_io_thread() &&
                  "udp_socket must be stopped on a runtime IO thread before destruction");
        if (on_runtime_io_thread()) {
            static_cast<void>(stop());
        }
    }
}

inline bool udp_socket::start(udp_socket_config config, udp_socket_callbacks callbacks) noexcept {
    if (running_ || !normalize_config(config)) {
        return false;
    }
    if (validate_config(config) != 0) {
        return false;
    }

    running_ = true;
    state_->accepting_operations.store(true, std::memory_order_release);

    bool ok = true;
    for (const af::thread_ref thread : config.threads) {
        auto shard = shard_for_thread(thread);
        if (shard == nullptr) {
            ok = false;
            continue;
        }
        if (af::runtime::current() == state_->owner &&
            af::runtime::current_thread_index() == thread.index) {
            ok = start_shard_on_owner(*shard, config, callbacks) && ok;
            continue;
        }
        try {
            const bool posted = state_->owner->post(thread, [shard, config, callbacks]() mutable {
                if (shard == nullptr || shard->state == nullptr ||
                    !shard->state->accepting_operations.load(std::memory_order_acquire)) {
                    return;
                }
                static_cast<void>(detail::runtime_udp_start_shard_on_owner(shard->state, *shard,
                                                                           config, callbacks));
            });
            if (!posted) {
                ok = false;
            }
        } catch (...) {
            ok = false;
        }
    }

    if (!ok) {
        static_cast<void>(stop());
    }
    return ok;
}

inline bool udp_socket::stop() noexcept {
    if (!running_) {
        return false;
    }
    if (state_ == nullptr || state_->owner == nullptr || af::runtime::current() != state_->owner ||
        !state_->owner->valid_thread(af::runtime::current_thread_index()) ||
        state_->owner->thread_kind_of(af::runtime::current_thread_index()) != af::thread_kind::io) {
        return false;
    }

    state_->accepting_operations.store(false, std::memory_order_release);
    running_ = false;
    bool ok = true;
    for (const auto &shard : state_->shards) {
        if (shard == nullptr || !shard->active()) {
            continue;
        }
        if (af::runtime::current_thread_index() == shard->owner_thread.index) {
            stop_shard_on_owner(*shard);
            continue;
        }
        try {
            const bool posted = state_->owner->post(shard->owner_thread, [shard]() mutable {
                if (shard != nullptr) {
                    detail::runtime_udp_stop_shard_on_owner(shard->state, *shard);
                }
            });
            ok = posted && ok;
        } catch (...) {
            ok = false;
        }
    }
    return ok;
}

inline bool udp_socket::on_runtime_io_thread() const noexcept {
    if (state_ == nullptr || state_->owner == nullptr || af::runtime::current() != state_->owner) {
        return false;
    }
    const af::runtime::thread_index index = af::runtime::current_thread_index();
    return state_->owner->valid_thread(index) &&
           state_->owner->thread_kind_of(index) == af::thread_kind::io;
}

inline bool udp_socket::normalize_config(udp_socket_config &config) const {
    if (!on_runtime_io_thread()) {
        return false;
    }
    if (config.threads.empty()) {
        const af::thread_group_ref io_threads = state_->owner->io_threads();
        try {
            config.threads.reserve(io_threads.size());
            for (std::uint16_t thread : io_threads) {
                config.threads.push_back(af::thread_ref(thread));
            }
        } catch (...) {
            return false;
        }
    }
    return true;
}

inline int udp_socket::validate_config(const udp_socket_config &config) const noexcept {
    if (state_ == nullptr || state_->owner == nullptr || config.threads.empty()) {
        return EINVAL;
    }
    if (config.options.read_budget_datagrams == 0U || config.options.receive_buffer_size == 0U ||
        config.options.max_datagram_size == 0U) {
        return EINVAL;
    }

    af::detail::SocketAddress local{};
    int address_error = 0;
    if (!af::detail::socket_address_from_endpoint(config.local_endpoint, local, address_error)) {
        return address_error == 0 ? EINVAL : address_error;
    }
    if (local.family == AF_UNIX && config.threads.size() > 1U) {
        return EINVAL;
    }
    if (config.threads.size() > 1U && !config.options.reuse_port) {
        return EINVAL;
    }
    if (config.connect_remote) {
        af::detail::SocketAddress remote{};
        if (!af::detail::socket_address_from_endpoint(config.remote_endpoint, remote,
                                                      address_error)) {
            return address_error == 0 ? EINVAL : address_error;
        }
        if (remote.family != local.family ||
            (remote.family != AF_UNIX && config.remote_endpoint.port == 0U)) {
            return EINVAL;
        }
    }
    for (std::size_t i = 0; i < config.threads.size(); ++i) {
        const af::thread_ref thread = config.threads[i];
        if (!thread || !state_->owner->valid_thread(thread) ||
            state_->owner->thread_kind_of(thread) != af::thread_kind::io) {
            return EINVAL;
        }
        for (std::size_t j = i + 1U; j < config.threads.size(); ++j) {
            if (config.threads[j] == thread) {
                return EINVAL;
            }
        }
    }
    return 0;
}

inline bool udp_socket::start_shard_on_owner(detail::runtime_udp_shard &shard,
                                             const udp_socket_config &config,
                                             udp_socket_callbacks callbacks) noexcept {
    return detail::runtime_udp_start_shard_on_owner(state_, shard, config, callbacks);
}

inline void udp_socket::stop_shard_on_owner(detail::runtime_udp_shard &shard) noexcept {
    detail::runtime_udp_stop_shard_on_owner(state_, shard);
}

inline std::shared_ptr<detail::runtime_udp_shard>
udp_socket::shard_for_thread(af::thread_ref thread) const noexcept {
    if (state_ == nullptr || !thread || thread.index >= state_->shards.size()) {
        return {};
    }
    return state_->shards[thread.index];
}

inline udp_send_result udp_socket::send_on_owner(af::thread_ref thread, std::uint32_t generation,
                                                 af::Buffer buffer) noexcept {
    auto shard = shard_for_thread(thread);
    if (shard == nullptr || !shard->matches_generation(generation)) {
        return udp_send_result::closed;
    }
    return shard->send(std::move(buffer));
}

inline udp_send_result udp_socket::send_on_owner(af::thread_ref thread, std::uint32_t generation,
                                                 af::BufferView view) noexcept {
    auto shard = shard_for_thread(thread);
    if (shard == nullptr || !shard->matches_generation(generation)) {
        return udp_send_result::closed;
    }
    return shard->send(view);
}

inline udp_send_result
udp_socket::send_to_on_owner(af::thread_ref thread, std::uint32_t generation, af::Buffer buffer,
                             const af::detail::SocketAddress &address) noexcept {
    auto shard = shard_for_thread(thread);
    if (shard == nullptr || !shard->matches_generation(generation)) {
        return udp_send_result::closed;
    }
    return shard->send_to(std::move(buffer), address);
}

inline udp_send_result
udp_socket::send_to_on_owner(af::thread_ref thread, std::uint32_t generation, af::BufferView view,
                             const af::detail::SocketAddress &address) noexcept {
    auto shard = shard_for_thread(thread);
    if (shard == nullptr || !shard->matches_generation(generation)) {
        return udp_send_result::closed;
    }
    return shard->send_to(view, address);
}

inline std::size_t udp_socket::active_shard_count() const noexcept {
    if (state_ == nullptr) {
        return 0;
    }
    std::size_t count = 0;
    for (const auto &shard : state_->shards) {
        if (shard != nullptr && shard->active()) {
            ++count;
        }
    }
    return count;
}

inline udp_socket_handle udp_socket::handle() const noexcept {
    if (state_ == nullptr || state_->shards.empty()) {
        return {};
    }
    static thread_local std::uint32_t next_handle_slot = 0;
    const std::size_t count = state_->shards.size();
    const std::size_t start = next_handle_slot++ % count;
    for (std::size_t i = 0; i < count; ++i) {
        const std::size_t index = (start + i) % count;
        const auto &shard = state_->shards[index];
        if (shard != nullptr && shard->active()) {
            return shard->handle();
        }
    }
    return {};
}

inline udp_socket_handle udp_socket::handle_for_thread(af::thread_ref thread) const noexcept {
    auto shard = shard_for_thread(thread);
    return shard == nullptr || !shard->active() ? udp_socket_handle{} : shard->handle();
}

inline std::vector<udp_socket_handle> udp_socket::handles() const {
    std::vector<udp_socket_handle> result;
    if (state_ == nullptr) {
        return result;
    }
    try {
        result.reserve(active_shard_count());
        for (const auto &shard : state_->shards) {
            if (shard != nullptr && shard->active()) {
                result.push_back(shard->handle());
            }
        }
    } catch (...) {
        result.clear();
    }
    return result;
}

inline const udp_endpoint *udp_socket::local_endpoint(af::thread_ref thread) const noexcept {
    auto shard = shard_for_thread(thread);
    if (shard == nullptr || !shard->active()) {
        return nullptr;
    }
    return &shard->local_endpoint;
}

inline std::shared_ptr<detail::runtime_udp_shard> udp_socket_handle::lock_shard(
    const std::shared_ptr<detail::runtime_udp_state> &state) const noexcept {
    if (state == nullptr || !owner_thread_ || owner_thread_.index >= state->shards.size()) {
        return {};
    }
    auto shard = state->shards[owner_thread_.index];
    if (shard == nullptr || !shard->matches_generation(generation_)) {
        return {};
    }
    return shard;
}

inline udp_send_result udp_socket_handle::send(af::Buffer buffer) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send(std::move(buffer));
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        const bool posted = state->owner->post(
            owner_thread_, [shard, generation = generation_, buffer = std::move(buffer)]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send(std::move(buffer)));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline udp_send_result udp_socket_handle::send(af::BufferView view) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send(view);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        af::Buffer buffer = af::Buffer::copy(view);
        const bool posted = state->owner->post(
            owner_thread_, [shard, generation = generation_, buffer = std::move(buffer)]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send(std::move(buffer)));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline udp_send_result udp_socket_handle::send_to(af::Buffer buffer,
                                                  udp_endpoint endpoint) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    af::detail::SocketAddress address{};
    if (!shard->resolve_endpoint(std::move(endpoint), address)) {
        return udp_send_result::unsupported;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send_to(std::move(buffer), address);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        const bool posted =
            state->owner->post(owner_thread_, [shard, generation = generation_,
                                               buffer = std::move(buffer), address]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send_to(std::move(buffer), address));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline udp_send_result udp_socket_handle::send_to(af::BufferView view,
                                                  udp_endpoint endpoint) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    af::detail::SocketAddress address{};
    if (!shard->resolve_endpoint(std::move(endpoint), address)) {
        return udp_send_result::unsupported;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send_to(view, address);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        af::Buffer buffer = af::Buffer::copy(view);
        const bool posted =
            state->owner->post(owner_thread_, [shard, generation = generation_,
                                               buffer = std::move(buffer), address]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send_to(std::move(buffer), address));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline udp_send_result udp_socket_handle::send_to(af::Buffer buffer,
                                                  const udp_peer &peer) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    const af::detail::SocketAddress &address = peer.socket_address();
    if (!shard->supports_peer_address(address)) {
        return udp_send_result::unsupported;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send_to(std::move(buffer), address);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        const bool posted =
            state->owner->post(owner_thread_, [shard, generation = generation_,
                                               buffer = std::move(buffer), address]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send_to(std::move(buffer), address));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline udp_send_result udp_socket_handle::send_to(af::BufferView view,
                                                  const udp_peer &peer) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    const af::detail::SocketAddress &address = peer.socket_address();
    if (!shard->supports_peer_address(address)) {
        return udp_send_result::unsupported;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send_to(view, address);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        af::Buffer buffer = af::Buffer::copy(view);
        const bool posted =
            state->owner->post(owner_thread_, [shard, generation = generation_,
                                               buffer = std::move(buffer), address]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send_to(std::move(buffer), address));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline bool udp_socket_ref::valid() const noexcept {
    return shard_ != nullptr && shard_->active();
}

inline udp_socket_handle udp_socket_ref::handle() const noexcept {
    return shard_ == nullptr ? udp_socket_handle{} : shard_->handle();
}

inline af::thread_ref udp_socket_ref::owner_thread() const noexcept {
    return shard_ == nullptr ? af::thread_ref{} : shard_->owner_thread;
}

inline const udp_endpoint &udp_socket_ref::local_endpoint() const noexcept {
    static const udp_endpoint empty{};
    return shard_ == nullptr ? empty : shard_->local_endpoint;
}

inline udp_send_result udp_socket_ref::send(af::Buffer buffer) const noexcept {
    return shard_ == nullptr ? udp_send_result::closed : shard_->send(std::move(buffer));
}

inline udp_send_result udp_socket_ref::send(af::BufferView view) const noexcept {
    return shard_ == nullptr ? udp_send_result::closed : shard_->send(view);
}

inline udp_send_result udp_socket_ref::send_to(af::Buffer buffer,
                                               udp_endpoint endpoint) const noexcept {
    if (shard_ == nullptr) {
        return udp_send_result::closed;
    }
    af::detail::SocketAddress address{};
    return shard_->resolve_endpoint(std::move(endpoint), address)
               ? shard_->send_to(std::move(buffer), address)
               : udp_send_result::unsupported;
}

inline udp_send_result udp_socket_ref::send_to(af::BufferView view,
                                               udp_endpoint endpoint) const noexcept {
    if (shard_ == nullptr) {
        return udp_send_result::closed;
    }
    af::detail::SocketAddress address{};
    return shard_->resolve_endpoint(std::move(endpoint), address) ? shard_->send_to(view, address)
                                                                  : udp_send_result::unsupported;
}

inline udp_send_result udp_socket_ref::send_to(af::Buffer buffer,
                                               const udp_peer &peer) const noexcept {
    return shard_ == nullptr ? udp_send_result::closed : shard_->send_to(std::move(buffer), peer);
}

inline udp_send_result udp_socket_ref::send_to(af::BufferView view,
                                               const udp_peer &peer) const noexcept {
    return shard_ == nullptr ? udp_send_result::closed : shard_->send_to(view, peer);
}

} // namespace af::net
