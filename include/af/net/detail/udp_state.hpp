#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "af/detail/config.hpp"
#include "af/net/udp_types.hpp"

namespace af::net::detail {

template <typename Runtime> class UdpSocketShard;

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

} // namespace af::net::detail
