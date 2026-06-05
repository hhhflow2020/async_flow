#pragma once

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/detail/net/socket_address.hpp"
#include "af/net/detail/udp_control_tasks.hpp"
#include "af/net/detail/udp_handler.hpp"
#include "af/net/detail/udp_socket_shard.hpp"
#include "af/net/detail/udp_state.hpp"
#include "af/net/udp_socket_handle.hpp"
#include "af/net/udp_types.hpp"
#include "af/thread_kind.hpp"

#include <sys/socket.h>

namespace af::net {

template <typename Runtime> class UdpSocketHandle;
template <typename Runtime> class UdpSocketRef;

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

template <typename Runtime> using udp_socket = UdpSocket<Runtime>;

} // namespace af::net
