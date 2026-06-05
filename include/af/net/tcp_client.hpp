#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/net/detail/tcp_client_handler.hpp"
#include "af/net/detail/tcp_client_tasks.hpp"
#include "af/net/tcp_client_types.hpp"
#include "af/net/tcp_server.hpp"

#include <sys/socket.h>

namespace af::net {

template <typename Runtime> class TcpClient;

template <typename Runtime> class TcpClient {
public:
    using Thread = typename Runtime::Thread;
    using State = detail::TcpServerState<Runtime>;
    using ClientControlState = detail::TcpClientControlState<Runtime>;
    using ConnectControl = detail::TcpClientConnectControl<Runtime>;

    struct ConnectConfig {
        std::string name;
        TcpEndpoint remote_endpoint;
        TcpEndpoint local_endpoint = TcpEndpoint::any(0);
        bool bind_local{false};
        std::vector<Thread> threads;
        TcpClientOptions options;
        std::chrono::nanoseconds connect_timeout{std::chrono::seconds(30)};
    };

    TcpClient() : TcpClient(TcpClientRuntimeConfig{}) {}

    explicit TcpClient(TcpClientRuntimeConfig config)
        : state_(std::make_shared<State>()),
          client_control_(std::make_shared<ClientControlState>()) {
        static_cast<void>(config);
        state_->config = TcpServerConfig{};
        state_->control_thread_index = first_io_thread_index();
        init_shards();
    }

    explicit TcpClient(std::vector<Thread> threads) : TcpClient() {
        bind_threads(std::move(threads));
    }

    ~TcpClient() = default;

    TcpClient(const TcpClient &) = delete;
    TcpClient &operator=(const TcpClient &) = delete;
    TcpClient(TcpClient &&) noexcept = default;
    TcpClient &operator=(TcpClient &&) noexcept = default;

    TcpClient &bind_threads(std::vector<Thread> threads) {
        state_->default_threads = std::move(threads);
        return *this;
    }

    template <typename Group> TcpClient &bind_threads(Group) {
        return bind_threads(thread_list<Runtime>(Group{}));
    }

    template <typename Handler>
    [[nodiscard]] bool connect(ConnectConfig config, Handler handler = Handler{}) {
        static_assert(std::is_copy_constructible_v<Handler>,
                      "Tcp client handlers must be copy constructible");
        std::unique_ptr<detail::TcpHandlerBase<Runtime>> prototype;
        try {
            prototype = std::make_unique<detail::TcpClientHandlerModel<Runtime, Handler>>(
                std::move(handler));
        } catch (...) {
            return false;
        }
        return connect_impl(std::move(config), std::move(prototype));
    }

    [[nodiscard]] bool stop() {
        auto state = state_;
        if (state == nullptr) {
            return true;
        }
        if (client_control_->stopping) {
            return false;
        }
        client_control_->stopping = true;

        std::vector<std::uint16_t> shards;
        if (state->running) {
            try {
                shards.reserve(state->shards.size());
                for (std::uint16_t i = 0; i < state->shards.size(); ++i) {
                    if (state->shards[i] != nullptr) {
                        shards.push_back(i);
                    }
                }
            } catch (...) {
                client_control_->stopping = false;
                return false;
            }
        }

        state->running = false;
        state->accepting_connection_tasks.store(false, std::memory_order_release);
        auto pending_connects_by_shard = collect_pending_connects_by_shard();
        client_control_->pending_stop_shards = shards.size();
        if (shards.empty()) {
            detail::handle_tcp_client_stop_result_on_control<Runtime>(state, client_control_);
            return true;
        }
        return stop_shards(state, shards, std::move(pending_connects_by_shard));
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

    void init_shards() {
        state_->shards.reserve(Runtime::thread_count);
        for (std::uint16_t i = 0; i < Runtime::thread_count; ++i) {
            const Thread thread = Runtime::thread_from_index(i);
            state_->shards.push_back(
                std::make_unique<detail::TcpServerShard<Runtime>>(state_, i, thread));
        }
    }

    [[nodiscard]] int validate_config(const ConnectConfig &config) const noexcept {
        if (config.threads.empty()) {
            return EINVAL;
        }
        if (config.options.connection.read_budget_bytes == 0U ||
            config.options.connection.read_buffer_size == 0U ||
            config.options.connection.write_budget_bytes == 0U ||
            config.options.connection.output_high_watermark == 0U) {
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

    [[nodiscard]] bool connect_impl(ConnectConfig config,
                                    std::unique_ptr<detail::TcpHandlerBase<Runtime>> prototype) {
        if (prototype == nullptr) {
            return false;
        }

        af::detail::SocketAddress remote{};
        af::detail::SocketAddress local{};
        int address_error = 0;
        if (!af::detail::socket_address_from_endpoint(config.remote_endpoint, remote,
                                                      address_error)) {
            return false;
        }
        if (config.bind_local && !af::detail::socket_address_from_endpoint(config.local_endpoint,
                                                                           local, address_error)) {
            return false;
        }
        if (config.bind_local && local.family != remote.family) {
            return false;
        }

        std::uint16_t shard_index = 0;
        Thread selected_thread{};
        ListenerId context_id{};
        if (config.threads.empty()) {
            try {
                config.threads = state_->default_threads;
            } catch (...) {
                return false;
            }
        }
        if (validate_config(config) != 0) {
            return false;
        }
        const std::uint32_t ticket = next_connect_slot_++;
        const Thread thread = config.threads[ticket % config.threads.size()];
        selected_thread = thread;
        shard_index = Runtime::thread_index(thread);
        std::uint32_t generation = next_context_generation_++;
        if (generation == 0U) {
            generation = next_context_generation_++;
        }
        context_id = ListenerId{static_cast<std::uint32_t>(shard_index), generation};

        std::shared_ptr<detail::TcpListenerContext<Runtime>> context;
        try {
            context = std::make_shared<detail::TcpListenerContext<Runtime>>();
            context->id = context_id;
            context->name = std::move(config.name);
            context->endpoint = config.remote_endpoint;
            context->options = config.options.connection;
            context->target_shards = {shard_index};
            context->handler = std::move(prototype);
        } catch (...) {
            return false;
        }

        std::shared_ptr<ConnectControl> control;
        try {
            control = std::make_shared<ConnectControl>(shard_index, selected_thread);
        } catch (...) {
            return false;
        }
        if (!activate_pending_connect(control)) {
            return false;
        }
        bool scheduled = false;
        try {
            scheduled = Runtime::template start_task<detail::TcpConnectTask<Runtime>>(
                state_, shard_index, std::move(context), remote, local, config.bind_local,
                config.options, config.connect_timeout, client_control_, control);
        } catch (...) {
            scheduled = false;
        }
        finish_pending_connect_start(control, scheduled);
        return scheduled;
    }

    [[nodiscard]] bool activate_pending_connect(const std::shared_ptr<ConnectControl> &control) {
        if (control == nullptr) {
            return false;
        }
        if (client_control_->stopping) {
            return false;
        }
        compact_pending_connects_locked();
        try {
            client_control_->pending_connects.push_back(control);
        } catch (...) {
            return false;
        }
        ++client_control_->inflight_connects;
        state_->running = true;
        state_->accepting_connection_tasks.store(true, std::memory_order_release);
        return true;
    }

    void finish_pending_connect_start(const std::shared_ptr<ConnectControl> &control,
                                      bool scheduled) noexcept {
        if (scheduled) {
            return;
        }
        const bool matched_pending_connect = erase_pending_connect_locked(control);
        if (!matched_pending_connect) {
            compact_pending_connects_locked();
            return;
        }
        if (client_control_->inflight_connects > 0U) {
            --client_control_->inflight_connects;
        }
        compact_pending_connects_locked();
        if (client_control_->inflight_connects == 0U && !client_control_->has_connected &&
            !has_live_pending_connect_locked()) {
            state_->running = false;
            state_->accepting_connection_tasks.store(false, std::memory_order_release);
        }
    }

    void compact_pending_connects_locked() {
        auto &pending = client_control_->pending_connects;
        auto out = pending.begin();
        for (auto it = pending.begin(); it != pending.end(); ++it) {
            if (!it->expired()) {
                if (out != it) {
                    *out = std::move(*it);
                }
                ++out;
            }
        }
        pending.erase(out, pending.end());
    }

    [[nodiscard]] bool
    erase_pending_connect_locked(const std::shared_ptr<ConnectControl> &control) {
        if (control == nullptr) {
            return false;
        }
        bool matched_pending_connect = false;
        auto &pending = client_control_->pending_connects;
        auto out = pending.begin();
        for (auto it = pending.begin(); it != pending.end(); ++it) {
            std::shared_ptr<ConnectControl> current = it->lock();
            if (current == nullptr) {
                continue;
            }
            if (current == control) {
                matched_pending_connect = true;
                continue;
            }
            if (out != it) {
                *out = std::move(*it);
            }
            ++out;
        }
        pending.erase(out, pending.end());
        return matched_pending_connect;
    }

    [[nodiscard]] bool has_live_pending_connect_locked() const {
        for (const auto &weak : client_control_->pending_connects) {
            if (!weak.expired()) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] std::vector<std::vector<std::shared_ptr<ConnectControl>>>
    collect_pending_connects_by_shard() {
        std::vector<std::vector<std::shared_ptr<ConnectControl>>> by_shard;
        try {
            by_shard.resize(Runtime::thread_count);
        } catch (...) {
            return {};
        }

        auto &pending = client_control_->pending_connects;
        auto out = pending.begin();
        for (auto it = pending.begin(); it != pending.end(); ++it) {
            std::shared_ptr<ConnectControl> control = it->lock();
            if (control == nullptr) {
                continue;
            }
            *out = *it;
            ++out;
            const std::uint16_t shard_index = control->shard_index();
            if (shard_index < by_shard.size()) {
                try {
                    by_shard[shard_index].push_back(std::move(control));
                } catch (...) {
                }
            }
        }
        pending.erase(out, pending.end());
        return by_shard;
    }

    [[nodiscard]] bool stop_shards(
        std::shared_ptr<State> state, const std::vector<std::uint16_t> &shards,
        std::vector<std::vector<std::shared_ptr<ConnectControl>>> pending_connects_by_shard) {
        if (shards.empty()) {
            return true;
        }
        if (pending_connects_by_shard.size() < Runtime::thread_count) {
            try {
                pending_connects_by_shard.resize(Runtime::thread_count);
            } catch (...) {
                return false;
            }
        }
        bool ok = true;
        for (const std::uint16_t shard_index : shards) {
            bool scheduled = false;
            try {
                scheduled = Runtime::template start_task<detail::TcpStopClientShardTask<Runtime>>(
                    state, client_control_, shard_index,
                    std::move(pending_connects_by_shard[shard_index]));
            } catch (...) {
                scheduled = false;
            }
            if (!scheduled) {
                ok = false;
                detail::handle_tcp_client_stop_result_on_control<Runtime>(state, client_control_);
            }
        }
        return ok;
    }

    std::shared_ptr<State> state_;
    std::shared_ptr<ClientControlState> client_control_;
    std::uint32_t next_connect_slot_{0};
    std::uint32_t next_context_generation_{1};
};

template <typename Runtime> using tcp_client = TcpClient<Runtime>;

} // namespace af::net
