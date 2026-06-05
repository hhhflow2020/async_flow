#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/async_runtime.hpp"
#include "af/buffer/buffer.hpp"
#include "af/detail/config.hpp"
#include "af/net/detail/tcp_handler.hpp"
#include "af/net/detail/tcp_control_tasks.hpp"
#include "af/net/detail/tcp_server_shard.hpp"
#include "af/net/detail/tcp_state.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"
#include "af/thread_kind.hpp"

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

template <typename Runtime> class TcpServer {
public:
    using Thread = typename Runtime::Thread;
    using State = detail::TcpServerState<Runtime>;

    struct ListenerConfig {
        std::string name;
        TcpEndpoint endpoint;
        std::vector<Thread> threads;
        TcpListenerOptions options;
        AcceptStrategy accept_strategy{AcceptStrategy::Auto};
    };

    TcpServer() : TcpServer(TcpServerConfig{}) {}

    explicit TcpServer(TcpServerConfig config) : state_(std::make_shared<State>()) {
        state_->config = normalize_config(config);
        state_->control_thread_index = first_io_thread_index();
        init_shards();
    }

    explicit TcpServer(std::vector<Thread> threads) : TcpServer() {
        bind_threads(std::move(threads));
    }

    TcpServer(TcpServerConfig config, std::vector<Thread> threads) : TcpServer(config) {
        bind_threads(std::move(threads));
    }

    ~TcpServer() = default;

    TcpServer(const TcpServer &) = delete;
    TcpServer &operator=(const TcpServer &) = delete;
    TcpServer(TcpServer &&) noexcept = default;
    TcpServer &operator=(TcpServer &&) noexcept = default;

    TcpServer &bind_threads(std::vector<Thread> threads) {
        state_->default_threads = std::move(threads);
        return *this;
    }

    template <typename Group> TcpServer &bind_threads(Group) {
        return bind_threads(thread_list<Runtime>(Group{}));
    }

    template <typename Handler>
    [[nodiscard]] ListenerResult add_listener(ListenerConfig config, Handler handler = Handler{}) {
        static_assert(std::is_copy_constructible_v<Handler>,
                      "Tcp listener handlers must be copy constructible");
        std::unique_ptr<detail::TcpHandlerBase<Runtime>> prototype;
        try {
            prototype =
                std::make_unique<detail::TcpHandlerModel<Runtime, Handler>>(std::move(handler));
        } catch (...) {
            return ListenerResult::failure(ENOMEM);
        }
        return add_listener_impl(std::move(config), std::move(prototype));
    }

    template <typename Handler>
    [[nodiscard]] ListenerResult start_listener(ListenerConfig config,
                                                Handler handler = Handler{}) {
        return add_listener(std::move(config), std::move(handler));
    }

    [[nodiscard]] bool
    remove_listener(TcpListenerHandle listener,
                    RemoveListenerPolicy policy = RemoveListenerPolicy::StopAcceptOnly) {
        if (!listener.valid()) {
            return false;
        }
        std::vector<std::uint16_t> shards;
        if (listener.slot() >= state_->listeners.size()) {
            return false;
        }
        auto &entry = state_->listeners[listener.slot()];
        if (entry == nullptr || entry->id != listener.id ||
            entry->state == ListenerState::Removed) {
            return false;
        }
        try {
            shards = entry->active_shards;
            for (const std::uint16_t shard : entry->started_shards) {
                if (!detail::contains_shard_index(shards, shard)) {
                    shards.push_back(shard);
                }
            }
        } catch (...) {
            return false;
        }
        entry->state = ListenerState::Removed;
        entry->active_shards.clear();
        entry->starting_shards.clear();
        entry->started_shards.clear();
        entry->pending_start_shards = 0U;
        release_listener_slot(listener.slot());
        detail::schedule_remove_listener_from_shards<Runtime>(state_, listener.id, shards, policy);
        return true;
    }

    [[nodiscard]] bool start() {
        std::vector<std::uint32_t> listener_slots;
        if (state_->running) {
            return true;
        }
        try {
            listener_slots.reserve(state_->listeners.size());
        } catch (...) {
            return false;
        }
        state_->running = true;
        for (std::uint32_t i = 0; i < state_->listeners.size(); ++i) {
            if (state_->listeners[i] != nullptr &&
                (state_->listeners[i]->state == ListenerState::Configured ||
                 state_->listeners[i]->state == ListenerState::Failed)) {
                listener_slots.push_back(i);
            }
        }

        bool ok = true;
        state_->accepting_connection_tasks.store(true, std::memory_order_release);
        for (const std::uint32_t slot : listener_slots) {
            ok = start_listener_slot(slot).ok() && ok;
        }
        return ok;
    }

    [[nodiscard]] bool stop() {
        auto state = state_;
        if (state == nullptr) {
            return true;
        }
        if (!state->running) {
            return true;
        }

        std::vector<std::uint16_t> shards;
        try {
            shards.reserve(state->shards.size());
            for (std::uint16_t i = 0; i < state->shards.size(); ++i) {
                if (state->shards[i] != nullptr) {
                    shards.push_back(i);
                }
            }
        } catch (...) {
            return false;
        }

        state->running = false;
        state->accepting_connection_tasks.store(false, std::memory_order_release);
        for (auto &listener : state->listeners) {
            if (listener == nullptr || listener->state == ListenerState::Removed) {
                continue;
            }
            try {
                for (const std::uint16_t shard : listener->started_shards) {
                    if (!detail::contains_shard_index(listener->active_shards, shard)) {
                        listener->active_shards.push_back(shard);
                    }
                }
            } catch (...) {
                listener->active_shards.clear();
            }
            listener->state = ListenerState::Configured;
            listener->starting_shards.clear();
            listener->started_shards.clear();
            listener->pending_start_shards = 0U;
        }

        if (shards.empty()) {
            return true;
        }
        bool ok = true;
        for (const std::uint16_t shard_index : shards) {
            bool scheduled = false;
            try {
                scheduled = Runtime::template start_task<detail::TcpStopShardTask<Runtime>>(
                    state, shard_index, state->config.connection_close_timeout);
            } catch (...) {
                scheduled = false;
            }
            if (!scheduled) {
                ok = false;
            }
        }
        return ok;
    }

    [[nodiscard]] std::shared_ptr<State> state() const noexcept {
        return state_;
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

    [[nodiscard]] static TcpServerConfig normalize_config(TcpServerConfig config) noexcept {
        const TcpConnectionConfig defaults{};
        if (config.connection.read_buffer_size == 0U) {
            config.connection.read_buffer_size = defaults.read_buffer_size;
        }
        if (config.connection.read_budget_bytes == 0U) {
            config.connection.read_budget_bytes = defaults.read_budget_bytes;
        }
        if (config.connection.write_budget_bytes == 0U) {
            config.connection.write_budget_bytes = defaults.write_budget_bytes;
        }
        if (config.connection.output_high_watermark == 0U) {
            config.connection.output_high_watermark = defaults.output_high_watermark;
        }
        if (config.connection_close_timeout.count() < 0) {
            config.connection_close_timeout = std::chrono::milliseconds(0);
        }
        return config;
    }

    [[nodiscard]] TcpListenerOptions
    normalize_listener_options(TcpListenerOptions options) const noexcept {
        const TcpListenerOptions listener_defaults{};
        const TcpConnectionConfig &connection = state_->config.connection;
        if (options.read_buffer_size == listener_defaults.read_buffer_size) {
            options.read_buffer_size = connection.read_buffer_size;
        }
        if (options.read_budget_bytes == listener_defaults.read_budget_bytes) {
            options.read_budget_bytes = connection.read_budget_bytes;
        }
        if (options.write_budget_bytes == listener_defaults.write_budget_bytes) {
            options.write_budget_bytes = connection.write_budget_bytes;
        }
        if (options.output_high_watermark == listener_defaults.output_high_watermark) {
            options.output_high_watermark = connection.output_high_watermark;
        }
        if (options.no_delay == listener_defaults.no_delay) {
            options.no_delay = connection.no_delay;
        }
        if (options.keepalive == listener_defaults.keepalive) {
            options.keepalive = connection.keepalive;
        }
        return options;
    }

    void init_shards() {
        state_->shards.reserve(Runtime::thread_count);
        for (std::uint16_t i = 0; i < Runtime::thread_count; ++i) {
            const Thread thread = Runtime::thread_from_index(i);
            state_->shards.push_back(
                std::make_unique<detail::TcpServerShard<Runtime>>(state_, i, thread));
        }
    }

    [[nodiscard]] ListenerId acquire_listener_slot() {
        std::uint32_t slot = 0;
        if (!state_->free_listener_slots.empty()) {
            slot = state_->free_listener_slots.back();
            if (slot >= state_->listeners.size()) {
                state_->listeners.resize(static_cast<std::size_t>(slot) + 1U);
            }
            if (slot >= state_->listener_generations.size()) {
                state_->listener_generations.resize(static_cast<std::size_t>(slot) + 1U, 0U);
            }
            state_->free_listener_slots.pop_back();
        } else {
            slot = static_cast<std::uint32_t>(state_->listeners.size());
            state_->listeners.reserve(static_cast<std::size_t>(slot) + 1U);
            if (slot >= state_->listener_generations.size()) {
                state_->listener_generations.reserve(static_cast<std::size_t>(slot) + 1U);
            }
            state_->listeners.push_back(nullptr);
            if (slot >= state_->listener_generations.size()) {
                state_->listener_generations.push_back(0U);
            }
        }

        std::uint32_t next_generation = state_->listener_generations[slot] + 1U;
        if (next_generation == 0U) {
            next_generation = 1U;
        }
        state_->listener_generations[slot] = next_generation;
        return ListenerId{slot, next_generation};
    }

    void release_listener_slot(std::uint32_t slot) noexcept {
        if (slot >= state_->listeners.size()) {
            return;
        }
        state_->listeners[slot].reset();
        if (slot + 1U < state_->listeners.size() && !listener_slot_is_free(slot)) {
            try {
                state_->free_listener_slots.push_back(slot);
            } catch (...) {
            }
        }
        trim_empty_listener_tail();
    }

    [[nodiscard]] bool listener_slot_is_free(std::uint32_t slot) const noexcept {
        for (const std::uint32_t free_slot : state_->free_listener_slots) {
            if (free_slot == slot) {
                return true;
            }
        }
        return false;
    }

    void trim_empty_listener_tail() noexcept {
        while (!state_->listeners.empty() && state_->listeners.back() == nullptr) {
            const auto tail = static_cast<std::uint32_t>(state_->listeners.size() - 1U);
            erase_free_listener_slot(tail);
            state_->listeners.pop_back();
        }
    }

    void erase_free_listener_slot(std::uint32_t slot) noexcept {
        for (auto it = state_->free_listener_slots.rbegin();
             it != state_->free_listener_slots.rend(); ++it) {
            if (*it == slot) {
                state_->free_listener_slots.erase(std::next(it).base());
                return;
            }
        }
    }

    [[nodiscard]] ListenerResult
    add_listener_impl(ListenerConfig config,
                      std::unique_ptr<detail::TcpHandlerBase<Runtime>> prototype) {
        if (prototype == nullptr) {
            return ListenerResult::failure(EINVAL);
        }

        if (config.threads.empty()) {
            try {
                config.threads = state_->default_threads;
            } catch (...) {
                return ListenerResult::failure(ENOMEM);
            }
        }
        config.options = normalize_listener_options(config.options);

        const int validation_error = validate_config(config);
        if (validation_error != 0) {
            return ListenerResult::failure(validation_error);
        }

        std::unique_ptr<typename State::ListenerEntry> entry;
        try {
            entry = std::make_unique<typename State::ListenerEntry>();
            entry->name = std::move(config.name);
            entry->endpoint = std::move(config.endpoint);
            entry->options = config.options;
            entry->accept_strategy = config.accept_strategy;
            entry->threads = std::move(config.threads);
            entry->handler_prototype = std::move(prototype);
            entry->state = ListenerState::Configured;
        } catch (...) {
            return ListenerResult::failure(ENOMEM);
        }

        ListenerId id{};
        bool should_start = false;
        try {
            id = acquire_listener_slot();
            entry->id = id;
            should_start = state_->running;
            state_->listeners[id.slot] = std::move(entry);
        } catch (...) {
            if (id.valid()) {
                release_listener_slot(id.slot);
            }
            return ListenerResult::failure(ENOMEM);
        }

        if (!should_start) {
            return ListenerResult::success(TcpListenerHandle{id});
        }
        ListenerResult result = start_listener_slot(id.slot);
        if (!result.ok()) {
            if (id.slot < state_->listeners.size() && state_->listeners[id.slot] != nullptr &&
                state_->listeners[id.slot]->id == id) {
                release_listener_slot(id.slot);
            }
        }
        return result;
    }

    [[nodiscard]] int validate_config(const ListenerConfig &config) const noexcept {
        if (config.threads.empty()) {
            return EINVAL;
        }
        if (config.options.backlog <= 0 || config.options.accept_budget == 0U ||
            config.options.read_budget_bytes == 0U || config.options.read_buffer_size == 0U ||
            config.options.write_budget_bytes == 0U || config.options.output_high_watermark == 0U) {
            return EINVAL;
        }
        if (config.endpoint.family == AddressFamily::Unix &&
            config.accept_strategy != AcceptStrategy::SingleAcceptor &&
            config.threads.size() > 1U) {
            return EINVAL;
        }
        if (config.endpoint.family == AddressFamily::Unix &&
            config.accept_strategy == AcceptStrategy::ReusePortPerIoThread) {
            return EINVAL;
        }
        if (config.accept_strategy == AcceptStrategy::ReusePortPerIoThread &&
            !config.options.reuse_port) {
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

    [[nodiscard]] std::vector<std::uint16_t>
    listener_open_shards(const typename State::ListenerEntry &entry) const {
        std::vector<std::uint16_t> shards;
        shards.reserve(entry.threads.size());
        const bool per_thread =
            entry.accept_strategy == AcceptStrategy::ReusePortPerIoThread ||
            (entry.accept_strategy == AcceptStrategy::Auto && entry.options.reuse_port);
        if (!per_thread) {
            shards.push_back(Runtime::thread_index(entry.threads.front()));
            return shards;
        }
        for (Thread thread : entry.threads) {
            shards.push_back(Runtime::thread_index(thread));
        }
        return shards;
    }

    [[nodiscard]] std::vector<std::uint16_t>
    listener_install_shards(const typename State::ListenerEntry &entry) const {
        std::vector<std::uint16_t> shards;
        shards.reserve(entry.threads.size());
        for (Thread thread : entry.threads) {
            shards.push_back(Runtime::thread_index(thread));
        }
        return shards;
    }

    [[nodiscard]] ListenerResult start_listener_slot(std::uint32_t slot) {
        ListenerId id{};
        std::string name;
        TcpEndpoint endpoint;
        TcpListenerOptions options;
        std::vector<std::uint16_t> install_shards;
        std::vector<std::uint16_t> open_shards;
        std::vector<std::unique_ptr<detail::TcpHandlerBase<Runtime>>> handlers;
        detail::TcpHandlerBase<Runtime> *handler_prototype = nullptr;
        if (slot >= state_->listeners.size() || state_->listeners[slot] == nullptr) {
            return ListenerResult::failure(EINVAL);
        }
        auto &entry = *state_->listeners[slot];
        if (entry.state == ListenerState::Active || entry.state == ListenerState::Starting) {
            return ListenerResult::success(TcpListenerHandle{entry.id});
        }
        if (!state_->running ||
            (entry.state != ListenerState::Configured && entry.state != ListenerState::Failed)) {
            return ListenerResult::failure(EINVAL);
        }
        id = entry.id;
        name = entry.name;
        endpoint = entry.endpoint;
        options = entry.options;
        handler_prototype = entry.handler_prototype.get();
        try {
            install_shards = listener_install_shards(entry);
            open_shards = listener_open_shards(entry);
        } catch (...) {
            mark_listener_failed(slot, ENOMEM);
            return ListenerResult::failure(ENOMEM);
        }
        if (install_shards.empty()) {
            mark_listener_failed(slot, EINVAL);
            return ListenerResult::failure(EINVAL);
        }

        if (handler_prototype == nullptr) {
            mark_listener_failed(slot, EINVAL);
            return ListenerResult::failure(EINVAL);
        }

        try {
            handlers.reserve(install_shards.size());
            for (std::size_t i = 0; i < install_shards.size(); ++i) {
                handlers.push_back(handler_prototype->clone());
            }
        } catch (...) {
            mark_listener_failed(slot, ENOMEM);
            return ListenerResult::failure(ENOMEM);
        }

        std::vector<std::vector<std::uint16_t>> target_shards_by_install;
        try {
            target_shards_by_install.reserve(install_shards.size());
            for (const std::uint16_t shard_index : install_shards) {
                target_shards_by_install.push_back(open_shards.size() == install_shards.size()
                                                       ? std::vector<std::uint16_t>{shard_index}
                                                       : install_shards);
            }
        } catch (...) {
            mark_listener_failed(slot, ENOMEM);
            return ListenerResult::failure(ENOMEM);
        }

        try {
            entry.state = ListenerState::Starting;
            entry.active_shards.clear();
            entry.starting_shards = install_shards;
            entry.started_shards.clear();
            entry.pending_start_shards = install_shards.size();
            entry.start_error = 0;
        } catch (...) {
            mark_listener_failed(slot, ENOMEM);
            return ListenerResult::failure(ENOMEM);
        }

        bool ok = true;
        for (std::size_t i = 0; i < install_shards.size(); ++i) {
            const std::uint16_t shard_index = install_shards[i];
            const bool open_listener = detail::contains_shard_index(open_shards, shard_index);
            bool scheduled = false;
            try {
                scheduled = Runtime::template start_task<detail::TcpAddListenerTask<Runtime>>(
                    state_, shard_index, slot, id, name, endpoint, options,
                    std::move(target_shards_by_install[i]), std::move(handlers[i]), open_listener);
            } catch (...) {
                scheduled = false;
            }
            if (!scheduled) {
                detail::handle_listener_start_result_on_control<Runtime>(state_, id, shard_index,
                                                                         EIO);
                ok = false;
                break;
            }
        }
        return ok ? ListenerResult::success(TcpListenerHandle{id}) : ListenerResult::failure(EIO);
    }

    void mark_listener_failed(std::uint32_t slot, int error) {
        if (slot < state_->listeners.size() && state_->listeners[slot] != nullptr) {
            state_->listeners[slot]->state = ListenerState::Failed;
            state_->listeners[slot]->active_shards.clear();
            state_->listeners[slot]->starting_shards.clear();
            state_->listeners[slot]->started_shards.clear();
            state_->listeners[slot]->pending_start_shards = 0U;
            state_->listeners[slot]->start_error = error == 0 ? EIO : error;
        }
    }

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

using send_result = SendResult;
using close_reason = CloseReason;
using accept_strategy = AcceptStrategy;
using listener_state = ListenerState;
using remove_listener_policy = RemoveListenerPolicy;
using tcp_listener_options = TcpListenerOptions;
using tcp_server_config = TcpServerConfig;
using listener_id = ListenerId;
using tcp_listener_handle = TcpListenerHandle;
using listener_result = ListenerResult;

template <typename Runtime> using tcp_connection_handle = TcpConnectionHandle<Runtime>;
template <typename Runtime> using tcp_connection_ref = TcpConnectionRef<Runtime>;
template <typename Runtime> using tcp_server = TcpServer<Runtime>;

} // namespace af::net
