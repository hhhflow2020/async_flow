#pragma once

namespace af::net {

template <typename Runtime>
bool TcpServer<Runtime>::remove_listener(TcpListenerHandle listener, RemoveListenerPolicy policy) {
    if (!listener.valid()) {
        return false;
    }
    std::vector<std::uint16_t> shards;
    if (listener.slot() >= state_->listeners.size()) {
        return false;
    }
    auto &entry = state_->listeners[listener.slot()];
    if (entry == nullptr || entry->id != listener.id || entry->state == ListenerState::Removed) {
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

template <typename Runtime> bool TcpServer<Runtime>::start() {
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

template <typename Runtime> bool TcpServer<Runtime>::stop() {
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

template <typename Runtime>
constexpr bool TcpServer<Runtime>::is_io_thread(Thread thread) noexcept {
    const af::thread_kind kind = Runtime::thread_kind(thread);
    return kind == af::thread_kind::io;
}

template <typename Runtime> std::uint16_t TcpServer<Runtime>::first_io_thread_index() noexcept {
    for (std::uint16_t i = 0; i < Runtime::thread_count; ++i) {
        if (is_io_thread(Runtime::thread_from_index(i))) {
            return i;
        }
    }
    return Runtime::invalid_thread_index;
}

template <typename Runtime>
TcpServerConfig TcpServer<Runtime>::normalize_config(TcpServerConfig config) noexcept {
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

template <typename Runtime>
TcpListenerOptions
TcpServer<Runtime>::normalize_listener_options(TcpListenerOptions options) const noexcept {
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

template <typename Runtime> void TcpServer<Runtime>::init_shards() {
    state_->shards.reserve(Runtime::thread_count);
    for (std::uint16_t i = 0; i < Runtime::thread_count; ++i) {
        const Thread thread = Runtime::thread_from_index(i);
        state_->shards.push_back(
            std::make_unique<detail::TcpServerShard<Runtime>>(state_, i, thread));
    }
}

template <typename Runtime> ListenerId TcpServer<Runtime>::acquire_listener_slot() {
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

template <typename Runtime>
void TcpServer<Runtime>::release_listener_slot(std::uint32_t slot) noexcept {
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

template <typename Runtime>
bool TcpServer<Runtime>::listener_slot_is_free(std::uint32_t slot) const noexcept {
    for (const std::uint32_t free_slot : state_->free_listener_slots) {
        if (free_slot == slot) {
            return true;
        }
    }
    return false;
}

template <typename Runtime> void TcpServer<Runtime>::trim_empty_listener_tail() noexcept {
    while (!state_->listeners.empty() && state_->listeners.back() == nullptr) {
        const auto tail = static_cast<std::uint32_t>(state_->listeners.size() - 1U);
        erase_free_listener_slot(tail);
        state_->listeners.pop_back();
    }
}

template <typename Runtime>
void TcpServer<Runtime>::erase_free_listener_slot(std::uint32_t slot) noexcept {
    for (auto it = state_->free_listener_slots.rbegin(); it != state_->free_listener_slots.rend();
         ++it) {
        if (*it == slot) {
            state_->free_listener_slots.erase(std::next(it).base());
            return;
        }
    }
}

template <typename Runtime>
ListenerResult
TcpServer<Runtime>::add_listener_impl(typename TcpServer<Runtime>::ListenerConfig config,
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

template <typename Runtime>
int TcpServer<Runtime>::validate_config(const ListenerConfig &config) const noexcept {
    if (config.threads.empty()) {
        return EINVAL;
    }
    if (config.options.backlog <= 0 || config.options.accept_budget == 0U ||
        config.options.read_budget_bytes == 0U || config.options.read_buffer_size == 0U ||
        config.options.write_budget_bytes == 0U || config.options.output_high_watermark == 0U) {
        return EINVAL;
    }
    if (config.endpoint.family == AddressFamily::Unix &&
        config.accept_strategy != AcceptStrategy::SingleAcceptor && config.threads.size() > 1U) {
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

template <typename Runtime>
std::vector<std::uint16_t>
TcpServer<Runtime>::listener_open_shards(const typename State::ListenerEntry &entry) const {
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

template <typename Runtime>
std::vector<std::uint16_t>
TcpServer<Runtime>::listener_install_shards(const typename State::ListenerEntry &entry) const {
    std::vector<std::uint16_t> shards;
    shards.reserve(entry.threads.size());
    for (Thread thread : entry.threads) {
        shards.push_back(Runtime::thread_index(thread));
    }
    return shards;
}

template <typename Runtime>
ListenerResult TcpServer<Runtime>::start_listener_slot(std::uint32_t slot) {
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
            detail::handle_listener_start_result_on_control<Runtime>(state_, id, shard_index, EIO);
            ok = false;
            break;
        }
    }
    return ok ? ListenerResult::success(TcpListenerHandle{id}) : ListenerResult::failure(EIO);
}

template <typename Runtime>
void TcpServer<Runtime>::mark_listener_failed(std::uint32_t slot, int error) {
    if (slot < state_->listeners.size() && state_->listeners[slot] != nullptr) {
        state_->listeners[slot]->state = ListenerState::Failed;
        state_->listeners[slot]->active_shards.clear();
        state_->listeners[slot]->starting_shards.clear();
        state_->listeners[slot]->started_shards.clear();
        state_->listeners[slot]->pending_start_shards = 0U;
        state_->listeners[slot]->start_error = error == 0 ? EIO : error;
    }
}

} // namespace af::net
