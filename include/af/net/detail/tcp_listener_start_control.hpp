#pragma once

#include <cerrno>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "af/net/detail/tcp_listener_tasks.hpp"

namespace af::net::detail {

[[nodiscard]] inline bool contains_shard_index(const std::vector<std::uint16_t> &shards,
                                               std::uint16_t shard) noexcept {
    for (const std::uint16_t candidate : shards) {
        if (candidate == shard) {
            return true;
        }
    }
    return false;
}

template <typename Runtime>
void schedule_remove_listener_from_shard(const std::shared_ptr<TcpServerState<Runtime>> &state,
                                         ListenerId id, std::uint16_t shard_index,
                                         RemoveListenerPolicy policy) noexcept {
    if (state == nullptr || shard_index >= state->shards.size() ||
        state->shards[shard_index] == nullptr) {
        return;
    }
    try {
        static_cast<void>(Runtime::template start_task<TcpRemoveListenerTask<Runtime>>(
            state, shard_index, id, policy));
    } catch (...) {
    }
}

template <typename Runtime>
void schedule_remove_listener_from_shards(const std::shared_ptr<TcpServerState<Runtime>> &state,
                                          ListenerId id, const std::vector<std::uint16_t> &shards,
                                          RemoveListenerPolicy policy) noexcept {
    for (const std::uint16_t shard_index : shards) {
        schedule_remove_listener_from_shard<Runtime>(state, id, shard_index, policy);
    }
}

template <typename Runtime>
void clear_listener_start_progress(TcpListenerEntry<Runtime> &entry) noexcept {
    entry.starting_shards.clear();
    entry.started_shards.clear();
    entry.pending_start_shards = 0U;
}

template <typename Runtime>
void handle_listener_start_result_on_control(const std::shared_ptr<TcpServerState<Runtime>> &state,
                                             ListenerId id, std::uint16_t shard_index,
                                             int error) noexcept {
    if (state == nullptr || id.slot >= state->listeners.size()) {
        return;
    }
    auto &entry_ptr = state->listeners[id.slot];
    if (entry_ptr == nullptr || entry_ptr->id != id) {
        if (error == 0) {
            schedule_remove_listener_from_shard<Runtime>(state, id, shard_index,
                                                         RemoveListenerPolicy::StopAcceptOnly);
        }
        return;
    }

    auto &entry = *entry_ptr;
    if (entry.state != ListenerState::Starting) {
        if (error == 0) {
            schedule_remove_listener_from_shard<Runtime>(state, id, shard_index,
                                                         RemoveListenerPolicy::StopAcceptOnly);
        }
        return;
    }

    if (error != 0) {
        const int reported_error = error == 0 ? EIO : error;
        std::vector<std::uint16_t> started;
        try {
            started = entry.started_shards;
        } catch (...) {
            started.clear();
        }
        entry.state = ListenerState::Failed;
        entry.active_shards.clear();
        entry.start_error = reported_error;
        clear_listener_start_progress(entry);
        if (entry.handler_prototype != nullptr) {
            entry.handler_prototype->on_listener_error(TcpListenerHandle{id}, reported_error);
        }
        schedule_remove_listener_from_shards<Runtime>(state, id, started,
                                                      RemoveListenerPolicy::StopAcceptOnly);
        return;
    }

    if (!contains_shard_index(entry.started_shards, shard_index)) {
        try {
            entry.started_shards.push_back(shard_index);
        } catch (...) {
            handle_listener_start_result_on_control<Runtime>(state, id, shard_index, ENOMEM);
            return;
        }
    }
    if (entry.pending_start_shards > 0U) {
        --entry.pending_start_shards;
    }
    if (entry.pending_start_shards != 0U) {
        return;
    }

    std::vector<std::uint16_t> installed;
    try {
        installed = entry.starting_shards;
    } catch (...) {
        std::vector<std::uint16_t> started;
        try {
            started = entry.started_shards;
        } catch (...) {
            started.clear();
        }
        entry.state = ListenerState::Failed;
        entry.active_shards.clear();
        clear_listener_start_progress(entry);
        if (entry.handler_prototype != nullptr) {
            entry.handler_prototype->on_listener_error(TcpListenerHandle{id}, ENOMEM);
        }
        schedule_remove_listener_from_shards<Runtime>(state, id, started,
                                                      RemoveListenerPolicy::StopAcceptOnly);
        return;
    }

    if (state->running) {
        entry.active_shards = std::move(installed);
        entry.state = ListenerState::Active;
        entry.start_error = 0;
        clear_listener_start_progress(entry);
        return;
    }

    std::vector<std::uint16_t> started = std::move(entry.started_shards);
    entry.state = ListenerState::Configured;
    entry.active_shards.clear();
    clear_listener_start_progress(entry);
    schedule_remove_listener_from_shards<Runtime>(state, id, started,
                                                  RemoveListenerPolicy::CloseExistingConnections);
}

template <typename Runtime> class TcpListenerStartResultTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpListenerStartResultTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, ListenerId listener_id, std::uint16_t shard_index,
               int error) {
        state_ = std::move(state);
        listener_id_ = listener_id;
        shard_index_ = shard_index;
        error_ = error;
        if (state_ == nullptr || state_->control_thread_index >= Runtime::thread_count) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(state_->control_thread_index));
    }

private:
    af::TaskResult run() override {
        handle_listener_start_result_on_control<Runtime>(state_, listener_id_, shard_index_,
                                                         error_);
        return this->done();
    }

    std::shared_ptr<State> state_;
    ListenerId listener_id_{};
    std::uint16_t shard_index_{0};
    int error_{0};
};

template <typename Runtime>
void complete_listener_start_from_shard(std::shared_ptr<TcpServerState<Runtime>> state,
                                        ListenerId listener_id, std::uint16_t shard_index,
                                        int error) noexcept {
    if (state == nullptr || state->control_thread_index >= Runtime::thread_count) {
        return;
    }
    if (Runtime::current_thread_index() == state->control_thread_index) {
        handle_listener_start_result_on_control<Runtime>(state, listener_id, shard_index, error);
        return;
    }
    try {
        static_cast<void>(Runtime::template start_task<TcpListenerStartResultTask<Runtime>>(
            std::move(state), listener_id, shard_index, error));
    } catch (...) {
    }
}

} // namespace af::net::detail
