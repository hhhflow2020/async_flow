#pragma once

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "af/buffer/buffer.hpp"
#include "af/net/detail/tcp_handler.hpp"
#include "af/net/detail/tcp_server_shard.hpp"
#include "af/net/detail/tcp_socket_ops.hpp"
#include "af/net/detail/tcp_state.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"

#include <sys/socket.h>

namespace af::net::detail {

template <typename Runtime> class TcpListenerStartResultTask;

template <typename Runtime>
void complete_listener_start_from_shard(std::shared_ptr<TcpServerState<Runtime>> state,
                                        ListenerId listener_id, std::uint16_t shard_index,
                                        int error) noexcept;

template <typename Runtime> class TcpAddListenerTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;
    using HandlerPtr = std::unique_ptr<TcpHandlerBase<Runtime>>;
    using ListenerContext = TcpListenerContext<Runtime>;

public:
    explicit TcpAddListenerTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t listener_slot,
               ListenerId listener_id, std::string name, TcpEndpoint endpoint,
               TcpListenerOptions options, std::vector<std::uint16_t> target_shards,
               HandlerPtr handler, bool open_listener) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        listener_slot_ = listener_slot;
        listener_id_ = listener_id;
        name_ = std::move(name);
        endpoint_ = std::move(endpoint);
        options_ = options;
        target_shards_ = std::move(target_shards);
        handler_ = std::move(handler);
        open_listener_ = open_listener;
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || handler_ == nullptr) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

private:
    af::TaskResult run() override {
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr && handler_ != nullptr) {
            int error = EINVAL;
            try {
                auto context = std::make_shared<ListenerContext>();
                context->id = listener_id_;
                context->name = std::move(name_);
                context->endpoint = std::move(endpoint_);
                context->options = options_;
                context->target_shards = std::move(target_shards_);
                context->handler = std::move(handler_);
                error = state_->shards[shard_index_]->add_listener_on_owner(
                    listener_slot_, std::move(context), open_listener_);
            } catch (...) {
                error = ENOMEM;
            }
            complete_listener_start_from_shard<Runtime>(state_, listener_id_, shard_index_, error);
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::uint32_t listener_slot_{0};
    ListenerId listener_id_{};
    std::string name_;
    TcpEndpoint endpoint_;
    TcpListenerOptions options_;
    std::vector<std::uint16_t> target_shards_;
    HandlerPtr handler_;
    bool open_listener_{false};
};

template <typename Runtime> class TcpRemoveListenerTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpRemoveListenerTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, ListenerId listener_id,
               RemoveListenerPolicy policy) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        listener_id_ = listener_id;
        policy_ = policy;
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

private:
    af::TaskResult run() override {
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr) {
            state_->shards[shard_index_]->remove_listener_on_owner(listener_id_, policy_);
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    ListenerId listener_id_{};
    RemoveListenerPolicy policy_{RemoveListenerPolicy::StopAcceptOnly};
};

template <typename Runtime> class TcpStopShardTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;
    using Clock = std::chrono::steady_clock;

public:
    explicit TcpStopShardTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index,
               std::chrono::milliseconds close_timeout) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        close_timeout_ = close_timeout.count() < 0 ? std::chrono::milliseconds(0) : close_timeout;
        deadline_ = Clock::now() + close_timeout_;
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

private:
    af::TaskResult run() override {
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr) {
            auto *shard = state_->shards[shard_index_].get();
            if (!listeners_closed_) {
                shard->close_listeners_on_owner();
                listeners_closed_ = true;
            }

            const std::size_t remaining =
                close_timeout_.count() <= 0 ? 0U : shard->close_connections_after_flush_on_owner();
            const auto now = Clock::now();
            if (remaining == 0U || close_timeout_.count() <= 0 || now >= deadline_) {
                shard->force_close_connections_on_owner();
                return this->done();
            }

            auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(deadline_ - now);
            if (delay > std::chrono::milliseconds(1)) {
                delay = std::chrono::milliseconds(1);
            }
            return this->pending_after(Runtime::thread_from_index(shard_index_), delay);
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::chrono::milliseconds close_timeout_{0};
    Clock::time_point deadline_{};
    bool listeners_closed_{false};
};

template <typename Runtime> class TcpAdoptConnectionTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpAdoptConnectionTask(typename Base::FactoryToken token) : Base(token) {}

    ~TcpAdoptConnectionTask() override {
        detail::close_fd(fd_);
    }

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, ListenerId listener_id,
               int fd, const sockaddr *peer, socklen_t peer_size) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        listener_id_ = listener_id;
        fd_ = fd;
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || fd_ < 0 || peer == nullptr ||
            peer_size > sizeof(peer_)) {
            fd_ = -1;
            return false;
        }
        peer_size_ = peer_size;
        std::memcpy(&peer_, peer, peer_size_);
        if (!this->schedule(Runtime::thread_from_index(shard_index_))) {
            fd_ = -1;
            return false;
        }
        return true;
    }

private:
    af::TaskResult run() override {
        int fd = fd_;
        fd_ = -1;
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr && fd >= 0) {
            state_->shards[shard_index_]->adopt_connection(
                listener_id_, fd, reinterpret_cast<const sockaddr *>(&peer_), peer_size_);
            return this->done();
        }
        detail::close_fd(fd);
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    ListenerId listener_id_{};
    int fd_{-1};
    sockaddr_storage peer_{};
    socklen_t peer_size_{0};
};

template <typename Runtime> class TcpConnectionCommandTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpConnectionCommandTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t slot,
               std::uint32_t generation, af::Buffer buffer) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        slot_ = slot;
        generation_ = generation;
        kind_ = TcpConnectionCommandKind::Send;
        buffer_ = std::move(buffer);
        return schedule_on_owner();
    }

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t slot,
               std::uint32_t generation, TcpConnectionCommandKind kind, bool flag) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        slot_ = slot;
        generation_ = generation;
        kind_ = kind;
        flag_ = flag;
        return schedule_on_owner();
    }

private:
    [[nodiscard]] bool schedule_on_owner() noexcept {
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

    af::TaskResult run() override {
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            return this->done();
        }
        auto *shard = state_->shards[shard_index_].get();
        switch (kind_) {
        case TcpConnectionCommandKind::Send:
            static_cast<void>(shard->send_to(slot_, generation_, std::move(buffer_)));
            break;
        case TcpConnectionCommandKind::Close:
            static_cast<void>(shard->close_connection(slot_, generation_));
            break;
        case TcpConnectionCommandKind::CloseAfterFlush:
            static_cast<void>(shard->close_connection_after_flush(slot_, generation_));
            break;
        case TcpConnectionCommandKind::ShutdownWrite:
            static_cast<void>(shard->shutdown_connection_write(slot_, generation_));
            break;
        case TcpConnectionCommandKind::PauseRead:
            static_cast<void>(shard->pause_connection_read(slot_, generation_));
            break;
        case TcpConnectionCommandKind::ResumeRead:
            static_cast<void>(shard->resume_connection_read(slot_, generation_));
            break;
        case TcpConnectionCommandKind::SetNoDelay:
            static_cast<void>(shard->set_connection_no_delay(slot_, generation_, flag_));
            break;
        case TcpConnectionCommandKind::SetKeepAlive:
            static_cast<void>(shard->set_connection_keepalive(slot_, generation_, flag_));
            break;
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::uint32_t slot_{0};
    std::uint32_t generation_{0};
    TcpConnectionCommandKind kind_{TcpConnectionCommandKind::Close};
    bool flag_{false};
    af::Buffer buffer_;
};

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
