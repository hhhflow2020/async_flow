#pragma once

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "af/net/detail/tcp_handler.hpp"
#include "af/net/detail/tcp_server_shard.hpp"
#include "af/net/detail/tcp_state.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"

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

} // namespace af::net::detail
