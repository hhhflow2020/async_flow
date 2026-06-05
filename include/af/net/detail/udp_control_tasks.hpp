#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <utility>

#include "af/buffer/buffer.hpp"
#include "af/detail/net/socket_address.hpp"
#include "af/net/detail/udp_handler.hpp"
#include "af/net/detail/udp_socket_shard.hpp"
#include "af/net/detail/udp_state.hpp"

namespace af::net::detail {

enum class UdpSendKind : std::uint8_t {
    Send,
    SendTo,
};

template <typename Runtime> class UdpStartResultTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = UdpSocketState<Runtime>;

public:
    explicit UdpStartResultTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, int error) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        error_ = error;
        if (state_ == nullptr || state_->control_thread_index >= Runtime::thread_count) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(state_->control_thread_index));
    }

private:
    af::TaskResult run() override {
        if (state_ == nullptr || error_ == 0) {
            return this->done();
        }
        auto &active = state_->active_shards;
        auto out = active.begin();
        bool removed = false;
        for (auto it = active.begin(); it != active.end(); ++it) {
            if (*it == shard_index_) {
                removed = true;
                continue;
            }
            if (out != it) {
                *out = *it;
            }
            ++out;
        }
        if (removed) {
            active.erase(out, active.end());
            state_->running = !active.empty();
            if (state_->running) {
                publish_udp_active_shard_snapshot<Runtime>(*state_, active);
            } else {
                state_->accepting_send_tasks.store(false, std::memory_order_release);
                clear_udp_active_shard_snapshot<Runtime>(*state_);
            }
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    int error_{0};
};

template <typename Runtime>
void complete_udp_start_from_shard(std::shared_ptr<UdpSocketState<Runtime>> state,
                                   std::uint16_t shard_index, int error) noexcept {
    if (state == nullptr || state->control_thread_index >= Runtime::thread_count) {
        return;
    }
    if (Runtime::current_thread_index() == state->control_thread_index) {
        if (error == 0) {
            return;
        }
        auto &active = state->active_shards;
        auto out = active.begin();
        bool removed = false;
        for (auto it = active.begin(); it != active.end(); ++it) {
            if (*it == shard_index) {
                removed = true;
                continue;
            }
            if (out != it) {
                *out = *it;
            }
            ++out;
        }
        if (removed) {
            active.erase(out, active.end());
            state->running = !active.empty();
            if (state->running) {
                publish_udp_active_shard_snapshot<Runtime>(*state, active);
            } else {
                state->accepting_send_tasks.store(false, std::memory_order_release);
                clear_udp_active_shard_snapshot<Runtime>(*state);
            }
        }
        return;
    }
    try {
        static_cast<void>(Runtime::template start_task<UdpStartResultTask<Runtime>>(
            std::move(state), shard_index, error));
    } catch (...) {
    }
}

template <typename Runtime>
void notify_udp_start_error(const std::shared_ptr<UdpSocketState<Runtime>> &state,
                            std::uint16_t shard_index,
                            const std::shared_ptr<UdpSocketContext<Runtime>> &context,
                            int error) noexcept;

template <typename Runtime> class UdpStartShardTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = UdpSocketState<Runtime>;
    using Context = UdpSocketContext<Runtime>;

public:
    explicit UdpStartShardTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index,
               std::shared_ptr<Context> context) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        context_ = std::move(context);
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || context_ == nullptr) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

private:
    af::TaskResult run() override {
        int error = EINVAL;
        std::shared_ptr<Context> context = std::move(context_);
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr && context != nullptr) {
            error = state_->shards[shard_index_]->start_on_owner(context);
        }
        if (error != 0) {
            notify_udp_start_error<Runtime>(state_, shard_index_, context, error);
        }
        complete_udp_start_from_shard<Runtime>(state_, shard_index_, error);
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::shared_ptr<Context> context_;
};

template <typename Runtime> class UdpStopShardTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = UdpSocketState<Runtime>;

public:
    explicit UdpStopShardTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index) {
        state_ = std::move(state);
        shard_index_ = shard_index;
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
            state_->shards[shard_index_]->stop_on_owner();
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
};

template <typename Runtime> class UdpSendTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = UdpSocketState<Runtime>;

public:
    explicit UdpSendTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t generation,
               af::Buffer buffer) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        generation_ = generation;
        kind_ = UdpSendKind::Send;
        buffer_ = std::move(buffer);
        return schedule_on_owner();
    }

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index, std::uint32_t generation,
               af::Buffer buffer, af::detail::SocketAddress address) {
        state_ = std::move(state);
        shard_index_ = shard_index;
        generation_ = generation;
        kind_ = UdpSendKind::SendTo;
        buffer_ = std::move(buffer);
        address_ = address;
        return schedule_on_owner();
    }

private:
    [[nodiscard]] bool schedule_on_owner() noexcept {
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || generation_ == 0U) {
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
        if (!shard->matches_generation(generation_)) {
            return this->done();
        }
        switch (kind_) {
        case UdpSendKind::Send:
            static_cast<void>(shard->send(std::move(buffer_)));
            break;
        case UdpSendKind::SendTo:
            static_cast<void>(shard->send_to_address(std::move(buffer_), address_));
            break;
        }
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    std::uint32_t generation_{0};
    UdpSendKind kind_{UdpSendKind::Send};
    af::Buffer buffer_;
    af::detail::SocketAddress address_{};
};

} // namespace af::net::detail
