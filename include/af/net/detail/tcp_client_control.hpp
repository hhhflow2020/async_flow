#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "af/io_socket.hpp"
#include "af/net/detail/tcp_socket_ops.hpp"
#include "af/net/detail/tcp_state.hpp"

namespace af::net::detail {

template <typename Runtime> class TcpClientConnectControl {
public:
    using Thread = typename Runtime::Thread;

    TcpClientConnectControl(std::uint16_t shard_index, Thread thread) noexcept
        : shard_index_(shard_index), thread_(thread) {}

    [[nodiscard]] std::uint16_t shard_index() const noexcept {
        return shard_index_;
    }

    [[nodiscard]] bool cancel_requested() const noexcept {
        return cancel_requested_;
    }

    void bind(int *fd, IoOpState *connect_state) noexcept {
        fd_ = fd;
        connect_state_ = connect_state;
        active_ = true;
        if (cancel_requested_) {
            cancel();
        }
    }

    void deactivate() noexcept {
        active_ = false;
        fd_ = nullptr;
        connect_state_ = nullptr;
    }

    void request_cancel_on_owner() noexcept {
        cancel_requested_ = true;
        cancel();
    }

private:
    void cancel() noexcept {
        if (!active_) {
            return;
        }
        if (connect_state_ != nullptr && connect_state_->waiting) {
            static_cast<void>(Runtime::cancel_io(thread_, *connect_state_));
        }
        if (fd_ != nullptr) {
            close_fd(*fd_);
        }
    }

    const std::uint16_t shard_index_{0};
    const Thread thread_;
    int *fd_{nullptr};
    IoOpState *connect_state_{nullptr};
    bool cancel_requested_{false};
    bool active_{false};
};

template <typename Runtime> struct TcpClientControlState {
    bool stopping{false};
    std::size_t inflight_connects{0};
    std::size_t pending_stop_shards{0};
    bool has_connected{false};
    std::vector<std::weak_ptr<TcpClientConnectControl<Runtime>>> pending_connects;
};

template <typename Runtime> class TcpClientConnectResultTask;
template <typename Runtime> class TcpClientStopResultTask;

template <typename Runtime>
void handle_tcp_client_connect_result_on_control(
    const std::shared_ptr<TcpServerState<Runtime>> &state,
    const std::shared_ptr<TcpClientControlState<Runtime>> &control,
    const std::shared_ptr<TcpClientConnectControl<Runtime>> &connect, bool connected) noexcept {
    if (state == nullptr || control == nullptr) {
        return;
    }
    bool matched_pending_connect = false;
    auto &pending = control->pending_connects;
    auto out = pending.begin();
    for (auto it = pending.begin(); it != pending.end(); ++it) {
        std::shared_ptr<TcpClientConnectControl<Runtime>> current = it->lock();
        if (current == nullptr) {
            continue;
        }
        if (current == connect) {
            matched_pending_connect = true;
            continue;
        }
        if (out != it) {
            *out = std::move(*it);
        }
        ++out;
    }
    pending.erase(out, pending.end());

    if (!matched_pending_connect) {
        return;
    }
    if (control->inflight_connects > 0U) {
        --control->inflight_connects;
    }
    if (connected) {
        control->has_connected = true;
    }
    if (control->inflight_connects == 0U && !control->has_connected && !control->stopping) {
        state->running = false;
        state->accepting_connection_tasks.store(false, std::memory_order_release);
    }
}

template <typename Runtime>
void complete_tcp_client_connect_from_shard(
    std::shared_ptr<TcpServerState<Runtime>> state,
    std::shared_ptr<TcpClientControlState<Runtime>> control,
    std::shared_ptr<TcpClientConnectControl<Runtime>> connect, bool connected) noexcept;

template <typename Runtime>
void handle_tcp_client_stop_result_on_control(
    const std::shared_ptr<TcpServerState<Runtime>> &state,
    const std::shared_ptr<TcpClientControlState<Runtime>> &control) noexcept {
    if (state == nullptr || control == nullptr || !control->stopping) {
        return;
    }
    if (control->pending_stop_shards > 0U) {
        --control->pending_stop_shards;
    }
    if (control->pending_stop_shards != 0U) {
        return;
    }
    control->inflight_connects = 0U;
    control->has_connected = false;
    control->pending_connects.clear();
    control->stopping = false;
    state->running = false;
    state->accepting_connection_tasks.store(false, std::memory_order_release);
}

template <typename Runtime>
void complete_tcp_client_stop_from_shard(
    std::shared_ptr<TcpServerState<Runtime>> state,
    std::shared_ptr<TcpClientControlState<Runtime>> control) noexcept;

} // namespace af::net::detail
