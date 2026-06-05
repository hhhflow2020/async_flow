#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "af/detail/net/socket_address.hpp"
#include "af/io_socket.hpp"
#include "af/io_timeout.hpp"
#include "af/net/detail/tcp_client_control.hpp"
#include "af/net/detail/tcp_handler.hpp"
#include "af/net/detail/tcp_server_shard.hpp"
#include "af/net/detail/tcp_socket_ops.hpp"
#include "af/net/detail/tcp_state.hpp"
#include "af/net/tcp_client_types.hpp"
#include "af/net/tcp_types.hpp"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace af::net::detail {

template <typename Runtime> class TcpConnectTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;
    using Context = TcpListenerContext<Runtime>;
    using ConnectControl = TcpClientConnectControl<Runtime>;

public:
    using Thread = typename Runtime::Thread;

    explicit TcpConnectTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state, std::uint16_t shard_index,
               std::shared_ptr<Context> context, af::detail::SocketAddress remote,
               af::detail::SocketAddress local, bool bind_local, TcpClientOptions options,
               std::chrono::nanoseconds connect_timeout,
               std::shared_ptr<TcpClientControlState<Runtime>> client_control,
               std::shared_ptr<ConnectControl> control) noexcept {
        state_ = std::move(state);
        shard_index_ = shard_index;
        context_ = std::move(context);
        remote_ = remote;
        local_ = local;
        bind_local_ = bind_local;
        options_ = options;
        connect_timeout_ = connect_timeout;
        client_control_ = std::move(client_control);
        control_ = std::move(control);
        if (state_ == nullptr || context_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || remote_.size == 0U || control_ == nullptr) {
            return false;
        }
        thread_ = Runtime::thread_from_index(shard_index_);
        return this->schedule(thread_);
    }

private:
    af::TaskResult run() override {
        if (this->runtime_stopping() || !client_running() || control_->cancel_requested()) {
            finish_failed(ECANCELED);
            return this->done();
        }
        if (!prepared_) {
            const int error = prepare_socket();
            if (error != 0) {
                finish_failed(error);
                return this->done();
            }
            prepared_ = true;
            control_->bind(&fd_, &connect_state_);
            if (control_->cancel_requested()) {
                finish_failed(ECANCELED);
                return this->done();
            }
            if (connect_timeout_.count() > 0) {
                deadline_.set_after(connect_timeout_);
            }
        }

        if (deadline_.armed || deadline_.cancel_pending || deadline_.timeout_cancel_pending) {
            const IoStatus timeout = af::arm_io_timeout(*this, thread_, deadline_, connect_state_);
            if (timeout.pending()) {
                return this->pending();
            }
            if (timeout.failed()) {
                finish_failed(timeout.error);
                return this->done();
            }
        }

        const IoStatus connect = af::io_connect(
            *this, thread_, fd_, reinterpret_cast<const sockaddr *>(&remote_.storage), remote_.size,
            connect_state_);
        if (connect.pending()) {
            if (deadline_.configured() && !deadline_.armed) {
                const IoStatus timeout =
                    af::arm_io_timeout(*this, thread_, deadline_, connect_state_);
                if (timeout.pending()) {
                    return this->pending();
                }
                if (timeout.failed()) {
                    finish_failed(timeout.error);
                    return this->done();
                }
            }
            return this->pending();
        }
        if (connect.failed()) {
            finish_failed(connect.error);
            return this->done();
        }
        if (!client_running() || control_->cancel_requested()) {
            finish_failed(ECANCELED);
            return this->done();
        }

        finish_connected();
        return this->done();
    }

    void on_runtime_cancel() noexcept override {
        if (control_ != nullptr) {
            control_->deactivate();
        }
        close_fd(fd_);
        connect_state_.reset();
        deadline_.reset();
        complete_client_connect(false);
    }

    [[nodiscard]] bool client_running() const noexcept {
        if (state_ == nullptr || client_control_ == nullptr) {
            return false;
        }
        return state_->running && !client_control_->stopping;
    }

    [[nodiscard]] int prepare_socket() noexcept {
        fd_ = ::socket(remote_.family, SOCK_STREAM, 0);
        if (fd_ < 0) {
            return errno == 0 ? EIO : errno;
        }
        if (!set_nonblocking(fd_) || !set_cloexec(fd_)) {
            const int error = errno == 0 ? EIO : errno;
            close_fd(fd_);
            return error;
        }
        set_no_sigpipe(fd_);
        if (options_.no_delay && remote_.family != AF_UNIX) {
            int one = 1;
            static_cast<void>(::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)));
        }
        if (options_.keep_alive) {
            int one = 1;
            static_cast<void>(::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)));
        }
        if (bind_local_) {
            if (local_.size == 0U || local_.family != remote_.family) {
                close_fd(fd_);
                return EINVAL;
            }
            if (::bind(fd_, reinterpret_cast<const sockaddr *>(&local_.storage), local_.size) !=
                0) {
                const int error = errno == 0 ? EIO : errno;
                close_fd(fd_);
                return error;
            }
        }
        return 0;
    }

    void finish_connected() noexcept {
        if (control_ != nullptr) {
            control_->deactivate();
        }
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr) {
            finish_failed(ECANCELED);
            return;
        }
        auto *shard = state_->shards[shard_index_].get();
        int connected_fd = fd_;
        fd_ = -1;
        state_->accepting_connection_tasks.store(true, std::memory_order_release);
        const bool connected = shard->create_connection(
            context_, connected_fd, reinterpret_cast<const sockaddr *>(&remote_.storage),
            remote_.size);
        complete_client_connect(connected);
    }

    void finish_failed(int error) noexcept {
        if (control_ != nullptr) {
            control_->deactivate();
        }
        close_fd(fd_);
        connect_state_.reset();
        deadline_.reset();
        if (context_ != nullptr && context_->handler != nullptr) {
            context_->handler->on_listener_error(TcpListenerHandle{context_->id},
                                                 error == 0 ? EIO : error);
        }
        complete_client_connect(false);
    }

    void complete_client_connect(bool connected) noexcept {
        if (completed_) {
            return;
        }
        completed_ = true;
        complete_tcp_client_connect_from_shard(state_, client_control_, control_, connected);
    }

    std::shared_ptr<State> state_;
    std::uint16_t shard_index_{0};
    Thread thread_{};
    std::shared_ptr<Context> context_;
    af::detail::SocketAddress remote_{};
    af::detail::SocketAddress local_{};
    bool bind_local_{false};
    TcpClientOptions options_{};
    std::chrono::nanoseconds connect_timeout_{};
    std::shared_ptr<TcpClientControlState<Runtime>> client_control_;
    std::shared_ptr<ConnectControl> control_;
    IoOpState connect_state_{};
    IoDeadline deadline_{};
    int fd_{-1};
    bool prepared_{false};
    bool completed_{false};
};

template <typename Runtime> class TcpStopClientShardTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;
    using ConnectControl = TcpClientConnectControl<Runtime>;

public:
    explicit TcpStopClientShardTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state,
               std::shared_ptr<TcpClientControlState<Runtime>> client_control,
               std::uint16_t shard_index,
               std::vector<std::shared_ptr<ConnectControl>> pending_connects) {
        state_ = std::move(state);
        client_control_ = std::move(client_control);
        shard_index_ = shard_index;
        pending_connects_ = std::move(pending_connects);
        if (state_ == nullptr || shard_index_ >= state_->shards.size() ||
            state_->shards[shard_index_] == nullptr || client_control_ == nullptr) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(shard_index_));
    }

private:
    af::TaskResult run() override {
        for (const auto &control : pending_connects_) {
            if (control != nullptr) {
                control->request_cancel_on_owner();
            }
        }
        if (state_ != nullptr && shard_index_ < state_->shards.size() &&
            state_->shards[shard_index_] != nullptr) {
            state_->shards[shard_index_]->stop_on_owner();
        }
        complete_tcp_client_stop_from_shard(state_, client_control_);
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::shared_ptr<TcpClientControlState<Runtime>> client_control_;
    std::uint16_t shard_index_{0};
    std::vector<std::shared_ptr<ConnectControl>> pending_connects_;
};

template <typename Runtime> class TcpClientConnectResultTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;
    using ConnectControl = TcpClientConnectControl<Runtime>;

public:
    explicit TcpClientConnectResultTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state,
               std::shared_ptr<TcpClientControlState<Runtime>> control,
               std::shared_ptr<ConnectControl> connect, bool connected) {
        state_ = std::move(state);
        control_ = std::move(control);
        connect_ = std::move(connect);
        connected_ = connected;
        if (state_ == nullptr || control_ == nullptr ||
            state_->control_thread_index >= Runtime::thread_count) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(state_->control_thread_index));
    }

private:
    af::TaskResult run() override {
        handle_tcp_client_connect_result_on_control<Runtime>(state_, control_, connect_,
                                                             connected_);
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::shared_ptr<TcpClientControlState<Runtime>> control_;
    std::shared_ptr<ConnectControl> connect_;
    bool connected_{false};
};

template <typename Runtime> class TcpClientStopResultTask final : public Runtime::Task {
    using Base = typename Runtime::Task;
    using State = TcpServerState<Runtime>;

public:
    explicit TcpClientStopResultTask(typename Base::FactoryToken token) : Base(token) {}

    bool do_it(std::shared_ptr<State> state,
               std::shared_ptr<TcpClientControlState<Runtime>> control) {
        state_ = std::move(state);
        control_ = std::move(control);
        if (state_ == nullptr || control_ == nullptr ||
            state_->control_thread_index >= Runtime::thread_count) {
            return false;
        }
        return this->schedule(Runtime::thread_from_index(state_->control_thread_index));
    }

private:
    af::TaskResult run() override {
        handle_tcp_client_stop_result_on_control<Runtime>(state_, control_);
        return this->done();
    }

    std::shared_ptr<State> state_;
    std::shared_ptr<TcpClientControlState<Runtime>> control_;
};

template <typename Runtime>
void complete_tcp_client_connect_from_shard(
    std::shared_ptr<TcpServerState<Runtime>> state,
    std::shared_ptr<TcpClientControlState<Runtime>> control,
    std::shared_ptr<TcpClientConnectControl<Runtime>> connect, bool connected) noexcept {
    if (state == nullptr || control == nullptr ||
        state->control_thread_index >= Runtime::thread_count) {
        return;
    }
    if (Runtime::current_thread_index() == state->control_thread_index) {
        handle_tcp_client_connect_result_on_control<Runtime>(state, control, connect, connected);
        return;
    }
    try {
        static_cast<void>(Runtime::template start_task<TcpClientConnectResultTask<Runtime>>(
            std::move(state), std::move(control), std::move(connect), connected));
    } catch (...) {
    }
}

template <typename Runtime>
void complete_tcp_client_stop_from_shard(
    std::shared_ptr<TcpServerState<Runtime>> state,
    std::shared_ptr<TcpClientControlState<Runtime>> control) noexcept {
    if (state == nullptr || control == nullptr ||
        state->control_thread_index >= Runtime::thread_count) {
        return;
    }
    if (Runtime::current_thread_index() == state->control_thread_index) {
        handle_tcp_client_stop_result_on_control<Runtime>(state, control);
        return;
    }
    try {
        static_cast<void>(Runtime::template start_task<TcpClientStopResultTask<Runtime>>(
            std::move(state), std::move(control)));
    } catch (...) {
    }
}

} // namespace af::net::detail
