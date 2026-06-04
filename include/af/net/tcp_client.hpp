#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/io_socket.hpp"
#include "af/io_timeout.hpp"
#include "af/net/tcp_server.hpp"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace af::net {

struct TcpClientOptions {
    TcpListenerOptions connection;
    bool no_delay{true};
    bool keep_alive{false};
};

struct TcpClientRuntimeConfig {};

template <typename Runtime> class TcpClient;

namespace detail {

template <typename Handler, typename Runtime, typename = void>
struct TcpClientHandlerHasOnConnect : std::false_type {};
template <typename Handler, typename Runtime>
struct TcpClientHandlerHasOnConnect<Handler, Runtime,
                                    std::void_t<decltype(std::declval<Handler &>().on_connect(
                                        std::declval<TcpConnectionRef<Runtime>>()))>>
    : std::true_type {};

template <typename Handler, typename = void>
struct TcpClientHandlerHasOnConnectError : std::false_type {};
template <typename Handler>
struct TcpClientHandlerHasOnConnectError<
    Handler, std::void_t<decltype(std::declval<Handler &>().on_connect_error(std::declval<int>()))>>
    : std::true_type {};

template <typename Handler, typename = void> struct TcpClientHandlerHasOnError : std::false_type {};
template <typename Handler>
struct TcpClientHandlerHasOnError<
    Handler, std::void_t<decltype(std::declval<Handler &>().on_error(std::declval<int>()))>>
    : std::true_type {};

template <typename Runtime, typename Handler>
class TcpClientHandlerModel final : public TcpHandlerBase<Runtime> {
public:
    explicit TcpClientHandlerModel(Handler handler) : handler_(std::move(handler)) {}

    [[nodiscard]] std::unique_ptr<TcpHandlerBase<Runtime>> clone() const override {
        return std::make_unique<TcpClientHandlerModel>(handler_);
    }

    void on_accept(TcpConnectionRef<Runtime> conn) noexcept override {
        if constexpr (TcpClientHandlerHasOnConnect<Handler, Runtime>::value) {
            try {
                handler_.on_connect(conn);
            } catch (...) {
                conn.close(CloseReason::Error);
            }
        } else if constexpr (TcpHandlerHasOnAccept<Handler, Runtime>::value) {
            try {
                handler_.on_accept(conn);
            } catch (...) {
                conn.close(CloseReason::Error);
            }
        } else {
            static_cast<void>(conn);
        }
    }

    void on_read(TcpConnectionRef<Runtime> conn, af::BufferView bytes) noexcept override {
        if constexpr (TcpHandlerHasOnRead<Handler, Runtime>::value) {
            try {
                handler_.on_read(conn, bytes);
            } catch (...) {
                conn.close(CloseReason::Error);
            }
        } else {
            static_cast<void>(conn);
            static_cast<void>(bytes);
        }
    }

    void on_close(TcpConnectionHandle<Runtime> conn, CloseReason reason) noexcept override {
        if constexpr (TcpHandlerHasOnClose<Handler, Runtime>::value) {
            try {
                handler_.on_close(conn, reason);
            } catch (...) {
            }
        } else {
            static_cast<void>(conn);
            static_cast<void>(reason);
        }
    }

    void on_listener_error(TcpListenerHandle listener, int error) noexcept override {
        if constexpr (TcpClientHandlerHasOnConnectError<Handler>::value) {
            try {
                handler_.on_connect_error(error);
            } catch (...) {
            }
        } else if constexpr (TcpClientHandlerHasOnError<Handler>::value) {
            try {
                handler_.on_error(error);
            } catch (...) {
            }
        } else if constexpr (TcpHandlerHasOnListenerErrorAlias<Handler>::value) {
            try {
                handler_.on_error(listener, error);
            } catch (...) {
            }
        } else {
            static_cast<void>(listener);
            static_cast<void>(error);
        }
    }

private:
    Handler handler_;
};

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

} // namespace detail

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

using tcp_client_options = TcpClientOptions;
using tcp_client_runtime_config = TcpClientRuntimeConfig;

template <typename Runtime> using tcp_client = TcpClient<Runtime>;

} // namespace af::net
