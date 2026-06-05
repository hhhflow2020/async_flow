#pragma once

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/async_runtime.hpp"
#include "af/detail/config.hpp"
#include "af/net/detail/tcp_control_tasks.hpp"
#include "af/net/detail/tcp_handler.hpp"
#include "af/net/detail/tcp_server_shard.hpp"
#include "af/net/detail/tcp_state.hpp"
#include "af/net/tcp_connection.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"
#include "af/net/thread_list.hpp"
#include "af/thread_kind.hpp"

namespace af::net {

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
                    RemoveListenerPolicy policy = RemoveListenerPolicy::StopAcceptOnly);

    [[nodiscard]] bool start();

    [[nodiscard]] bool stop();

    [[nodiscard]] std::shared_ptr<State> state() const noexcept {
        return state_;
    }

private:
    static constexpr bool is_io_thread(Thread thread) noexcept;
    [[nodiscard]] static std::uint16_t first_io_thread_index() noexcept;
    [[nodiscard]] static TcpServerConfig normalize_config(TcpServerConfig config) noexcept;
    [[nodiscard]] TcpListenerOptions
    normalize_listener_options(TcpListenerOptions options) const noexcept;
    void init_shards();
    [[nodiscard]] ListenerId acquire_listener_slot();
    void release_listener_slot(std::uint32_t slot) noexcept;
    [[nodiscard]] bool listener_slot_is_free(std::uint32_t slot) const noexcept;
    void trim_empty_listener_tail() noexcept;
    void erase_free_listener_slot(std::uint32_t slot) noexcept;
    [[nodiscard]] ListenerResult
    add_listener_impl(ListenerConfig config,
                      std::unique_ptr<detail::TcpHandlerBase<Runtime>> prototype);
    [[nodiscard]] int validate_config(const ListenerConfig &config) const noexcept;
    [[nodiscard]] std::vector<std::uint16_t>
    listener_open_shards(const typename State::ListenerEntry &entry) const;
    [[nodiscard]] std::vector<std::uint16_t>
    listener_install_shards(const typename State::ListenerEntry &entry) const;
    [[nodiscard]] ListenerResult start_listener_slot(std::uint32_t slot);
    void mark_listener_failed(std::uint32_t slot, int error);

    std::shared_ptr<State> state_;
};

} // namespace af::net

#include "af/net/detail/tcp_server_impl.hpp"

namespace af::net {

using send_result = SendResult;
using close_reason = CloseReason;
using accept_strategy = AcceptStrategy;
using listener_state = ListenerState;
using remove_listener_policy = RemoveListenerPolicy;
using tcp_listener_options = TcpListenerOptions;
using tcp_connection_config = TcpConnectionConfig;
using tcp_listener_config = TcpListenerConfig;
using tcp_server_config = TcpServerConfig;
using listener_id = ListenerId;
using tcp_listener_handle = TcpListenerHandle;
using listener_result = ListenerResult;

template <typename Runtime> using tcp_connection_handle = TcpConnectionHandle<Runtime>;
template <typename Runtime> using tcp_connection_ref = TcpConnectionRef<Runtime>;
template <typename Runtime> using tcp_server = TcpServer<Runtime>;

} // namespace af::net
