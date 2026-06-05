#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "af/detail/config.hpp"
#include "af/net/detail/tcp_handler.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_types.hpp"

namespace af::net::detail {

template <typename Runtime> class TcpConnection;
template <typename Runtime> class TcpServerShard;
template <typename Runtime> class TcpListenerShard;
template <typename Runtime> struct TcpListenerContext;
template <typename Runtime> struct TcpListenerEntry;
template <typename Runtime> struct TcpServerState;
template <typename Runtime> class TcpAdoptConnectionTask;
template <typename Runtime> class TcpConnectionCommandTask;

enum class TcpConnectionCommandKind : std::uint8_t {
    Send,
    Close,
    CloseAfterFlush,
    ShutdownWrite,
    PauseRead,
    ResumeRead,
    SetNoDelay,
    SetKeepAlive,
};

template <typename Runtime> struct TcpListenerContext {
    ListenerId id{};
    std::string name;
    TcpEndpoint endpoint;
    TcpListenerOptions options;
    std::vector<std::uint16_t> target_shards;
    std::uint32_t next_target_shard{0};
    std::unique_ptr<TcpHandlerBase<Runtime>> handler;
};

template <typename Runtime> struct TcpListenerEntry {
    using Thread = typename Runtime::Thread;

    ListenerId id{};
    std::string name;
    TcpEndpoint endpoint;
    TcpListenerOptions options;
    AcceptStrategy accept_strategy{AcceptStrategy::Auto};
    std::vector<Thread> threads;
    std::vector<std::uint16_t> active_shards;
    std::vector<std::uint16_t> starting_shards;
    std::vector<std::uint16_t> started_shards;
    std::unique_ptr<TcpHandlerBase<Runtime>> handler_prototype;
    ListenerState state{ListenerState::Configured};
    std::size_t pending_start_shards{0};
    int start_error{0};
};

template <typename Runtime> struct TcpServerState {
    using Thread = typename Runtime::Thread;
    using Shard = TcpServerShard<Runtime>;
    using ListenerEntry = TcpListenerEntry<Runtime>;

    TcpServerConfig config;
    std::uint16_t control_thread_index{Runtime::invalid_thread_index};
    bool running{false};
    alignas(af::detail::hardware_cache_line_size) std::atomic<bool> accepting_connection_tasks{
        false};
    std::vector<Thread> default_threads;
    std::vector<std::unique_ptr<Shard>> shards;
    std::vector<std::unique_ptr<ListenerEntry>> listeners;
    std::vector<std::uint32_t> listener_generations;
    std::vector<std::uint32_t> free_listener_slots;
};

} // namespace af::net::detail
