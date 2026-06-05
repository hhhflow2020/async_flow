#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "af/buffer/buffer.hpp"
#include "af/detail/net/reactor/net_io_channel.hpp"
#include "af/net/detail/tcp_connection.hpp"
#include "af/net/detail/tcp_listener_shard.hpp"
#include "af/net/detail/tcp_state.hpp"
#include "af/net/tcp_types.hpp"

#include <sys/socket.h>

namespace af::net::detail {

template <typename Runtime> class TcpServerShard {
public:
    using State = TcpServerState<Runtime>;
    using Thread = typename Runtime::Thread;
    using Connection = TcpConnection<Runtime>;
    using ListenerContext = TcpListenerContext<Runtime>;
    using ListenerShard = TcpListenerShard<Runtime>;

    struct ConnectionSlot {
        std::uint32_t index{0};
        std::uint32_t generation{1};
    };

    TcpServerShard(std::weak_ptr<State> state, std::uint16_t shard_index, Thread thread);

    TcpServerShard(const TcpServerShard &) = delete;
    TcpServerShard &operator=(const TcpServerShard &) = delete;

    ~TcpServerShard() = default;

    [[nodiscard]] Thread thread() const noexcept;
    [[nodiscard]] std::weak_ptr<State> weak_state() const noexcept;

    [[nodiscard]] SendResult send_to(std::uint32_t slot, std::uint32_t generation,
                                     af::Buffer buffer) noexcept;
    [[nodiscard]] SendResult send_to(std::uint32_t slot, std::uint32_t generation,
                                     af::BufferView view) noexcept;
    [[nodiscard]] bool close_connection(std::uint32_t slot, std::uint32_t generation) noexcept;
    [[nodiscard]] bool close_connection_after_flush(std::uint32_t slot,
                                                    std::uint32_t generation) noexcept;
    [[nodiscard]] bool shutdown_connection_write(std::uint32_t slot,
                                                 std::uint32_t generation) noexcept;
    [[nodiscard]] bool pause_connection_read(std::uint32_t slot, std::uint32_t generation) noexcept;
    [[nodiscard]] bool resume_connection_read(std::uint32_t slot,
                                              std::uint32_t generation) noexcept;
    [[nodiscard]] bool set_connection_no_delay(std::uint32_t slot, std::uint32_t generation,
                                               bool enabled) noexcept;
    [[nodiscard]] bool set_connection_keepalive(std::uint32_t slot, std::uint32_t generation,
                                                bool enabled) noexcept;

    [[nodiscard]] int add_listener_on_owner(std::uint32_t listener_slot,
                                            std::shared_ptr<ListenerContext> context,
                                            bool open_listener) noexcept;
    void remove_listener_on_owner(ListenerId id, RemoveListenerPolicy policy) noexcept;
    void close_listeners_on_owner() noexcept;
    [[nodiscard]] std::size_t close_connections_after_flush_on_owner() noexcept;
    void force_close_connections_on_owner() noexcept;
    void stop_on_owner() noexcept;

    [[nodiscard]] bool create_connection(std::shared_ptr<ListenerContext> context, int fd,
                                         const sockaddr *peer, socklen_t peer_size) noexcept;
    [[nodiscard]] bool route_connection(std::shared_ptr<ListenerContext> context, int fd,
                                        const sockaddr *peer, socklen_t peer_size) noexcept;

private:
    friend class TcpConnection<Runtime>;
    friend class TcpListenerShard<Runtime>;
    friend class TcpConnectionHandle<Runtime>;
    friend class TcpAdoptConnectionTask<Runtime>;
    friend class TcpConnectionCommandTask<Runtime>;

    [[nodiscard]] Connection *find(std::uint32_t slot, std::uint32_t generation) noexcept;
    [[nodiscard]] bool try_acquire_connection_slot(ConnectionSlot &slot) noexcept;
    [[nodiscard]] ConnectionSlot acquire_connection_slot();
    void release_unused_connection_slot(ConnectionSlot slot) noexcept;
    void retire_connection(std::uint32_t slot, std::uint32_t generation) noexcept;
    void reap_retired_connections() noexcept;
    void reap_retired_connections_if_safe() noexcept;
    [[nodiscard]] std::size_t alive_connection_count() const noexcept;
    void begin_user_callback() noexcept;
    void end_user_callback() noexcept;
    void trim_empty_tail_slots() noexcept;
    void erase_free_connection_slot(std::uint32_t slot) noexcept;
    [[nodiscard]] std::shared_ptr<ListenerContext> find_listener_context(ListenerId id) noexcept;
    [[nodiscard]] bool schedule_adopt_connection(std::uint16_t target_shard, ListenerId listener_id,
                                                 int fd, const sockaddr *peer,
                                                 socklen_t peer_size) noexcept;
    void adopt_connection(ListenerId listener_id, int fd, const sockaddr *peer,
                          socklen_t peer_size) noexcept;
    static void notify_listener_error(const std::shared_ptr<ListenerContext> &context,
                                      int error) noexcept;

    std::weak_ptr<State> state_;
    std::uint16_t shard_index_{0};
    Thread thread_;
    std::vector<std::unique_ptr<ListenerShard>> listeners_;
    std::vector<std::unique_ptr<Connection>> connections_;
    std::vector<std::uint32_t> generations_;
    std::vector<ConnectionSlot> retired_connection_slots_;
    std::vector<std::uint32_t> free_connection_slots_;
    std::uint32_t user_callback_depth_{0};
};

} // namespace af::net::detail

#include "af/net/detail/tcp_server_shard_impl.hpp"
