#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "af/buffer/buffer.hpp"
#include "af/net/detail/socket_address.hpp"
#include "af/net/endpoint.hpp"
#include "af/net/udp_types.hpp"
#include "af/runtime.hpp"

namespace af::net {

class udp_socket;
class udp_socket_handle;
class udp_socket_ref;

using udp_datagram_callback = void (*)(void *owner, udp_socket_ref socket, af::buffer_view bytes,
                                       const udp_peer &peer) noexcept;
using udp_error_callback = void (*)(void *owner, udp_socket_handle socket, int error) noexcept;

struct udp_socket_callbacks {
    void *owner{nullptr};
    udp_datagram_callback on_datagram{nullptr};
    udp_error_callback on_error{nullptr};
};

struct udp_socket_config {
    std::string name;
    udp_endpoint local_endpoint = udp_endpoint::any(0);
    udp_endpoint remote_endpoint;
    std::vector<af::thread_ref> threads;
    udp_socket_options options;
    bool connect_remote{false};
};

namespace detail {

struct runtime_udp_shard;

struct runtime_udp_state {
    explicit runtime_udp_state(af::runtime &runtime_owner) noexcept : owner(&runtime_owner) {}

    af::runtime *owner{nullptr};
    std::vector<std::shared_ptr<runtime_udp_shard>> shards;
    std::atomic<bool> accepting_operations{false};
};

} // namespace detail

class udp_socket_handle {
public:
    udp_socket_handle() noexcept = default;

    [[nodiscard]] bool valid() const noexcept {
        return owner_thread_.valid() && generation_ != 0U;
    }

    [[nodiscard]] af::thread_ref owner_thread() const noexcept {
        return owner_thread_;
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return generation_;
    }

    [[nodiscard]] udp_send_result send(af::buffer buffer) const noexcept;
    [[nodiscard]] udp_send_result send(af::buffer_view view) const noexcept;
    [[nodiscard]] udp_send_result send_to(af::buffer buffer, udp_endpoint endpoint) const noexcept;
    [[nodiscard]] udp_send_result send_to(af::buffer_view view,
                                          udp_endpoint endpoint) const noexcept;
    [[nodiscard]] udp_send_result send_to(af::buffer buffer, const udp_peer &peer) const noexcept;
    [[nodiscard]] udp_send_result send_to(af::buffer_view view,
                                          const udp_peer &peer) const noexcept;

private:
    friend class udp_socket;
    friend struct detail::runtime_udp_shard;

    udp_socket_handle(std::weak_ptr<detail::runtime_udp_state> state, af::thread_ref owner_thread,
                      std::uint32_t generation) noexcept
        : state_(std::move(state)), owner_thread_(owner_thread), generation_(generation) {}

    [[nodiscard]] std::shared_ptr<detail::runtime_udp_shard>
    lock_shard(const std::shared_ptr<detail::runtime_udp_state> &state) const noexcept;

    std::weak_ptr<detail::runtime_udp_state> state_;
    af::thread_ref owner_thread_{};
    std::uint32_t generation_{0};
};

class udp_socket_ref {
public:
    explicit udp_socket_ref(detail::runtime_udp_shard *shard = nullptr) noexcept : shard_(shard) {}

    [[nodiscard]] bool valid() const noexcept;
    [[nodiscard]] udp_socket_handle handle() const noexcept;
    [[nodiscard]] af::thread_ref owner_thread() const noexcept;
    [[nodiscard]] const udp_endpoint &local_endpoint() const noexcept;
    [[nodiscard]] udp_send_result send(af::buffer buffer) const noexcept;
    [[nodiscard]] udp_send_result send(af::buffer_view view) const noexcept;
    [[nodiscard]] udp_send_result send_to(af::buffer buffer, udp_endpoint endpoint) const noexcept;
    [[nodiscard]] udp_send_result send_to(af::buffer_view view,
                                          udp_endpoint endpoint) const noexcept;
    [[nodiscard]] udp_send_result send_to(af::buffer buffer, const udp_peer &peer) const noexcept;
    [[nodiscard]] udp_send_result send_to(af::buffer_view view,
                                          const udp_peer &peer) const noexcept;

private:
    detail::runtime_udp_shard *shard_{nullptr};
};

class udp_socket {
public:
    explicit udp_socket(af::runtime &owner);
    udp_socket(const udp_socket &) = delete;
    udp_socket &operator=(const udp_socket &) = delete;
    ~udp_socket();

    [[nodiscard]] bool start(udp_socket_config config, udp_socket_callbacks callbacks) noexcept;
    [[nodiscard]] bool stop() noexcept;

    [[nodiscard]] bool running() const noexcept {
        return running_;
    }

    [[nodiscard]] std::size_t active_shard_count() const noexcept;
    [[nodiscard]] udp_socket_handle handle() const noexcept;
    [[nodiscard]] udp_socket_handle handle_for_thread(af::thread_ref thread) const noexcept;
    [[nodiscard]] std::vector<udp_socket_handle> handles() const;
    [[nodiscard]] const udp_endpoint *local_endpoint(af::thread_ref thread) const noexcept;

private:
    friend class udp_socket_handle;
    friend class udp_socket_ref;
    friend struct detail::runtime_udp_shard;

    [[nodiscard]] bool on_runtime_io_thread() const noexcept;
    [[nodiscard]] bool normalize_config(udp_socket_config &config) const;
    [[nodiscard]] int validate_config(const udp_socket_config &config) const noexcept;
    [[nodiscard]] bool start_shard_on_owner(detail::runtime_udp_shard &shard,
                                            const udp_socket_config &config,
                                            udp_socket_callbacks callbacks) noexcept;
    void stop_shard_on_owner(detail::runtime_udp_shard &shard) noexcept;

    [[nodiscard]] udp_send_result send_on_owner(af::thread_ref thread, std::uint32_t generation,
                                                af::buffer buffer) noexcept;
    [[nodiscard]] udp_send_result send_on_owner(af::thread_ref thread, std::uint32_t generation,
                                                af::buffer_view view) noexcept;
    [[nodiscard]] udp_send_result
    send_to_on_owner(af::thread_ref thread, std::uint32_t generation, af::buffer buffer,
                     const af::detail::socket_address &address) noexcept;
    [[nodiscard]] udp_send_result
    send_to_on_owner(af::thread_ref thread, std::uint32_t generation, af::buffer_view view,
                     const af::detail::socket_address &address) noexcept;

    [[nodiscard]] std::shared_ptr<detail::runtime_udp_shard>
    shard_for_thread(af::thread_ref thread) const noexcept;

    std::shared_ptr<detail::runtime_udp_state> state_;
    bool running_{false};
};

} // namespace af::net

#include "af/net/detail/udp_socket_runtime_impl.hpp"
