#pragma once

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <new>
#include <utility>

#include "af/net/detail/udp_socket_ops.hpp"

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

namespace af::net {

namespace detail {

inline void runtime_udp_unlink_unix_path(const udp_endpoint &endpoint) noexcept {
    if (endpoint.family == address_family::unix_domain && !endpoint.address.empty()) {
        static_cast<void>(::unlink(endpoint.address.c_str()));
    }
}

struct runtime_udp_shard {
    explicit runtime_udp_shard(std::shared_ptr<runtime_udp_state> shared_state,
                               af::thread_ref thread) noexcept
        : state(std::move(shared_state)), owner_thread(thread) {
        source.owner = this;
        source.on_event = &runtime_udp_shard::on_event;
    }

    runtime_udp_shard(const runtime_udp_shard &) = delete;
    runtime_udp_shard &operator=(const runtime_udp_shard &) = delete;

    ~runtime_udp_shard() {
        detail::udp_close_fd(fd);
    }

    [[nodiscard]] bool active() const noexcept {
        return active_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint32_t generation() const noexcept {
        return generation_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool matches_generation(std::uint32_t expected) const noexcept {
        return expected != 0U && active() && generation() == expected;
    }

    [[nodiscard]] bool
    supports_peer_address(const af::detail::SocketAddress &address) const noexcept {
        return address.size != 0U &&
               address.family == socket_family.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool resolve_endpoint(udp_endpoint endpoint,
                                        af::detail::SocketAddress &address) const noexcept {
        int address_error = 0;
        return af::detail::socket_address_from_endpoint(std::move(endpoint), address,
                                                        address_error) &&
               supports_peer_address(address);
    }

    [[nodiscard]] udp_socket_handle handle() const noexcept {
        return udp_socket_handle(state, owner_thread, generation());
    }

    [[nodiscard]] udp_send_result send(af::Buffer buffer) noexcept {
        if (!connect_remote) {
            return udp_send_result::unsupported;
        }
        return send_impl(buffer.view(), nullptr);
    }

    [[nodiscard]] udp_send_result send(af::BufferView view) noexcept {
        if (!connect_remote) {
            return udp_send_result::unsupported;
        }
        return send_impl(view, nullptr);
    }

    [[nodiscard]] udp_send_result send_to(af::Buffer buffer,
                                          const af::detail::SocketAddress &address) noexcept {
        return send_impl(buffer.view(), &address);
    }

    [[nodiscard]] udp_send_result send_to(af::BufferView view,
                                          const af::detail::SocketAddress &address) noexcept {
        return send_impl(view, &address);
    }

    [[nodiscard]] udp_send_result send_to(af::Buffer buffer, const udp_peer &peer) noexcept {
        return send_to(std::move(buffer), peer.socket_address());
    }

    [[nodiscard]] udp_send_result send_to(af::BufferView view, const udp_peer &peer) noexcept {
        return send_to(view, peer.socket_address());
    }

    static void on_event(void *owner, af::fd_event_source &source, std::uint32_t events) noexcept {
        static_cast<void>(source);
        auto *shard = static_cast<runtime_udp_shard *>(owner);
        if (shard != nullptr) {
            shard->handle_events(events);
        }
    }

    void handle_events(std::uint32_t events) noexcept {
        if ((events & af::reactor_error) != 0U) {
            notify_error(EIO);
            return;
        }
        if ((events & af::reactor_readable) != 0U) {
            read_available();
        }
    }

    void read_available() noexcept {
        if (fd < 0 || callbacks.on_datagram == nullptr) {
            return;
        }
        const std::size_t buffer_size =
            std::max(options.receive_buffer_size, options.max_datagram_size);
        if (read_buffer.size() < buffer_size) {
            try {
                read_buffer.resize(buffer_size);
            } catch (...) {
                notify_error(ENOMEM);
                return;
            }
        }

        std::size_t received = 0;
        while (fd >= 0 && received < options.read_budget_datagrams) {
            sockaddr_storage peer_storage{};
            iovec iov{};
            iov.iov_base = read_buffer.data();
            iov.iov_len = read_buffer.size();

            msghdr message{};
            message.msg_name = &peer_storage;
            message.msg_namelen = sizeof(peer_storage);
            message.msg_iov = &iov;
            message.msg_iovlen = 1;

            const ssize_t n = ::recvmsg(fd, &message, 0);
            if (n >= 0) {
                ++received;
                const std::size_t size = static_cast<std::size_t>(n);
                if (size > options.max_datagram_size ||
                    (message.msg_flags & MSG_TRUNC) == MSG_TRUNC) {
                    notify_error(EMSGSIZE);
                    continue;
                }
                udp_peer peer(reinterpret_cast<const sockaddr *>(&peer_storage),
                              message.msg_namelen);
                callbacks.on_datagram(callbacks.owner, udp_socket_ref(this),
                                      af::BufferView(read_buffer.data(), size), peer);
                continue;
            }

            const int error = errno == 0 ? EIO : errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN || error == EWOULDBLOCK) {
                return;
            }
            notify_error(error);
            return;
        }
    }

    [[nodiscard]] udp_send_result send_impl(af::BufferView view,
                                            const af::detail::SocketAddress *address) noexcept {
        if (fd < 0 || !active()) {
            return udp_send_result::closed;
        }
        if (view.size() > options.max_datagram_size) {
            return udp_send_result::backpressure;
        }

        const sockaddr *raw_address = nullptr;
        socklen_t raw_size = 0;
        if (address != nullptr) {
            if (!supports_peer_address(*address)) {
                return udp_send_result::unsupported;
            }
            raw_address = reinterpret_cast<const sockaddr *>(&address->storage);
            raw_size = address->size;
        } else if (!connect_remote) {
            return udp_send_result::unsupported;
        }

        for (;;) {
            const ssize_t n = address == nullptr ? ::send(fd, view.data(), view.size(), 0)
                                                 : ::sendto(fd, view.data(), view.size(), 0,
                                                            raw_address, raw_size);
            if (n == static_cast<ssize_t>(view.size())) {
                return udp_send_result::accepted;
            }
            if (n >= 0) {
                return udp_send_result::backpressure;
            }
            const int error = errno == 0 ? EIO : errno;
            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN || error == EWOULDBLOCK || error == ENOBUFS) {
                return udp_send_result::backpressure;
            }
            if (error == EBADF || error == ENOTCONN) {
                return udp_send_result::closed;
            }
            notify_error(error);
            return udp_send_result::closed;
        }
    }

    void notify_error(int error) noexcept {
        if (callbacks.on_error != nullptr) {
            callbacks.on_error(callbacks.owner, handle(), error == 0 ? EIO : error);
        }
    }

    void publish_generation_on_owner() noexcept {
        std::uint32_t next = generation_.load(std::memory_order_relaxed) + 1U;
        if (next == 0U) {
            next = 1U;
        }
        generation_.store(next, std::memory_order_release);
    }

    std::shared_ptr<runtime_udp_state> state;
    af::thread_ref owner_thread{};
    int fd{-1};
    af::fd_event_source source{};
    udp_socket_callbacks callbacks{};
    udp_socket_options options{};
    udp_endpoint local_endpoint{};
    udp_endpoint remote_endpoint{};
    af::detail::SocketAddress remote_address{};
    std::vector<std::byte> read_buffer;
    std::atomic<int> socket_family{AF_UNSPEC};
    alignas(af::detail::hardware_cache_line_size) std::atomic<std::uint32_t> generation_{0};
    std::atomic<bool> active_{false};
    bool registered{false};
    bool connect_remote{false};
};

[[nodiscard]] inline bool
runtime_udp_start_shard_on_owner(const std::shared_ptr<runtime_udp_state> &state,
                                 runtime_udp_shard &shard, const udp_socket_config &config,
                                 udp_socket_callbacks callbacks) noexcept {
    if (state == nullptr || state->owner == nullptr || af::runtime::current() != state->owner ||
        af::runtime::current_thread_index() != shard.owner_thread.index || shard.active()) {
        return false;
    }

    af::detail::SocketAddress local{};
    int address_error = 0;
    if (!af::detail::socket_address_from_endpoint(config.local_endpoint, local, address_error)) {
        return false;
    }

    if (local.family == AF_UNIX && config.options.unlink_existing_unix_path) {
        runtime_udp_unlink_unix_path(config.local_endpoint);
    }

    int fd = ::socket(local.family, SOCK_DGRAM, 0);
    if (fd < 0) {
        if (callbacks.on_error != nullptr) {
            callbacks.on_error(callbacks.owner, shard.handle(), errno == 0 ? EIO : errno);
        }
        return false;
    }
    if (!detail::udp_set_nonblocking(fd) || !detail::udp_set_cloexec(fd)) {
        detail::udp_close_fd(fd);
        return false;
    }

    int one = 1;
    if (local.family != AF_UNIX) {
        static_cast<void>(::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)));
#if defined(SO_REUSEPORT)
        if (config.options.reuse_port) {
            static_cast<void>(::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)));
        }
#endif
        if (local.family == AF_INET6) {
            const int v6_only = config.options.ipv6_only ? 1 : 0;
            static_cast<void>(
                ::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6_only, sizeof(v6_only)));
        }
    }

    if (::bind(fd, reinterpret_cast<const sockaddr *>(&local.storage), local.size) != 0) {
        const int error = errno == 0 ? EADDRNOTAVAIL : errno;
        detail::udp_close_fd(fd);
        if (callbacks.on_error != nullptr) {
            callbacks.on_error(callbacks.owner, shard.handle(), error);
        }
        return false;
    }

    af::detail::SocketAddress remote{};
    if (config.connect_remote) {
        if (!af::detail::socket_address_from_endpoint(config.remote_endpoint, remote,
                                                      address_error) ||
            ::connect(fd, reinterpret_cast<const sockaddr *>(&remote.storage), remote.size) != 0) {
            const int error = errno == 0 ? EIO : errno;
            detail::udp_close_fd(fd);
            if (local.family == AF_UNIX && config.options.unlink_unix_path_on_close) {
                runtime_udp_unlink_unix_path(config.local_endpoint);
            }
            if (callbacks.on_error != nullptr) {
                callbacks.on_error(callbacks.owner, shard.handle(), error);
            }
            return false;
        }
    }

    udp_endpoint local_endpoint = config.local_endpoint;
    af::detail::SocketAddress actual_local{};
    socklen_t actual_size = sizeof(actual_local.storage);
    if (::getsockname(fd, reinterpret_cast<sockaddr *>(&actual_local.storage), &actual_size) == 0) {
        actual_local.size = actual_size;
        local_endpoint = af::detail::endpoint_from_socket_address(
            reinterpret_cast<const sockaddr *>(&actual_local.storage), actual_local.size);
    }

    shard.fd = fd;
    shard.source.fd = fd;
    shard.source.interests = af::reactor_readable;
    shard.callbacks = callbacks;
    shard.options = config.options;
    shard.local_endpoint = std::move(local_endpoint);
    shard.remote_endpoint = config.remote_endpoint;
    shard.remote_address = remote;
    shard.connect_remote = config.connect_remote;
    shard.socket_family.store(local.family, std::memory_order_release);
    shard.publish_generation_on_owner();
    if (!state->owner->register_reactor_source(shard.owner_thread, &shard.source)) {
        detail::udp_close_fd(shard.fd);
        if (local.family == AF_UNIX && config.options.unlink_unix_path_on_close) {
            runtime_udp_unlink_unix_path(config.local_endpoint);
        }
        shard.socket_family.store(AF_UNSPEC, std::memory_order_release);
        return false;
    }
    shard.registered = true;
    shard.active_.store(true, std::memory_order_release);
    return true;
}

inline void runtime_udp_stop_shard_on_owner(const std::shared_ptr<runtime_udp_state> &state,
                                            runtime_udp_shard &shard) noexcept {
    if (state == nullptr || state->owner == nullptr || af::runtime::current() != state->owner ||
        af::runtime::current_thread_index() != shard.owner_thread.index) {
        return;
    }
    shard.active_.store(false, std::memory_order_release);
    if (shard.registered) {
        static_cast<void>(
            state->owner->unregister_reactor_source(shard.owner_thread, &shard.source));
        shard.registered = false;
    }
    const udp_endpoint local_endpoint = shard.local_endpoint;
    const bool unlink_path = shard.socket_family.load(std::memory_order_acquire) == AF_UNIX &&
                             shard.options.unlink_unix_path_on_close;
    detail::udp_close_fd(shard.fd);
    if (unlink_path) {
        runtime_udp_unlink_unix_path(local_endpoint);
    }
    shard.socket_family.store(AF_UNSPEC, std::memory_order_release);
    shard.callbacks = {};
    shard.connect_remote = false;
    shard.read_buffer.clear();
}

} // namespace detail

inline udp_socket::udp_socket(af::runtime &owner)
    : state_(std::make_shared<detail::runtime_udp_state>(owner)) {
    state_->shards.reserve(owner.thread_count());
    for (af::runtime::thread_index i = 0; i < owner.thread_count(); ++i) {
        state_->shards.push_back(
            std::make_shared<detail::runtime_udp_shard>(state_, af::thread_ref(i)));
    }
}

inline udp_socket::~udp_socket() {
    if (state_ != nullptr) {
        state_->accepting_operations.store(false, std::memory_order_release);
    }
    if (running_) {
        AF_ASSERT(on_runtime_io_thread() &&
                  "udp_socket must be stopped on a runtime IO thread before destruction");
        if (on_runtime_io_thread()) {
            static_cast<void>(stop());
        }
    }
}

inline bool udp_socket::start(udp_socket_config config, udp_socket_callbacks callbacks) noexcept {
    if (running_ || !normalize_config(config)) {
        return false;
    }
    if (validate_config(config) != 0) {
        return false;
    }

    running_ = true;
    state_->accepting_operations.store(true, std::memory_order_release);

    bool ok = true;
    for (const af::thread_ref thread : config.threads) {
        auto shard = shard_for_thread(thread);
        if (shard == nullptr) {
            ok = false;
            continue;
        }
        if (af::runtime::current() == state_->owner &&
            af::runtime::current_thread_index() == thread.index) {
            ok = start_shard_on_owner(*shard, config, callbacks) && ok;
            continue;
        }
        try {
            const bool posted = state_->owner->post(thread, [shard, config, callbacks]() mutable {
                if (shard == nullptr || shard->state == nullptr ||
                    !shard->state->accepting_operations.load(std::memory_order_acquire)) {
                    return;
                }
                static_cast<void>(detail::runtime_udp_start_shard_on_owner(shard->state, *shard,
                                                                           config, callbacks));
            });
            if (!posted) {
                ok = false;
            }
        } catch (...) {
            ok = false;
        }
    }

    if (!ok) {
        static_cast<void>(stop());
    }
    return ok;
}

inline bool udp_socket::stop() noexcept {
    if (!running_) {
        return false;
    }
    if (state_ == nullptr || state_->owner == nullptr || af::runtime::current() != state_->owner ||
        !state_->owner->valid_thread(af::runtime::current_thread_index()) ||
        state_->owner->thread_kind_of(af::runtime::current_thread_index()) != af::thread_kind::io) {
        return false;
    }

    state_->accepting_operations.store(false, std::memory_order_release);
    running_ = false;
    bool ok = true;
    for (const auto &shard : state_->shards) {
        if (shard == nullptr || !shard->active()) {
            continue;
        }
        if (af::runtime::current_thread_index() == shard->owner_thread.index) {
            stop_shard_on_owner(*shard);
            continue;
        }
        try {
            const bool posted = state_->owner->post(shard->owner_thread, [shard]() mutable {
                if (shard != nullptr) {
                    detail::runtime_udp_stop_shard_on_owner(shard->state, *shard);
                }
            });
            ok = posted && ok;
        } catch (...) {
            ok = false;
        }
    }
    return ok;
}

inline bool udp_socket::on_runtime_io_thread() const noexcept {
    if (state_ == nullptr || state_->owner == nullptr || af::runtime::current() != state_->owner) {
        return false;
    }
    const af::runtime::thread_index index = af::runtime::current_thread_index();
    return state_->owner->valid_thread(index) &&
           state_->owner->thread_kind_of(index) == af::thread_kind::io;
}

inline bool udp_socket::normalize_config(udp_socket_config &config) const {
    if (!on_runtime_io_thread()) {
        return false;
    }
    if (config.threads.empty()) {
        const af::thread_group_ref io_threads = state_->owner->io_threads();
        try {
            config.threads.reserve(io_threads.size());
            for (std::uint16_t thread : io_threads) {
                config.threads.push_back(af::thread_ref(thread));
            }
        } catch (...) {
            return false;
        }
    }
    return true;
}

inline int udp_socket::validate_config(const udp_socket_config &config) const noexcept {
    if (state_ == nullptr || state_->owner == nullptr || config.threads.empty()) {
        return EINVAL;
    }
    if (config.options.read_budget_datagrams == 0U || config.options.receive_buffer_size == 0U ||
        config.options.max_datagram_size == 0U) {
        return EINVAL;
    }

    af::detail::SocketAddress local{};
    int address_error = 0;
    if (!af::detail::socket_address_from_endpoint(config.local_endpoint, local, address_error)) {
        return address_error == 0 ? EINVAL : address_error;
    }
    if (local.family == AF_UNIX && config.threads.size() > 1U) {
        return EINVAL;
    }
    if (config.threads.size() > 1U && !config.options.reuse_port) {
        return EINVAL;
    }
    if (config.connect_remote) {
        af::detail::SocketAddress remote{};
        if (!af::detail::socket_address_from_endpoint(config.remote_endpoint, remote,
                                                      address_error)) {
            return address_error == 0 ? EINVAL : address_error;
        }
        if (remote.family != local.family ||
            (remote.family != AF_UNIX && config.remote_endpoint.port == 0U)) {
            return EINVAL;
        }
    }
    for (std::size_t i = 0; i < config.threads.size(); ++i) {
        const af::thread_ref thread = config.threads[i];
        if (!thread || !state_->owner->valid_thread(thread) ||
            state_->owner->thread_kind_of(thread) != af::thread_kind::io) {
            return EINVAL;
        }
        for (std::size_t j = i + 1U; j < config.threads.size(); ++j) {
            if (config.threads[j] == thread) {
                return EINVAL;
            }
        }
    }
    return 0;
}

inline bool udp_socket::start_shard_on_owner(detail::runtime_udp_shard &shard,
                                             const udp_socket_config &config,
                                             udp_socket_callbacks callbacks) noexcept {
    return detail::runtime_udp_start_shard_on_owner(state_, shard, config, callbacks);
}

inline void udp_socket::stop_shard_on_owner(detail::runtime_udp_shard &shard) noexcept {
    detail::runtime_udp_stop_shard_on_owner(state_, shard);
}

inline std::shared_ptr<detail::runtime_udp_shard>
udp_socket::shard_for_thread(af::thread_ref thread) const noexcept {
    if (state_ == nullptr || !thread || thread.index >= state_->shards.size()) {
        return {};
    }
    return state_->shards[thread.index];
}

inline udp_send_result udp_socket::send_on_owner(af::thread_ref thread, std::uint32_t generation,
                                                 af::Buffer buffer) noexcept {
    auto shard = shard_for_thread(thread);
    if (shard == nullptr || !shard->matches_generation(generation)) {
        return udp_send_result::closed;
    }
    return shard->send(std::move(buffer));
}

inline udp_send_result udp_socket::send_on_owner(af::thread_ref thread, std::uint32_t generation,
                                                 af::BufferView view) noexcept {
    auto shard = shard_for_thread(thread);
    if (shard == nullptr || !shard->matches_generation(generation)) {
        return udp_send_result::closed;
    }
    return shard->send(view);
}

inline udp_send_result
udp_socket::send_to_on_owner(af::thread_ref thread, std::uint32_t generation, af::Buffer buffer,
                             const af::detail::SocketAddress &address) noexcept {
    auto shard = shard_for_thread(thread);
    if (shard == nullptr || !shard->matches_generation(generation)) {
        return udp_send_result::closed;
    }
    return shard->send_to(std::move(buffer), address);
}

inline udp_send_result
udp_socket::send_to_on_owner(af::thread_ref thread, std::uint32_t generation, af::BufferView view,
                             const af::detail::SocketAddress &address) noexcept {
    auto shard = shard_for_thread(thread);
    if (shard == nullptr || !shard->matches_generation(generation)) {
        return udp_send_result::closed;
    }
    return shard->send_to(view, address);
}

inline std::size_t udp_socket::active_shard_count() const noexcept {
    if (state_ == nullptr) {
        return 0;
    }
    std::size_t count = 0;
    for (const auto &shard : state_->shards) {
        if (shard != nullptr && shard->active()) {
            ++count;
        }
    }
    return count;
}

inline udp_socket_handle udp_socket::handle() const noexcept {
    if (state_ == nullptr || state_->shards.empty()) {
        return {};
    }
    static thread_local std::uint32_t next_handle_slot = 0;
    const std::size_t count = state_->shards.size();
    const std::size_t start = next_handle_slot++ % count;
    for (std::size_t i = 0; i < count; ++i) {
        const std::size_t index = (start + i) % count;
        const auto &shard = state_->shards[index];
        if (shard != nullptr && shard->active()) {
            return shard->handle();
        }
    }
    return {};
}

inline udp_socket_handle udp_socket::handle_for_thread(af::thread_ref thread) const noexcept {
    auto shard = shard_for_thread(thread);
    return shard == nullptr || !shard->active() ? udp_socket_handle{} : shard->handle();
}

inline std::vector<udp_socket_handle> udp_socket::handles() const {
    std::vector<udp_socket_handle> result;
    if (state_ == nullptr) {
        return result;
    }
    try {
        result.reserve(active_shard_count());
        for (const auto &shard : state_->shards) {
            if (shard != nullptr && shard->active()) {
                result.push_back(shard->handle());
            }
        }
    } catch (...) {
        result.clear();
    }
    return result;
}

inline const udp_endpoint *udp_socket::local_endpoint(af::thread_ref thread) const noexcept {
    auto shard = shard_for_thread(thread);
    if (shard == nullptr || !shard->active()) {
        return nullptr;
    }
    return &shard->local_endpoint;
}

inline std::shared_ptr<detail::runtime_udp_shard> udp_socket_handle::lock_shard(
    const std::shared_ptr<detail::runtime_udp_state> &state) const noexcept {
    if (state == nullptr || !owner_thread_ || owner_thread_.index >= state->shards.size()) {
        return {};
    }
    auto shard = state->shards[owner_thread_.index];
    if (shard == nullptr || !shard->matches_generation(generation_)) {
        return {};
    }
    return shard;
}

inline udp_send_result udp_socket_handle::send(af::Buffer buffer) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send(std::move(buffer));
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        const bool posted = state->owner->post(
            owner_thread_, [shard, generation = generation_, buffer = std::move(buffer)]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send(std::move(buffer)));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline udp_send_result udp_socket_handle::send(af::BufferView view) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send(view);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        af::Buffer buffer = af::Buffer::copy(view);
        const bool posted = state->owner->post(
            owner_thread_, [shard, generation = generation_, buffer = std::move(buffer)]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send(std::move(buffer)));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline udp_send_result udp_socket_handle::send_to(af::Buffer buffer,
                                                  udp_endpoint endpoint) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    af::detail::SocketAddress address{};
    if (!shard->resolve_endpoint(std::move(endpoint), address)) {
        return udp_send_result::unsupported;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send_to(std::move(buffer), address);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        const bool posted =
            state->owner->post(owner_thread_, [shard, generation = generation_,
                                               buffer = std::move(buffer), address]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send_to(std::move(buffer), address));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline udp_send_result udp_socket_handle::send_to(af::BufferView view,
                                                  udp_endpoint endpoint) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    af::detail::SocketAddress address{};
    if (!shard->resolve_endpoint(std::move(endpoint), address)) {
        return udp_send_result::unsupported;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send_to(view, address);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        af::Buffer buffer = af::Buffer::copy(view);
        const bool posted =
            state->owner->post(owner_thread_, [shard, generation = generation_,
                                               buffer = std::move(buffer), address]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send_to(std::move(buffer), address));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline udp_send_result udp_socket_handle::send_to(af::Buffer buffer,
                                                  const udp_peer &peer) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    const af::detail::SocketAddress &address = peer.socket_address();
    if (!shard->supports_peer_address(address)) {
        return udp_send_result::unsupported;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send_to(std::move(buffer), address);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        const bool posted =
            state->owner->post(owner_thread_, [shard, generation = generation_,
                                               buffer = std::move(buffer), address]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send_to(std::move(buffer), address));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline udp_send_result udp_socket_handle::send_to(af::BufferView view,
                                                  const udp_peer &peer) const noexcept {
    auto state = state_.lock();
    auto shard = lock_shard(state);
    if (state == nullptr || shard == nullptr) {
        return udp_send_result::closed;
    }
    const af::detail::SocketAddress &address = peer.socket_address();
    if (!shard->supports_peer_address(address)) {
        return udp_send_result::unsupported;
    }
    if (af::runtime::current() == state->owner &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return shard->send_to(view, address);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return udp_send_result::closed;
    }
    try {
        af::Buffer buffer = af::Buffer::copy(view);
        const bool posted =
            state->owner->post(owner_thread_, [shard, generation = generation_,
                                               buffer = std::move(buffer), address]() mutable {
                if (shard != nullptr && shard->matches_generation(generation)) {
                    static_cast<void>(shard->send_to(std::move(buffer), address));
                }
            });
        return posted ? udp_send_result::queued : udp_send_result::backpressure;
    } catch (...) {
        return udp_send_result::backpressure;
    }
}

inline bool udp_socket_ref::valid() const noexcept {
    return shard_ != nullptr && shard_->active();
}

inline udp_socket_handle udp_socket_ref::handle() const noexcept {
    return shard_ == nullptr ? udp_socket_handle{} : shard_->handle();
}

inline af::thread_ref udp_socket_ref::owner_thread() const noexcept {
    return shard_ == nullptr ? af::thread_ref{} : shard_->owner_thread;
}

inline const udp_endpoint &udp_socket_ref::local_endpoint() const noexcept {
    static const udp_endpoint empty{};
    return shard_ == nullptr ? empty : shard_->local_endpoint;
}

inline udp_send_result udp_socket_ref::send(af::Buffer buffer) const noexcept {
    return shard_ == nullptr ? udp_send_result::closed : shard_->send(std::move(buffer));
}

inline udp_send_result udp_socket_ref::send(af::BufferView view) const noexcept {
    return shard_ == nullptr ? udp_send_result::closed : shard_->send(view);
}

inline udp_send_result udp_socket_ref::send_to(af::Buffer buffer,
                                               udp_endpoint endpoint) const noexcept {
    if (shard_ == nullptr) {
        return udp_send_result::closed;
    }
    af::detail::SocketAddress address{};
    return shard_->resolve_endpoint(std::move(endpoint), address)
               ? shard_->send_to(std::move(buffer), address)
               : udp_send_result::unsupported;
}

inline udp_send_result udp_socket_ref::send_to(af::BufferView view,
                                               udp_endpoint endpoint) const noexcept {
    if (shard_ == nullptr) {
        return udp_send_result::closed;
    }
    af::detail::SocketAddress address{};
    return shard_->resolve_endpoint(std::move(endpoint), address) ? shard_->send_to(view, address)
                                                                  : udp_send_result::unsupported;
}

inline udp_send_result udp_socket_ref::send_to(af::Buffer buffer,
                                               const udp_peer &peer) const noexcept {
    return shard_ == nullptr ? udp_send_result::closed : shard_->send_to(std::move(buffer), peer);
}

inline udp_send_result udp_socket_ref::send_to(af::BufferView view,
                                               const udp_peer &peer) const noexcept {
    return shard_ == nullptr ? udp_send_result::closed : shard_->send_to(view, peer);
}

} // namespace af::net
