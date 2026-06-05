#pragma once

#include <algorithm>
#include <cerrno>

#include "af/net/detail/tcp_socket_ops.hpp"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace af::net {

inline tcp_client::tcp_client(af::runtime &owner)
    : owner_(&owner), handle_state_(std::make_shared<detail::tcp_connection_handle_state>()) {
    handle_state_->owner.store(this, std::memory_order_release);
}

inline tcp_client::~tcp_client() {
    if (handle_state_ != nullptr) {
        handle_state_->accepting_operations.store(false, std::memory_order_release);
    }
    if (running_ || !pending_connects_.empty() || connection_count_ != 0U) {
        AF_ASSERT(on_owner_io_thread() &&
                  "tcp_client must be stopped on the owner reactor thread before destruction");
        if (on_owner_io_thread()) {
            static_cast<void>(stop());
        }
    }
    if (handle_state_ != nullptr) {
        handle_state_->owner.store(nullptr, std::memory_order_release);
    }
}

inline bool tcp_client::connect(tcp_client_connect_config config,
                                tcp_client_callbacks callbacks) noexcept {
    if (!normalize_connect_config(config)) {
        return false;
    }
    if (validate_connect_config(config) != 0) {
        return false;
    }

    af::detail::SocketAddress remote;
    int address_error = 0;
    if (!af::detail::socket_address_from_endpoint(config.remote_endpoint, remote, address_error)) {
        return false;
    }

    af::detail::SocketAddress local;
    if (config.bind_local) {
        if (!af::detail::socket_address_from_endpoint(config.local_endpoint, local,
                                                      address_error)) {
            return false;
        }
        if (local.family != remote.family) {
            return false;
        }
    }

    int fd = open_socket(remote, local, config.bind_local, config);
    if (fd < 0) {
        if (callbacks.on_error != nullptr) {
            callbacks.on_error(callbacks.owner, errno == 0 ? EIO : errno);
        }
        return true;
    }

    const int rc = ::connect(fd, reinterpret_cast<const sockaddr *>(&remote.storage), remote.size);
    if (rc == 0) {
        return start_pending_connect(fd, remote, std::move(config), callbacks, true);
    }

    const int error = errno == 0 ? EIO : errno;
    if (error == EINPROGRESS || error == EALREADY || error == EWOULDBLOCK) {
        return start_pending_connect(fd, remote, std::move(config), callbacks);
    }

    detail::close_fd(fd);
    if (callbacks.on_error != nullptr) {
        callbacks.on_error(callbacks.owner, error);
    }
    return true;
}

inline bool tcp_client::stop() noexcept {
    if (!running_ && pending_connects_.empty() && connection_count_ == 0U) {
        return false;
    }
    if (!on_owner_io_thread()) {
        return false;
    }
    handle_state_->accepting_operations.store(false, std::memory_order_release);
    cancel_all_pending_connects();
    close_all_connections();
    running_ = false;
    return true;
}

inline af::runtime &tcp_client::runtime_owner() noexcept {
    return *owner_;
}

inline bool tcp_client::on_owner_io_thread() const noexcept {
    if (owner_ == nullptr || af::runtime::current() != owner_) {
        return false;
    }
    const af::runtime::thread_index index = af::runtime::current_thread_index();
    return owner_->valid_thread(index) && owner_->thread_kind_of(index) == af::thread_kind::io &&
           (!owner_thread_ || owner_thread_.index == index);
}

inline af::thread_ref
tcp_client::current_or_configured_owner_thread(af::thread_ref configured) const noexcept {
    if (owner_ == nullptr || af::runtime::current() != owner_) {
        return {};
    }
    const af::runtime::thread_index current = af::runtime::current_thread_index();
    if (!owner_->valid_thread(current) || owner_->thread_kind_of(current) != af::thread_kind::io) {
        return {};
    }
    if (configured && configured.index != current) {
        return {};
    }
    if (owner_thread_ && owner_thread_.index != current) {
        return {};
    }
    return af::thread_ref(current);
}

inline bool tcp_client::normalize_connect_config(tcp_client_connect_config &config) const noexcept {
    const af::thread_ref thread = current_or_configured_owner_thread(config.owner_thread);
    if (!thread) {
        return false;
    }
    config.owner_thread = thread;

    const tcp_connection_config defaults;
    if (config.connection.read_buffer_size == 0U) {
        config.connection.read_buffer_size = defaults.read_buffer_size;
    }
    if (config.connection.read_budget_bytes == 0U) {
        config.connection.read_budget_bytes = defaults.read_budget_bytes;
    }
    if (config.connection.write_budget_bytes == 0U) {
        config.connection.write_budget_bytes = defaults.write_budget_bytes;
    }
    if (config.connection.output_high_watermark == 0U) {
        config.connection.output_high_watermark = defaults.output_high_watermark;
    }
    return true;
}

inline int
tcp_client::validate_connect_config(const tcp_client_connect_config &config) const noexcept {
    if (owner_ == nullptr || !config.owner_thread || !owner_->valid_thread(config.owner_thread) ||
        owner_->thread_kind_of(config.owner_thread) != af::thread_kind::io) {
        return EINVAL;
    }
    if (config.connection.read_buffer_size == 0U || config.connection.read_budget_bytes == 0U ||
        config.connection.write_budget_bytes == 0U ||
        config.connection.output_high_watermark == 0U) {
        return EINVAL;
    }
    if (config.connect_timeout.count() < 0) {
        return EINVAL;
    }
    return 0;
}

inline int tcp_client::open_socket(const af::detail::SocketAddress &remote,
                                   const af::detail::SocketAddress &local, bool bind_local,
                                   const tcp_client_connect_config &config) noexcept {
    const bool unix_domain = remote.family == AF_UNIX;
    int fd = ::socket(remote.family, SOCK_STREAM, unix_domain ? 0 : IPPROTO_TCP);
    if (fd < 0) {
        return -1;
    }
    if (!detail::set_nonblocking(fd) || !detail::set_cloexec(fd)) {
        const int error = errno == 0 ? EIO : errno;
        detail::close_fd(fd);
        errno = error;
        return -1;
    }
    detail::set_no_sigpipe(fd);
    if (unix_domain) {
        if (bind_local &&
            ::bind(fd, reinterpret_cast<const sockaddr *>(&local.storage), local.size) != 0) {
            const int error = errno == 0 ? EADDRNOTAVAIL : errno;
            detail::close_fd(fd);
            errno = error;
            return -1;
        }
        return fd;
    }
    if (!detail::set_tcp_no_delay(fd, config.connection.no_delay) ||
        !detail::set_socket_keepalive(fd, config.connection.keepalive)) {
        const int error = errno == 0 ? EIO : errno;
        detail::close_fd(fd);
        errno = error;
        return -1;
    }
    if (bind_local &&
        ::bind(fd, reinterpret_cast<const sockaddr *>(&local.storage), local.size) != 0) {
        const int error = errno == 0 ? EADDRNOTAVAIL : errno;
        detail::close_fd(fd);
        errno = error;
        return -1;
    }
    return fd;
}

inline bool tcp_client::start_pending_connect(int fd, af::detail::SocketAddress remote,
                                              tcp_client_connect_config config,
                                              tcp_client_callbacks callbacks,
                                              bool already_connected) noexcept {
    std::shared_ptr<pending_connect> pending;
    try {
        pending = std::make_shared<pending_connect>();
        pending->client = this;
        pending->owner_thread = config.owner_thread;
        pending->fd = fd;
        pending->remote = remote;
        pending->config = std::move(config);
        pending->callbacks = callbacks;
        pending->source.fd = fd;
        pending->source.interests = af::reactor_writable;
        pending->source.owner = pending.get();
        pending->source.on_event = &tcp_client::on_pending_connect_event;
        pending_connects_.push_back(pending);
    } catch (...) {
        detail::close_fd(fd);
        return false;
    }

    if (!owner_thread_) {
        owner_thread_ = pending->owner_thread;
    }
    running_ = true;
    handle_state_->owner.store(this, std::memory_order_release);
    handle_state_->accepting_operations.store(true, std::memory_order_release);

    if (already_connected) {
        complete_pending_connect(*pending, 0);
        return true;
    }

    if (!register_pending_connect(pending)) {
        fail_pending_connect(*pending, EIO);
        erase_pending_connect(pending.get());
        refresh_running_state();
        return false;
    }

    if (pending->config.connect_timeout.count() > 0) {
        std::weak_ptr<pending_connect> weak = pending;
        const bool scheduled = owner_->schedule_after(
            pending->owner_thread, pending->config.connect_timeout, [weak]() mutable {
                auto locked = weak.lock();
                if (locked == nullptr || locked->client == nullptr) {
                    return;
                }
                locked->client->complete_pending_connect(*locked, ETIMEDOUT);
            });
        if (!scheduled) {
            cancel_pending_connect(*pending);
            refresh_running_state();
            return false;
        }
    }

    return true;
}

inline bool
tcp_client::register_pending_connect(const std::shared_ptr<pending_connect> &pending) noexcept {
    if (pending == nullptr || pending->registered || pending->fd < 0) {
        return false;
    }
    if (!owner_->register_reactor_source(pending->owner_thread, &pending->source)) {
        return false;
    }
    pending->registered = true;
    return true;
}

inline std::shared_ptr<tcp_client::pending_connect>
tcp_client::find_pending(pending_connect *pending) noexcept {
    if (pending == nullptr) {
        return {};
    }
    for (const auto &current : pending_connects_) {
        if (current.get() == pending) {
            return current;
        }
    }
    return {};
}

inline void tcp_client::complete_pending_connect(pending_connect &pending, int error) noexcept {
    std::shared_ptr<pending_connect> keep_alive = find_pending(&pending);
    if (keep_alive == nullptr || pending.completed) {
        return;
    }
    pending.completed = true;
    if (pending.registered) {
        static_cast<void>(owner_->unregister_reactor_source(pending.owner_thread, &pending.source));
        pending.registered = false;
    }

    if (error == 0) {
        if (!adopt_connected_socket(pending)) {
            fail_pending_connect(pending, EIO);
        }
    } else {
        fail_pending_connect(pending, error);
    }

    erase_pending_connect(&pending);
    refresh_running_state();
}

inline bool tcp_client::adopt_connected_socket(pending_connect &pending) noexcept {
    int owned_fd = pending.fd;
    pending.fd = -1;
    connection_entry *entry = nullptr;
    try {
        entry = acquire_connection_slot();
        entry->client = this;

        af::detail::SocketAddress local_address;
        socklen_t local_size = sizeof(local_address.storage);
        tcp_endpoint local_endpoint;
        if (::getsockname(owned_fd, reinterpret_cast<sockaddr *>(&local_address.storage),
                          &local_size) == 0) {
            local_address.size = local_size;
            local_endpoint = af::detail::endpoint_from_socket_address(
                reinterpret_cast<const sockaddr *>(&local_address.storage), local_address.size);
        } else {
            local_endpoint = pending.config.local_endpoint;
        }

        af::detail::SocketAddress peer_address;
        socklen_t peer_size = sizeof(peer_address.storage);
        tcp_endpoint peer_endpoint;
        if (::getpeername(owned_fd, reinterpret_cast<sockaddr *>(&peer_address.storage),
                          &peer_size) == 0) {
            peer_address.size = peer_size;
            peer_endpoint = af::detail::endpoint_from_socket_address(
                reinterpret_cast<const sockaddr *>(&peer_address.storage), peer_address.size);
        } else {
            peer_endpoint = pending.config.remote_endpoint;
        }

        tcp_connection_callbacks connection_callbacks;
        connection_callbacks.owner = pending.callbacks.owner;
        connection_callbacks.on_read = pending.callbacks.on_read;
        connection_callbacks.on_close = pending.callbacks.on_close;

        detail::tcp_connection_lifecycle lifecycle;
        lifecycle.owner = entry;
        lifecycle.on_inactive = &tcp_client::on_connection_inactive;
        lifecycle.on_callback_begin = &tcp_client::on_connection_callback_begin;
        lifecycle.on_callback_end = &tcp_client::on_connection_callback_end;

        entry->connection = std::make_unique<tcp_connection>(
            *owner_, pending.owner_thread, owned_fd, entry->slot, entry->generation,
            std::move(local_endpoint), std::move(peer_endpoint), pending.config.connection,
            connection_callbacks, lifecycle, handle_state_);
        owned_fd = -1;
        if (!entry->connection->start()) {
            release_connection_slot(entry->slot);
            return false;
        }
    } catch (...) {
        detail::close_fd(owned_fd);
        if (entry != nullptr) {
            release_connection_slot(entry->slot);
        }
        return false;
    }

    if (pending.callbacks.on_connect != nullptr) {
        begin_user_callback();
        pending.callbacks.on_connect(pending.callbacks.owner,
                                     tcp_connection_ref(entry->connection.get()));
        end_user_callback();
    }
    if (entry->connection == nullptr || !entry->connection->alive()) {
        retire_connection_slot(entry->slot, entry->generation);
    }
    reap_retired_connections_if_safe();
    return true;
}

inline void tcp_client::fail_pending_connect(pending_connect &pending, int error) noexcept {
    detail::close_fd(pending.fd);
    if (pending.callbacks.on_error != nullptr) {
        pending.callbacks.on_error(pending.callbacks.owner, error == 0 ? EIO : error);
    }
}

inline void tcp_client::erase_pending_connect(pending_connect *pending) noexcept {
    const auto it = std::find_if(pending_connects_.begin(), pending_connects_.end(),
                                 [pending](const std::shared_ptr<pending_connect> &current) {
                                     return current.get() == pending;
                                 });
    if (it != pending_connects_.end()) {
        pending_connects_.erase(it);
    }
}

inline void tcp_client::cancel_pending_connect(pending_connect &pending) noexcept {
    if (pending.registered) {
        static_cast<void>(owner_->unregister_reactor_source(pending.owner_thread, &pending.source));
        pending.registered = false;
    }
    pending.completed = true;
    detail::close_fd(pending.fd);
    erase_pending_connect(&pending);
}

inline void tcp_client::cancel_all_pending_connects() noexcept {
    while (!pending_connects_.empty()) {
        std::shared_ptr<pending_connect> pending = pending_connects_.back();
        if (pending == nullptr) {
            pending_connects_.pop_back();
            continue;
        }
        cancel_pending_connect(*pending);
    }
}

inline void tcp_client::refresh_running_state() noexcept {
    if (!pending_connects_.empty() || connection_count_ != 0U) {
        return;
    }
    running_ = false;
    handle_state_->accepting_operations.store(false, std::memory_order_release);
}

inline void tcp_client::begin_user_callback() noexcept {
    ++user_callback_depth_;
}

inline void tcp_client::end_user_callback() noexcept {
    if (user_callback_depth_ > 0U) {
        --user_callback_depth_;
    }
}

inline void tcp_client::on_connection_callback_begin(void *owner) noexcept {
    auto *entry = static_cast<connection_entry *>(owner);
    if (entry != nullptr && entry->client != nullptr) {
        entry->client->begin_user_callback();
    }
}

inline void tcp_client::on_connection_callback_end(void *owner) noexcept {
    auto *entry = static_cast<connection_entry *>(owner);
    if (entry != nullptr && entry->client != nullptr) {
        entry->client->end_user_callback();
    }
}

inline tcp_client::connection_entry *tcp_client::acquire_connection_slot() {
    const std::uint32_t generation = next_connection_generation();
    if (!free_connection_slots_.empty()) {
        const std::uint32_t slot = free_connection_slots_.back();
        free_connection_slots_.pop_back();
        connections_[slot] = std::make_unique<connection_entry>();
        connection_entry &entry = *connections_[slot];
        entry.slot = slot;
        entry.generation = generation;
        entry.occupied = true;
        entry.retired = false;
        ++connection_count_;
        return &entry;
    }
    const std::uint32_t slot = static_cast<std::uint32_t>(connections_.size());
    connections_.push_back(std::make_unique<connection_entry>());
    connection_entry &entry = *connections_.back();
    entry.slot = slot;
    entry.generation = generation;
    entry.occupied = true;
    entry.retired = false;
    ++connection_count_;
    return &entry;
}

inline std::uint32_t tcp_client::next_connection_generation() noexcept {
    std::uint32_t generation = next_connection_generation_++;
    if (generation == 0U) {
        generation = next_connection_generation_++;
    }
    return generation;
}

inline void tcp_client::retire_connection_slot(std::uint32_t slot,
                                               std::uint32_t generation) noexcept {
    if (slot >= connections_.size() || connections_[slot] == nullptr) {
        return;
    }
    connection_entry &entry = *connections_[slot];
    if (!entry.occupied || entry.generation != generation || entry.retired) {
        return;
    }
    entry.retired = true;
    try {
        retired_connection_slots_.push_back(connection_slot_ref{slot, generation});
    } catch (...) {
    }
}

inline void tcp_client::reap_retired_connections() noexcept {
    if (retired_connection_slots_.empty()) {
        return;
    }
    for (const connection_slot_ref retired : retired_connection_slots_) {
        if (retired.slot >= connections_.size() || connections_[retired.slot] == nullptr) {
            continue;
        }
        const connection_entry &entry = *connections_[retired.slot];
        if (!entry.occupied || entry.generation != retired.generation ||
            entry.connection == nullptr || entry.connection->alive()) {
            continue;
        }
        release_connection_slot(retired.slot);
    }
    retired_connection_slots_.clear();
}

inline void tcp_client::reap_retired_connections_if_safe() noexcept {
    if (user_callback_depth_ == 0U) {
        reap_retired_connections();
    }
}

inline void tcp_client::release_connection_slot(std::uint32_t slot) noexcept {
    if (slot >= connections_.size() || connections_[slot] == nullptr ||
        !connections_[slot]->occupied) {
        return;
    }
    connections_[slot].reset();
    --connection_count_;
    try {
        free_connection_slots_.push_back(slot);
    } catch (...) {
    }
    refresh_running_state();
}

inline void tcp_client::close_all_connections() noexcept {
    for (auto &entry : connections_) {
        if (entry == nullptr || !entry->occupied || entry->connection == nullptr) {
            continue;
        }
        entry->connection->close(close_reason::local);
    }
    connections_.clear();
    free_connection_slots_.clear();
    retired_connection_slots_.clear();
    connection_count_ = 0;
}

inline tcp_client::connection_entry *
tcp_client::find_connection_entry(std::uint32_t slot, std::uint32_t generation) noexcept {
    if (!on_owner_io_thread() || slot >= connections_.size()) {
        return nullptr;
    }
    connection_entry *entry = connections_[slot].get();
    if (entry == nullptr || !entry->occupied || entry->generation != generation ||
        entry->connection == nullptr || !entry->connection->alive()) {
        return nullptr;
    }
    return entry;
}

inline tcp_connection *tcp_client::find_connection(std::uint32_t slot,
                                                   std::uint32_t generation) noexcept {
    connection_entry *entry = find_connection_entry(slot, generation);
    if (entry == nullptr) {
        return nullptr;
    }
    return entry->connection.get();
}

inline send_result tcp_client::send_to_connection(std::uint32_t slot, std::uint32_t generation,
                                                  af::Buffer buffer) noexcept {
    connection_entry *entry = find_connection_entry(slot, generation);
    if (entry == nullptr) {
        return send_result::closed;
    }
    const send_result result = entry->connection->send(std::move(buffer));
    if (result == send_result::closed) {
        retire_connection_slot(slot, generation);
        reap_retired_connections_if_safe();
    }
    return result;
}

inline send_result tcp_client::send_to_connection(std::uint32_t slot, std::uint32_t generation,
                                                  af::BufferView view) noexcept {
    connection_entry *entry = find_connection_entry(slot, generation);
    if (entry == nullptr) {
        return send_result::closed;
    }
    const send_result result = entry->connection->send(view);
    if (result == send_result::closed) {
        retire_connection_slot(slot, generation);
        reap_retired_connections_if_safe();
    }
    return result;
}

inline bool tcp_client::pause_connection_read(std::uint32_t slot,
                                              std::uint32_t generation) noexcept {
    connection_entry *entry = find_connection_entry(slot, generation);
    return entry != nullptr && entry->connection->pause_read();
}

inline bool tcp_client::resume_connection_read(std::uint32_t slot,
                                               std::uint32_t generation) noexcept {
    connection_entry *entry = find_connection_entry(slot, generation);
    return entry != nullptr && entry->connection->resume_read();
}

inline bool tcp_client::set_connection_no_delay(std::uint32_t slot, std::uint32_t generation,
                                                bool enabled) noexcept {
    connection_entry *entry = find_connection_entry(slot, generation);
    return entry != nullptr && entry->connection->set_no_delay(enabled);
}

inline bool tcp_client::set_connection_keepalive(std::uint32_t slot, std::uint32_t generation,
                                                 bool enabled) noexcept {
    connection_entry *entry = find_connection_entry(slot, generation);
    return entry != nullptr && entry->connection->set_keepalive(enabled);
}

inline bool tcp_client::close_connection(std::uint32_t slot, std::uint32_t generation,
                                         close_reason reason) noexcept {
    connection_entry *entry = find_connection_entry(slot, generation);
    if (entry == nullptr) {
        return false;
    }
    entry->connection->close(reason);
    retire_connection_slot(slot, generation);
    reap_retired_connections_if_safe();
    return true;
}

inline bool tcp_client::close_connection_after_flush(std::uint32_t slot,
                                                     std::uint32_t generation) noexcept {
    connection_entry *entry = find_connection_entry(slot, generation);
    if (entry == nullptr) {
        return false;
    }
    const bool closed = entry->connection->close_after_flush();
    if (closed) {
        retire_connection_slot(slot, generation);
        reap_retired_connections_if_safe();
    }
    return true;
}

inline void tcp_client::on_connection_inactive(void *owner, tcp_connection &connection) noexcept {
    auto *entry = static_cast<connection_entry *>(owner);
    if (entry == nullptr || entry->client == nullptr || !entry->occupied ||
        entry->generation != connection.generation()) {
        return;
    }
    entry->client->retire_connection_slot(entry->slot, entry->generation);
    entry->client->reap_retired_connections_if_safe();
}

inline void tcp_client::on_pending_connect_event(void *owner, af::fd_event_source &source,
                                                 std::uint32_t events) noexcept {
    static_cast<void>(source);
    auto *pending = static_cast<pending_connect *>(owner);
    if (pending == nullptr || pending->client == nullptr || pending->completed) {
        return;
    }

    int error = 0;
    socklen_t error_size = sizeof(error);
    if (::getsockopt(pending->fd, SOL_SOCKET, SO_ERROR, &error, &error_size) != 0) {
        error = errno == 0 ? EIO : errno;
    }
    if (error == 0 && (events & (af::reactor_error | af::reactor_hangup)) != 0U &&
        (events & af::reactor_writable) == 0U) {
        error = ECONNRESET;
    }
    pending->client->complete_pending_connect(*pending, error);
}

} // namespace af::net
