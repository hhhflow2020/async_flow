#pragma once

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/detail/net/socket_address.hpp"
#include "af/net/tcp_connection_runtime.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_listener.hpp"
#include "af/net/tcp_types.hpp"
#include "af/runtime.hpp"
#include "af/thread_kind.hpp"

#include <sys/socket.h>
#include <unistd.h>

namespace af::net {

class tcp_server : private detail::tcp_connection_owner {
public:
    explicit tcp_server(af::runtime &owner, tcp_server_config config = {})
        : owner_(&owner), config_(std::move(config)),
          handle_state_(std::make_shared<detail::tcp_connection_handle_state>()) {
        handle_state_->owner.store(this, std::memory_order_release);
    }

    tcp_server(const tcp_server &) = delete;
    tcp_server &operator=(const tcp_server &) = delete;

    ~tcp_server() {
        if (handle_state_ != nullptr) {
            handle_state_->accepting_operations.store(false, std::memory_order_release);
        }
        if (running_) {
            AF_ASSERT(on_owner_io_thread() &&
                      "tcp_server must be stopped on the owner reactor thread before destruction");
            if (on_owner_io_thread()) {
                static_cast<void>(stop());
            }
        }
        if (handle_state_ != nullptr) {
            handle_state_->owner.store(nullptr, std::memory_order_release);
        }
    }

    [[nodiscard]] listener_result add_listener(tcp_listener_config config,
                                               tcp_listener_callbacks callbacks = {}) {
        return add_listener_raw(std::move(config), callbacks);
    }

    [[nodiscard]] listener_result add_listener(tcp_listener_config config,
                                               tcp_connection_callbacks callbacks) {
        return add_listener_connection(std::move(config), callbacks);
    }

    [[nodiscard]] listener_result add_listener_raw(tcp_listener_config config,
                                                   tcp_listener_callbacks callbacks = {}) {
        const af::thread_ref owner_thread = current_owner_thread();
        if (!owner_thread) {
            return listener_result::failure(EPERM);
        }
        if (!listener_config_targets_current_thread(config, owner_thread)) {
            return listener_result::failure(EOPNOTSUPP);
        }

        listener_id id;
        try {
            id = acquire_listener_slot();
            listener_entry &entry = *entries_[id.slot];
            entry.server = this;
            entry.id = id;
            entry.state = listener_state::configured;
            entry.config = std::move(config);
            entry.raw_callbacks = callbacks;
            entry.owner_thread = owner_thread;
            ++listener_count_;
        } catch (...) {
            return listener_result::failure(ENOMEM);
        }

        listener_entry &entry = *entries_[id.slot];
        if (running_) {
            const int error = start_entry(entry);
            if (error != 0) {
                release_listener_slot(id.slot);
                return listener_result::failure(error);
            }
        }
        return listener_result::success(tcp_listener_handle{id});
    }

    [[nodiscard]] listener_result add_listener_connection(tcp_listener_config config,
                                                          tcp_connection_callbacks callbacks) {
        const af::thread_ref owner_thread = current_owner_thread();
        if (!owner_thread) {
            return listener_result::failure(EPERM);
        }
        if (!listener_config_targets_current_thread(config, owner_thread)) {
            return listener_result::failure(EOPNOTSUPP);
        }

        listener_id id;
        try {
            id = acquire_listener_slot();
            listener_entry &entry = *entries_[id.slot];
            entry.server = this;
            entry.id = id;
            entry.state = listener_state::configured;
            entry.config = std::move(config);
            entry.connection_callbacks = callbacks;
            entry.connection_mode = true;
            entry.owner_thread = owner_thread;
            ++listener_count_;
        } catch (...) {
            return listener_result::failure(ENOMEM);
        }

        listener_entry &entry = *entries_[id.slot];
        if (running_) {
            const int error = start_entry(entry);
            if (error != 0) {
                release_listener_slot(id.slot);
                return listener_result::failure(error);
            }
        }
        return listener_result::success(tcp_listener_handle{id});
    }

    [[nodiscard]] bool start() noexcept {
        if (running_) {
            return false;
        }
        if (!on_owner_io_thread()) {
            return false;
        }

        running_ = true;
        handle_state_->owner.store(this, std::memory_order_release);
        handle_state_->accepting_operations.store(true, std::memory_order_release);
        bool ok = true;
        for (auto &entry_ptr : entries_) {
            if (entry_ptr == nullptr) {
                continue;
            }
            listener_entry &entry = *entry_ptr;
            if (!entry.occupied() || entry.state != listener_state::configured) {
                continue;
            }
            const int error = start_entry(entry);
            if (error != 0) {
                entry.state = listener_state::failed;
                entry.last_error = error;
                ok = false;
            }
        }
        return ok;
    }

    [[nodiscard]] bool stop() noexcept {
        if (!running_) {
            return false;
        }
        if (!on_owner_io_thread()) {
            return false;
        }

        handle_state_->accepting_operations.store(false, std::memory_order_release);
        bool ok = true;
        for (auto &entry_ptr : entries_) {
            if (entry_ptr == nullptr) {
                continue;
            }
            listener_entry &entry = *entry_ptr;
            if (!entry.occupied() || entry.state != listener_state::active) {
                continue;
            }
            if (entry.listener != nullptr && !entry.listener->stop()) {
                ok = false;
            }
            entry.listener.reset();
            entry.state = listener_state::configured;
        }
        close_all_connections();
        running_ = false;
        return ok;
    }

    [[nodiscard]] bool remove_listener(
        tcp_listener_handle handle,
        remove_listener_policy policy = remove_listener_policy::stop_accept_only) noexcept {
        static_cast<void>(policy);
        if (!on_owner_io_thread()) {
            return false;
        }
        listener_entry *entry = find_entry(handle);
        if (entry == nullptr) {
            return false;
        }
        if (entry->state == listener_state::active && entry->listener != nullptr) {
            if (!entry->listener->stop()) {
                return false;
            }
        }
        release_listener_slot(handle.slot());
        return true;
    }

    [[nodiscard]] bool running() const noexcept {
        return running_;
    }

    [[nodiscard]] std::size_t listener_count() const noexcept {
        return listener_count_;
    }

    [[nodiscard]] std::size_t connection_count() const noexcept {
        return connection_count_;
    }

    [[nodiscard]] listener_state state(tcp_listener_handle handle) const noexcept {
        const listener_entry *entry = find_entry(handle);
        return entry == nullptr ? listener_state::removed : entry->state;
    }

    [[nodiscard]] int last_error(tcp_listener_handle handle) const noexcept {
        const listener_entry *entry = find_entry(handle);
        return entry == nullptr ? ENOENT : entry->last_error;
    }

    [[nodiscard]] const tcp_endpoint *local_endpoint(tcp_listener_handle handle) const noexcept {
        const listener_entry *entry = find_entry(handle);
        if (entry == nullptr || entry->listener == nullptr || !entry->listener->started()) {
            return nullptr;
        }
        return &entry->listener->local_endpoint();
    }

    [[nodiscard]] af::runtime &owner() noexcept {
        return *owner_;
    }

    [[nodiscard]] const af::runtime &owner() const noexcept {
        return *owner_;
    }

    [[nodiscard]] const tcp_server_config &config() const noexcept {
        return config_;
    }

private:
    friend class tcp_connection_handle;

    struct listener_entry {
        tcp_server *server{nullptr};
        listener_id id{};
        listener_state state{listener_state::removed};
        tcp_listener_config config;
        tcp_listener_callbacks raw_callbacks{};
        tcp_connection_callbacks connection_callbacks{};
        std::unique_ptr<tcp_listener> listener;
        af::thread_ref owner_thread{};
        int last_error{0};
        bool connection_mode{false};

        [[nodiscard]] bool occupied() const noexcept {
            return id.valid();
        }
    };

    struct connection_entry {
        tcp_server *server{nullptr};
        std::unique_ptr<tcp_connection> connection;
        std::uint32_t slot{0};
        std::uint32_t generation{0};
        bool occupied{false};
    };

    [[nodiscard]] af::thread_ref current_owner_thread() const noexcept {
        if (!on_owner_io_thread()) {
            return {};
        }
        return af::thread_ref(af::runtime::current_thread_index());
    }

    [[nodiscard]] bool on_owner_io_thread() const noexcept {
        if (owner_ == nullptr || af::runtime::current() != owner_) {
            return false;
        }
        const af::runtime::thread_index index = af::runtime::current_thread_index();
        return owner_->valid_thread(index) && owner_->thread_kind_of(index) == af::thread_kind::io;
    }

    [[nodiscard]] bool
    listener_config_targets_current_thread(const tcp_listener_config &config,
                                           af::thread_ref owner_thread) const noexcept {
        if (config.threads.empty()) {
            return true;
        }
        if (config.threads.size() != 1U) {
            return false;
        }
        return config.threads.front() == owner_thread;
    }

    [[nodiscard]] listener_id acquire_listener_slot() {
        const std::uint32_t generation = next_listener_generation();
        if (!free_listener_slots_.empty()) {
            const std::uint32_t slot = free_listener_slots_.back();
            free_listener_slots_.pop_back();
            entries_[slot] = std::make_unique<listener_entry>();
            return listener_id{slot, generation};
        }
        const std::uint32_t slot = static_cast<std::uint32_t>(entries_.size());
        entries_.push_back(std::make_unique<listener_entry>());
        return listener_id{slot, generation};
    }

    [[nodiscard]] std::uint32_t next_listener_generation() noexcept {
        std::uint32_t generation = next_listener_generation_++;
        if (generation == 0U) {
            generation = next_listener_generation_++;
        }
        return generation;
    }

    void release_listener_slot(std::uint32_t slot) noexcept {
        if (slot >= entries_.size() || entries_[slot] == nullptr || !entries_[slot]->occupied()) {
            return;
        }
        listener_entry &entry = *entries_[slot];
        entry.listener.reset();
        entries_[slot].reset();
        --listener_count_;
        try {
            free_listener_slots_.push_back(slot);
        } catch (...) {
        }
    }

    [[nodiscard]] int start_entry(listener_entry &entry) noexcept {
        try {
            auto listener = std::make_unique<tcp_listener>(*owner_);
            tcp_listener_callbacks callbacks;
            callbacks.owner = &entry;
            callbacks.on_accept = &tcp_server::on_listener_accept;
            callbacks.on_error = &tcp_server::on_listener_error;
            if (!listener->start(entry.owner_thread, entry.config, callbacks)) {
                if (entry.last_error == 0) {
                    entry.last_error = EIO;
                }
                entry.state = listener_state::failed;
                return entry.last_error;
            }
            entry.listener = std::move(listener);
            entry.state = listener_state::active;
            entry.last_error = 0;
            return 0;
        } catch (...) {
            entry.last_error = ENOMEM;
            entry.state = listener_state::failed;
            return ENOMEM;
        }
    }

    static void on_listener_accept(void *owner, tcp_listener &listener, int fd,
                                   const sockaddr *peer, socklen_t peer_size) noexcept {
        auto *entry = static_cast<listener_entry *>(owner);
        if (entry == nullptr) {
            if (fd >= 0) {
                ::close(fd);
            }
            return;
        }
        if (entry->connection_mode) {
            if (entry->server == nullptr) {
                if (fd >= 0) {
                    ::close(fd);
                }
                return;
            }
            static_cast<void>(
                entry->server->adopt_connection(*entry, listener, fd, peer, peer_size));
            return;
        }
        if (entry->raw_callbacks.on_accept != nullptr) {
            entry->raw_callbacks.on_accept(entry->raw_callbacks.owner, listener, fd, peer,
                                           peer_size);
            return;
        }
        if (fd >= 0) {
            ::close(fd);
        }
    }

    static void on_listener_error(void *owner, tcp_listener &listener, int error) noexcept {
        auto *entry = static_cast<listener_entry *>(owner);
        if (entry == nullptr) {
            return;
        }
        entry->last_error = error == 0 ? EIO : error;
        if (entry->connection_mode) {
            return;
        }
        if (entry->raw_callbacks.on_error != nullptr) {
            entry->raw_callbacks.on_error(entry->raw_callbacks.owner, listener, entry->last_error);
        }
    }

    [[nodiscard]] listener_entry *find_entry(tcp_listener_handle handle) noexcept {
        if (!handle.valid() || handle.slot() >= entries_.size()) {
            return nullptr;
        }
        if (entries_[handle.slot()] == nullptr) {
            return nullptr;
        }
        listener_entry &entry = *entries_[handle.slot()];
        if (!entry.occupied() || entry.id != handle.id) {
            return nullptr;
        }
        return &entry;
    }

    [[nodiscard]] const listener_entry *find_entry(tcp_listener_handle handle) const noexcept {
        if (!handle.valid() || handle.slot() >= entries_.size()) {
            return nullptr;
        }
        if (entries_[handle.slot()] == nullptr) {
            return nullptr;
        }
        const listener_entry &entry = *entries_[handle.slot()];
        if (!entry.occupied() || entry.id != handle.id) {
            return nullptr;
        }
        return &entry;
    }

    [[nodiscard]] bool adopt_connection(listener_entry &listener, tcp_listener &source, int fd,
                                        const sockaddr *peer, socklen_t peer_size) noexcept {
        int owned_fd = fd;
        if (owned_fd < 0) {
            return false;
        }
        if (!on_owner_io_thread()) {
            ::close(owned_fd);
            return false;
        }
        connection_entry *entry = nullptr;
        try {
            entry = acquire_connection_slot();
            entry->server = this;
            const tcp_endpoint peer_endpoint =
                af::detail::endpoint_from_socket_address(peer, peer_size);
            detail::tcp_connection_lifecycle lifecycle;
            lifecycle.owner = entry;
            lifecycle.on_inactive = &tcp_server::on_connection_inactive;
            entry->connection = std::make_unique<tcp_connection>(
                *owner_, source.owner_thread(), owned_fd, entry->slot, entry->generation,
                source.local_endpoint(), peer_endpoint, config_.connection,
                listener.connection_callbacks, lifecycle, handle_state_);
            owned_fd = -1;
            if (!entry->connection->start()) {
                release_connection_slot(entry->slot);
                return false;
            }
        } catch (...) {
            if (owned_fd >= 0) {
                ::close(owned_fd);
            }
            if (entry != nullptr) {
                release_connection_slot(entry->slot);
            }
            return false;
        }
        if (listener.connection_callbacks.on_accept != nullptr) {
            listener.connection_callbacks.on_accept(listener.connection_callbacks.owner,
                                                    tcp_connection_ref(entry->connection.get()));
        }
        if (entry->connection == nullptr || !entry->connection->alive()) {
            release_connection_slot(entry->slot);
        }
        return true;
    }

    [[nodiscard]] af::runtime &runtime_owner() noexcept override {
        return *owner_;
    }

    [[nodiscard]] connection_entry *find_connection_entry(std::uint32_t slot,
                                                          std::uint32_t generation) noexcept {
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

    [[nodiscard]] tcp_connection *find_connection(std::uint32_t slot,
                                                  std::uint32_t generation) noexcept {
        connection_entry *entry = find_connection_entry(slot, generation);
        if (entry == nullptr) {
            return nullptr;
        }
        return entry->connection.get();
    }

    [[nodiscard]] send_result send_to_connection(std::uint32_t slot, std::uint32_t generation,
                                                 af::Buffer buffer) noexcept override {
        connection_entry *entry = find_connection_entry(slot, generation);
        if (entry == nullptr) {
            return send_result::closed;
        }
        const send_result result = entry->connection->send(std::move(buffer));
        if (result == send_result::closed) {
            release_connection_slot(slot);
        }
        return result;
    }

    [[nodiscard]] send_result send_to_connection(std::uint32_t slot, std::uint32_t generation,
                                                 af::BufferView view) noexcept override {
        connection_entry *entry = find_connection_entry(slot, generation);
        if (entry == nullptr) {
            return send_result::closed;
        }
        const send_result result = entry->connection->send(view);
        if (result == send_result::closed) {
            release_connection_slot(slot);
        }
        return result;
    }

    [[nodiscard]] bool close_connection(std::uint32_t slot, std::uint32_t generation,
                                        close_reason reason) noexcept override {
        connection_entry *entry = find_connection_entry(slot, generation);
        if (entry == nullptr) {
            return false;
        }
        entry->connection->close(reason);
        release_connection_slot(slot);
        return true;
    }

    [[nodiscard]] bool close_connection_after_flush(std::uint32_t slot,
                                                    std::uint32_t generation) noexcept override {
        connection_entry *entry = find_connection_entry(slot, generation);
        if (entry == nullptr) {
            return false;
        }
        const bool closed = entry->connection->close_after_flush();
        if (closed) {
            release_connection_slot(slot);
        }
        return true;
    }

    [[nodiscard]] connection_entry *acquire_connection_slot() {
        const std::uint32_t generation = next_connection_generation();
        if (!free_connection_slots_.empty()) {
            const std::uint32_t slot = free_connection_slots_.back();
            free_connection_slots_.pop_back();
            connections_[slot] = std::make_unique<connection_entry>();
            connection_entry &entry = *connections_[slot];
            entry.slot = slot;
            entry.generation = generation;
            entry.occupied = true;
            ++connection_count_;
            return &entry;
        }
        const std::uint32_t slot = static_cast<std::uint32_t>(connections_.size());
        connections_.push_back(std::make_unique<connection_entry>());
        connection_entry &entry = *connections_.back();
        entry.slot = slot;
        entry.generation = generation;
        entry.occupied = true;
        ++connection_count_;
        return &entry;
    }

    [[nodiscard]] std::uint32_t next_connection_generation() noexcept {
        std::uint32_t generation = next_connection_generation_++;
        if (generation == 0U) {
            generation = next_connection_generation_++;
        }
        return generation;
    }

    void release_connection_slot(std::uint32_t slot) noexcept {
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
    }

    void close_all_connections() noexcept {
        for (auto &entry : connections_) {
            if (entry == nullptr || !entry->occupied || entry->connection == nullptr) {
                continue;
            }
            entry->connection->close(close_reason::local);
        }
        connections_.clear();
        free_connection_slots_.clear();
        connection_count_ = 0;
    }

    static void on_connection_inactive(void *owner, tcp_connection &connection) noexcept {
        auto *entry = static_cast<connection_entry *>(owner);
        if (entry == nullptr || entry->server == nullptr || !entry->occupied ||
            entry->generation != connection.generation()) {
            return;
        }
        entry->server->release_connection_slot(entry->slot);
    }

    af::runtime *owner_{nullptr};
    tcp_server_config config_;
    std::shared_ptr<detail::tcp_connection_handle_state> handle_state_;
    std::vector<std::unique_ptr<listener_entry>> entries_;
    std::vector<std::unique_ptr<connection_entry>> connections_;
    std::vector<std::uint32_t> free_listener_slots_;
    std::vector<std::uint32_t> free_connection_slots_;
    std::uint32_t next_listener_generation_{1};
    std::uint32_t next_connection_generation_{1};
    std::size_t listener_count_{0};
    std::size_t connection_count_{0};
    bool running_{false};
};

} // namespace af::net

#include "af/net/detail/tcp_connection_handle_runtime_impl.hpp"
