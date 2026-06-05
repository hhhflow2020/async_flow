#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
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

class tcp_server_shard : private detail::tcp_connection_owner {
public:
    explicit tcp_server_shard(af::runtime &owner, tcp_server_config config = {})
        : owner_(&owner), config_(normalize_config(std::move(config))),
          handle_state_(std::make_shared<detail::tcp_connection_handle_state>()) {
        handle_state_->owner.store(this, std::memory_order_release);
    }

    tcp_server_shard(const tcp_server_shard &) = delete;
    tcp_server_shard &operator=(const tcp_server_shard &) = delete;

    ~tcp_server_shard() {
        if (handle_state_ != nullptr) {
            handle_state_->accepting_operations.store(false, std::memory_order_release);
        }
        if (running_) {
            AF_ASSERT(on_control_thread() &&
                      "tcp_server must be stopped on the owner reactor thread before destruction");
            if (on_control_thread()) {
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
        const listener_id id = next_listener_id();
        return add_listener_raw_with_id(id, std::move(config), callbacks);
    }

    [[nodiscard]] listener_result add_listener_raw_with_id(listener_id id,
                                                           tcp_listener_config config,
                                                           tcp_listener_callbacks callbacks = {}) {
        const af::thread_ref owner_thread = current_owner_thread();
        if (!owner_thread) {
            return listener_result::failure(EPERM);
        }
        if (!listener_config_targets_current_thread(config, owner_thread)) {
            return listener_result::failure(EOPNOTSUPP);
        }
        if (!id.valid()) {
            return listener_result::failure(EINVAL);
        }

        try {
            if (!acquire_listener_slot(id)) {
                return listener_result::failure(EEXIST);
            }
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
        const listener_id id = next_listener_id();
        return add_listener_connection_with_id(id, std::move(config), callbacks);
    }

    [[nodiscard]] listener_result
    add_listener_connection_with_id(listener_id id, tcp_listener_config config,
                                    tcp_connection_callbacks callbacks) {
        const af::thread_ref owner_thread = current_owner_thread();
        if (!owner_thread) {
            return listener_result::failure(EPERM);
        }
        if (!listener_config_targets_current_thread(config, owner_thread)) {
            return listener_result::failure(EOPNOTSUPP);
        }
        if (!id.valid()) {
            return listener_result::failure(EINVAL);
        }

        try {
            if (!acquire_listener_slot(id)) {
                return listener_result::failure(EEXIST);
            }
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
        if (running_ || stopping_connections_) {
            return false;
        }
        if (!prepare_current_control_thread()) {
            return false;
        }

        running_ = true;
        handle_state_->owner.store(this, std::memory_order_release);
        handle_state_->accepting_operations.store(true, std::memory_order_release);
        stopping_connections_ = false;
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
        if (!on_control_thread()) {
            return false;
        }

        const std::uint64_t stop_generation = ++stop_generation_;
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
        running_ = false;
        if (!begin_connection_stop(stop_generation)) {
            ok = false;
        }
        return ok;
    }

    [[nodiscard]] bool remove_listener(
        tcp_listener_handle handle,
        remove_listener_policy policy = remove_listener_policy::stop_accept_only) noexcept {
        static_cast<void>(policy);
        if (!on_control_thread()) {
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
        return listener_count_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::size_t connection_count() const noexcept {
        return connection_count_.load(std::memory_order_acquire);
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
        tcp_server_shard *server{nullptr};
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
        tcp_server_shard *server{nullptr};
        std::unique_ptr<tcp_connection> connection;
        std::uint32_t slot{0};
        std::uint32_t generation{0};
        bool occupied{false};
        bool retired{false};
    };

    struct connection_slot_ref {
        std::uint32_t slot{0};
        std::uint32_t generation{0};
    };

    [[nodiscard]] af::thread_ref current_owner_thread() noexcept {
        if (!prepare_current_control_thread()) {
            return {};
        }
        return control_thread_;
    }

    [[nodiscard]] bool on_owner_io_thread() const noexcept {
        if (owner_ == nullptr || af::runtime::current() != owner_) {
            return false;
        }
        const af::runtime::thread_index index = af::runtime::current_thread_index();
        return owner_->valid_thread(index) && owner_->thread_kind_of(index) == af::thread_kind::io;
    }

    [[nodiscard]] bool on_control_thread() const noexcept {
        return on_owner_io_thread() && control_thread_.valid() &&
               af::runtime::current_thread_index() == control_thread_.index;
    }

    [[nodiscard]] bool prepare_current_control_thread() noexcept {
        if (!on_owner_io_thread()) {
            return false;
        }
        const af::thread_ref current_thread(af::runtime::current_thread_index());
        if (!control_thread_) {
            control_thread_ = current_thread;
            return true;
        }
        return control_thread_ == current_thread;
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

    [[nodiscard]] static tcp_server_config normalize_config(tcp_server_config config) noexcept {
        const tcp_connection_config defaults{};
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
        if (config.connection_close_timeout.count() < 0) {
            config.connection_close_timeout = std::chrono::milliseconds(0);
        }
        return config;
    }

    [[nodiscard]] tcp_connection_config
    connection_config_for_listener(const tcp_listener_config &config) const noexcept {
        const tcp_listener_options defaults{};
        tcp_connection_config connection = config_.connection;
        if (config.options.read_buffer_size != defaults.read_buffer_size) {
            connection.read_buffer_size = config.options.read_buffer_size;
        }
        if (config.options.read_budget_bytes != defaults.read_budget_bytes) {
            connection.read_budget_bytes = config.options.read_budget_bytes;
        }
        if (config.options.write_budget_bytes != defaults.write_budget_bytes) {
            connection.write_budget_bytes = config.options.write_budget_bytes;
        }
        if (config.options.output_high_watermark != defaults.output_high_watermark) {
            connection.output_high_watermark = config.options.output_high_watermark;
        }
        if (config.options.no_delay != defaults.no_delay) {
            connection.no_delay = config.options.no_delay;
        }
        if (config.options.keepalive != defaults.keepalive) {
            connection.keepalive = config.options.keepalive;
        }
        return connection;
    }

    void begin_user_callback() noexcept {
        ++user_callback_depth_;
    }

    void end_user_callback() noexcept {
        if (user_callback_depth_ > 0U) {
            --user_callback_depth_;
        }
    }

    static void on_connection_callback_begin(void *owner) noexcept {
        auto *entry = static_cast<connection_entry *>(owner);
        if (entry != nullptr && entry->server != nullptr) {
            entry->server->begin_user_callback();
        }
    }

    static void on_connection_callback_end(void *owner) noexcept {
        auto *entry = static_cast<connection_entry *>(owner);
        if (entry != nullptr && entry->server != nullptr) {
            entry->server->end_user_callback();
        }
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

    [[nodiscard]] listener_id next_listener_id() noexcept {
        const std::uint32_t generation = next_listener_generation();
        const std::uint32_t slot = free_listener_slots_.empty()
                                       ? static_cast<std::uint32_t>(entries_.size())
                                       : free_listener_slots_.back();
        return listener_id{slot, generation};
    }

    [[nodiscard]] bool acquire_listener_slot(listener_id id) {
        if (!id.valid()) {
            return false;
        }
        if (id.slot >= entries_.size()) {
            entries_.resize(static_cast<std::size_t>(id.slot) + 1U);
        }
        if (entries_[id.slot] != nullptr && entries_[id.slot]->occupied()) {
            return false;
        }
        entries_[id.slot] = std::make_unique<listener_entry>();
        erase_free_listener_slot(id.slot);
        return true;
    }

    void erase_free_listener_slot(std::uint32_t slot) noexcept {
        for (std::size_t i = 0; i < free_listener_slots_.size(); ++i) {
            if (free_listener_slots_[i] != slot) {
                continue;
            }
            free_listener_slots_[i] = free_listener_slots_.back();
            free_listener_slots_.pop_back();
            return;
        }
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
            callbacks.on_accept = &tcp_server_shard::on_listener_accept;
            callbacks.on_error = &tcp_server_shard::on_listener_error;
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
        if (!on_control_thread()) {
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
            lifecycle.on_inactive = &tcp_server_shard::on_connection_inactive;
            lifecycle.on_callback_begin = &tcp_server_shard::on_connection_callback_begin;
            lifecycle.on_callback_end = &tcp_server_shard::on_connection_callback_end;
            const tcp_connection_config connection_config =
                connection_config_for_listener(listener.config);
            entry->connection = std::make_unique<tcp_connection>(
                *owner_, source.owner_thread(), owned_fd, entry->slot, entry->generation,
                source.local_endpoint(), peer_endpoint, connection_config,
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
            begin_user_callback();
            listener.connection_callbacks.on_accept(listener.connection_callbacks.owner,
                                                    tcp_connection_ref(entry->connection.get()));
            end_user_callback();
        }
        if (entry->connection == nullptr || !entry->connection->alive()) {
            retire_connection_slot(entry->slot, entry->generation);
        }
        reap_retired_connections_if_safe();
        return true;
    }

    [[nodiscard]] af::runtime &runtime_owner() noexcept override {
        return *owner_;
    }

    [[nodiscard]] connection_entry *find_connection_entry(std::uint32_t slot,
                                                          std::uint32_t generation) noexcept {
        if (!on_control_thread() || slot >= connections_.size()) {
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
            retire_connection_slot(slot, generation);
            reap_retired_connections_if_safe();
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
            retire_connection_slot(slot, generation);
            reap_retired_connections_if_safe();
        }
        return result;
    }

    [[nodiscard]] bool pause_connection_read(std::uint32_t slot,
                                             std::uint32_t generation) noexcept override {
        connection_entry *entry = find_connection_entry(slot, generation);
        return entry != nullptr && entry->connection->pause_read();
    }

    [[nodiscard]] bool resume_connection_read(std::uint32_t slot,
                                              std::uint32_t generation) noexcept override {
        connection_entry *entry = find_connection_entry(slot, generation);
        return entry != nullptr && entry->connection->resume_read();
    }

    [[nodiscard]] bool set_connection_no_delay(std::uint32_t slot, std::uint32_t generation,
                                               bool enabled) noexcept override {
        connection_entry *entry = find_connection_entry(slot, generation);
        return entry != nullptr && entry->connection->set_no_delay(enabled);
    }

    [[nodiscard]] bool set_connection_keepalive(std::uint32_t slot, std::uint32_t generation,
                                                bool enabled) noexcept override {
        connection_entry *entry = find_connection_entry(slot, generation);
        return entry != nullptr && entry->connection->set_keepalive(enabled);
    }

    [[nodiscard]] bool close_connection(std::uint32_t slot, std::uint32_t generation,
                                        close_reason reason) noexcept override {
        connection_entry *entry = find_connection_entry(slot, generation);
        if (entry == nullptr) {
            return false;
        }
        entry->connection->close(reason);
        retire_connection_slot(slot, generation);
        reap_retired_connections_if_safe();
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
            retire_connection_slot(slot, generation);
            reap_retired_connections_if_safe();
        }
        return true;
    }

    [[nodiscard]] bool begin_connection_stop(std::uint64_t generation) noexcept {
        if (connection_count_ == 0U) {
            stopping_connections_ = false;
            return true;
        }

        stopping_connections_ = true;
        close_connections_after_flush();
        if (connection_count_ == 0U) {
            stopping_connections_ = false;
            return true;
        }

        if (config_.connection_close_timeout.count() <= 0) {
            force_close_stopping_connections(generation);
            return true;
        }
        if (!schedule_stop_force_close(generation)) {
            force_close_stopping_connections(generation);
            return false;
        }
        return true;
    }

    void close_connections_after_flush() noexcept {
        for (auto &entry : connections_) {
            if (entry == nullptr || !entry->occupied || entry->retired ||
                entry->connection == nullptr) {
                continue;
            }
            const std::uint32_t slot = entry->slot;
            const std::uint32_t generation = entry->generation;
            const bool closed = entry->connection->close_after_flush();
            if (closed) {
                retire_connection_slot(slot, generation);
            }
        }
        reap_retired_connections_if_safe();
    }

    [[nodiscard]] bool schedule_stop_force_close(std::uint64_t generation) noexcept {
        const af::thread_ref target_thread(af::runtime::current_thread_index());
        const std::weak_ptr<detail::tcp_connection_handle_state> weak_state = handle_state_;
        tcp_server_shard *const self = this;
        return owner_->schedule_after(
            target_thread, config_.connection_close_timeout, [weak_state, self, generation] {
                const std::shared_ptr<detail::tcp_connection_handle_state> state =
                    weak_state.lock();
                if (state == nullptr) {
                    return;
                }
                if (state->owner.load(std::memory_order_acquire) != self) {
                    return;
                }
                self->force_close_stopping_connections(generation);
            });
    }

    void force_close_stopping_connections(std::uint64_t generation) noexcept {
        if (generation != stop_generation_ || !stopping_connections_) {
            return;
        }
        close_all_connections();
        stopping_connections_ = false;
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

    [[nodiscard]] std::uint32_t next_connection_generation() noexcept {
        std::uint32_t generation = next_connection_generation_++;
        if (generation == 0U) {
            generation = next_connection_generation_++;
        }
        return generation;
    }

    void retire_connection_slot(std::uint32_t slot, std::uint32_t generation) noexcept {
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

    void reap_retired_connections() noexcept {
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
        if (stopping_connections_ && connection_count_ == 0U) {
            stopping_connections_ = false;
        }
    }

    void reap_retired_connections_if_safe() noexcept {
        if (user_callback_depth_ == 0U) {
            reap_retired_connections();
        }
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
        retired_connection_slots_.clear();
        connection_count_ = 0;
    }

    static void on_connection_inactive(void *owner, tcp_connection &connection) noexcept {
        auto *entry = static_cast<connection_entry *>(owner);
        if (entry == nullptr || entry->server == nullptr || !entry->occupied ||
            entry->generation != connection.generation()) {
            return;
        }
        entry->server->retire_connection_slot(entry->slot, entry->generation);
        entry->server->reap_retired_connections_if_safe();
    }

    af::runtime *owner_{nullptr};
    tcp_server_config config_;
    std::shared_ptr<detail::tcp_connection_handle_state> handle_state_;
    af::thread_ref control_thread_{};
    std::vector<std::unique_ptr<listener_entry>> entries_;
    std::vector<std::unique_ptr<connection_entry>> connections_;
    std::vector<std::uint32_t> free_listener_slots_;
    std::vector<std::uint32_t> free_connection_slots_;
    std::vector<connection_slot_ref> retired_connection_slots_;
    std::uint32_t next_listener_generation_{1};
    std::uint32_t next_connection_generation_{1};
    std::atomic<std::size_t> listener_count_{0};
    std::atomic<std::size_t> connection_count_{0};
    std::uint64_t stop_generation_{0};
    std::uint32_t user_callback_depth_{0};
    bool stopping_connections_{false};
    bool running_{false};
};

class tcp_server {
public:
    explicit tcp_server(af::runtime &owner, tcp_server_config config = {})
        : owner_(&owner), config_(normalize_config(std::move(config))) {}

    tcp_server(const tcp_server &) = delete;
    tcp_server &operator=(const tcp_server &) = delete;

    ~tcp_server() {
        if (running_ && on_control_thread()) {
            static_cast<void>(stop());
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
        return add_listener_impl(std::move(config), callbacks, tcp_connection_callbacks{}, false);
    }

    [[nodiscard]] listener_result add_listener_connection(tcp_listener_config config,
                                                          tcp_connection_callbacks callbacks) {
        return add_listener_impl(std::move(config), tcp_listener_callbacks{}, callbacks, true);
    }

    [[nodiscard]] bool start() noexcept {
        if (running_) {
            return false;
        }
        if (!prepare_current_control_thread()) {
            return false;
        }

        running_ = true;
        bool ok = true;
        for (auto &record_ptr : listeners_) {
            if (record_ptr == nullptr || !record_ptr->occupied()) {
                continue;
            }
            record_ptr->state = listener_state::starting;
            if (!start_record(*record_ptr)) {
                record_ptr->state = listener_state::failed;
                record_ptr->last_error = EIO;
                ok = false;
            }
        }
        return ok;
    }

    [[nodiscard]] bool stop() noexcept {
        if (!running_) {
            return false;
        }
        if (!on_control_thread()) {
            return false;
        }

        running_ = false;
        bool ok = true;
        for (auto &record_ptr : listeners_) {
            if (record_ptr != nullptr && record_ptr->occupied() &&
                record_ptr->state == listener_state::active) {
                record_ptr->state = listener_state::configured;
            }
        }
        for (auto &shard_record : shards_) {
            if (shard_record.shard == nullptr) {
                continue;
            }
            if (is_current_thread(shard_record.thread)) {
                if (shard_record.shard->running() && !shard_record.shard->stop()) {
                    ok = false;
                }
                continue;
            }
            auto shard = shard_record.shard;
            if (!owner_->post(shard_record.thread, [shard]() {
                    if (shard->running()) {
                        static_cast<void>(shard->stop());
                    }
                })) {
                ok = false;
            }
        }
        return ok;
    }

    [[nodiscard]] bool remove_listener(
        tcp_listener_handle handle,
        remove_listener_policy policy = remove_listener_policy::stop_accept_only) noexcept {
        if (!on_control_thread()) {
            return false;
        }
        listener_record *record = find_record(handle);
        if (record == nullptr) {
            return false;
        }

        bool ok = true;
        for (const af::thread_ref thread : record->threads) {
            std::shared_ptr<tcp_server_shard> shard = find_shard(thread);
            if (shard == nullptr) {
                continue;
            }
            if (is_current_thread(thread)) {
                if (!shard->remove_listener(handle, policy)) {
                    ok = false;
                }
                continue;
            }
            if (!owner_->post(thread, [shard, handle, policy]() {
                    static_cast<void>(shard->remove_listener(handle, policy));
                })) {
                ok = false;
            }
        }
        release_listener_slot(handle.slot());
        return ok;
    }

    [[nodiscard]] bool running() const noexcept {
        return running_;
    }

    [[nodiscard]] std::size_t listener_count() const noexcept {
        return listener_count_;
    }

    [[nodiscard]] std::size_t connection_count() const noexcept {
        std::size_t total = 0;
        for (const auto &shard_record : shards_) {
            if (shard_record.shard != nullptr) {
                total += shard_record.shard->connection_count();
            }
        }
        return total;
    }

    [[nodiscard]] listener_state state(tcp_listener_handle handle) const noexcept {
        const listener_record *record = find_record(handle);
        if (record == nullptr) {
            return listener_state::removed;
        }
        const std::shared_ptr<tcp_server_shard> shard = find_current_shard();
        if (shard != nullptr) {
            const listener_state shard_state = shard->state(handle);
            if (shard_state != listener_state::removed) {
                return shard_state;
            }
        }
        return record->state;
    }

    [[nodiscard]] int last_error(tcp_listener_handle handle) const noexcept {
        const listener_record *record = find_record(handle);
        if (record == nullptr) {
            return ENOENT;
        }
        const std::shared_ptr<tcp_server_shard> shard = find_current_shard();
        if (shard != nullptr) {
            const int error = shard->last_error(handle);
            if (error != ENOENT) {
                return error;
            }
        }
        return record->last_error;
    }

    [[nodiscard]] const tcp_endpoint *local_endpoint(tcp_listener_handle handle) const noexcept {
        const listener_record *record = find_record(handle);
        if (record == nullptr) {
            return nullptr;
        }
        const std::shared_ptr<tcp_server_shard> shard = find_current_shard();
        if (shard != nullptr) {
            const tcp_endpoint *endpoint = shard->local_endpoint(handle);
            if (endpoint != nullptr) {
                return endpoint;
            }
        }
        return record->static_endpoint_available ? &record->static_endpoint : nullptr;
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
    struct listener_record {
        listener_id id{};
        listener_state state{listener_state::removed};
        tcp_listener_config config;
        tcp_listener_callbacks raw_callbacks{};
        tcp_connection_callbacks connection_callbacks{};
        std::vector<af::thread_ref> threads;
        tcp_endpoint static_endpoint;
        int last_error{0};
        bool static_endpoint_available{false};
        bool connection_mode{false};

        [[nodiscard]] bool occupied() const noexcept {
            return id.valid();
        }
    };

    struct shard_record {
        af::thread_ref thread{};
        std::shared_ptr<tcp_server_shard> shard;
    };

    [[nodiscard]] static tcp_server_config normalize_config(tcp_server_config config) noexcept {
        const tcp_connection_config defaults{};
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
        if (config.connection_close_timeout.count() < 0) {
            config.connection_close_timeout = std::chrono::milliseconds(0);
        }
        return config;
    }

    [[nodiscard]] listener_result add_listener_impl(tcp_listener_config config,
                                                    tcp_listener_callbacks raw_callbacks,
                                                    tcp_connection_callbacks connection_callbacks,
                                                    bool connection_mode) {
        const af::thread_ref control_thread = current_control_thread();
        if (!control_thread) {
            return listener_result::failure(EPERM);
        }

        std::vector<af::thread_ref> threads;
        const int thread_error = normalize_listener_threads(config, threads);
        if (thread_error != 0) {
            return listener_result::failure(thread_error);
        }

        const listener_id id = next_listener_id();
        try {
            if (!acquire_listener_slot(id)) {
                return listener_result::failure(EEXIST);
            }
            listener_record &record = *listeners_[id.slot];
            record.id = id;
            record.state = listener_state::configured;
            record.config = std::move(config);
            record.raw_callbacks = raw_callbacks;
            record.connection_callbacks = connection_callbacks;
            record.threads = std::move(threads);
            record.static_endpoint = record.config.endpoint;
            record.static_endpoint_available =
                record.config.endpoint.family != address_family::unix_domain &&
                record.config.endpoint.port != 0U;
            record.connection_mode = connection_mode;
            ++listener_count_;
        } catch (...) {
            release_listener_slot(id.slot);
            return listener_result::failure(ENOMEM);
        }

        listener_record &record = *listeners_[id.slot];
        if (running_) {
            record.state = listener_state::starting;
            if (!start_record(record)) {
                release_listener_slot(id.slot);
                return listener_result::failure(EIO);
            }
        }
        return listener_result::success(tcp_listener_handle{id});
    }

    [[nodiscard]] bool on_owner_io_thread() const noexcept {
        if (owner_ == nullptr || af::runtime::current() != owner_) {
            return false;
        }
        const af::runtime::thread_index index = af::runtime::current_thread_index();
        return owner_->valid_thread(index) && owner_->thread_kind_of(index) == af::thread_kind::io;
    }

    [[nodiscard]] bool on_control_thread() const noexcept {
        return on_owner_io_thread() && control_thread_.valid() &&
               af::runtime::current_thread_index() == control_thread_.index;
    }

    [[nodiscard]] bool prepare_current_control_thread() noexcept {
        if (!on_owner_io_thread()) {
            return false;
        }
        const af::thread_ref current_thread(af::runtime::current_thread_index());
        if (!control_thread_) {
            control_thread_ = current_thread;
            return true;
        }
        return control_thread_ == current_thread;
    }

    [[nodiscard]] af::thread_ref current_control_thread() noexcept {
        if (!prepare_current_control_thread()) {
            return {};
        }
        return control_thread_;
    }

    [[nodiscard]] bool is_current_thread(af::thread_ref thread) const noexcept {
        return af::runtime::current() == owner_ &&
               af::runtime::current_thread_index() == thread.index;
    }

    [[nodiscard]] int normalize_listener_threads(const tcp_listener_config &config,
                                                 std::vector<af::thread_ref> &threads) const {
        if (owner_ == nullptr) {
            return EINVAL;
        }
        if (config.threads.empty()) {
            const af::thread_group_ref io_threads = owner_->io_threads();
            if (io_threads.empty()) {
                return EINVAL;
            }
            threads.reserve(io_threads.size());
            for (const std::uint16_t index : io_threads) {
                threads.emplace_back(index);
            }
        } else {
            threads = config.threads;
        }

        std::size_t write = 0;
        for (const af::thread_ref thread : threads) {
            if (!thread || !owner_->valid_thread(thread) ||
                owner_->thread_kind_of(thread) != af::thread_kind::io) {
                return EINVAL;
            }
            bool duplicate = false;
            for (std::size_t i = 0; i < write; ++i) {
                if (threads[i] == thread) {
                    duplicate = true;
                    break;
                }
            }
            if (!duplicate) {
                threads[write++] = thread;
            }
        }
        threads.resize(write);
        if (threads.empty()) {
            return EINVAL;
        }

        if (threads.size() > 1U) {
            if (config.endpoint.family == address_family::unix_domain) {
                return EOPNOTSUPP;
            }
            if (!config.options.reuse_port ||
                config.accept_strategy == accept_strategy::single_acceptor) {
                return EOPNOTSUPP;
            }
            if (config.endpoint.port == 0U) {
                return EINVAL;
            }
        }
        return 0;
    }

    [[nodiscard]] bool start_record(listener_record &record) noexcept {
        bool ok = true;
        for (const af::thread_ref thread : record.threads) {
            if (!start_record_on_thread(record, thread)) {
                ok = false;
            }
        }
        if (ok) {
            record.state = listener_state::active;
            record.last_error = 0;
        }
        return ok;
    }

    [[nodiscard]] bool start_record_on_thread(listener_record &record,
                                              af::thread_ref thread) noexcept {
        std::shared_ptr<tcp_server_shard> shard = ensure_shard(thread);
        if (shard == nullptr) {
            return false;
        }

        tcp_listener_config config = record.config;
        config.threads.clear();
        config.threads.push_back(thread);
        const listener_id id = record.id;
        const tcp_listener_callbacks raw_callbacks = record.raw_callbacks;
        const tcp_connection_callbacks connection_callbacks = record.connection_callbacks;
        const bool connection_mode = record.connection_mode;

        if (is_current_thread(thread)) {
            if (!shard->running() && !shard->start()) {
                return false;
            }
            const listener_result result =
                connection_mode
                    ? shard->add_listener_connection_with_id(id, std::move(config),
                                                             connection_callbacks)
                    : shard->add_listener_raw_with_id(id, std::move(config), raw_callbacks);
            return result.ok();
        }

        return owner_->post(thread, [shard, id, config = std::move(config), raw_callbacks,
                                     connection_callbacks, connection_mode]() mutable {
            if (!shard->running()) {
                static_cast<void>(shard->start());
            }
            const listener_result result =
                connection_mode
                    ? shard->add_listener_connection_with_id(id, std::move(config),
                                                             connection_callbacks)
                    : shard->add_listener_raw_with_id(id, std::move(config), raw_callbacks);
            static_cast<void>(result);
        });
    }

    [[nodiscard]] std::shared_ptr<tcp_server_shard> ensure_shard(af::thread_ref thread) {
        std::shared_ptr<tcp_server_shard> shard = find_shard(thread);
        if (shard != nullptr) {
            return shard;
        }
        try {
            shard = std::make_shared<tcp_server_shard>(*owner_, config_);
            shards_.push_back(shard_record{thread, shard});
            return shard;
        } catch (...) {
            return nullptr;
        }
    }

    [[nodiscard]] std::shared_ptr<tcp_server_shard> find_shard(af::thread_ref thread) noexcept {
        for (auto &record : shards_) {
            if (record.thread == thread) {
                return record.shard;
            }
        }
        return nullptr;
    }

    [[nodiscard]] std::shared_ptr<tcp_server_shard>
    find_shard(af::thread_ref thread) const noexcept {
        for (const auto &record : shards_) {
            if (record.thread == thread) {
                return record.shard;
            }
        }
        return nullptr;
    }

    [[nodiscard]] std::shared_ptr<tcp_server_shard> find_current_shard() const noexcept {
        if (owner_ == nullptr || af::runtime::current() != owner_) {
            return nullptr;
        }
        return find_shard(af::thread_ref(af::runtime::current_thread_index()));
    }

    [[nodiscard]] listener_id next_listener_id() noexcept {
        const std::uint32_t generation = next_listener_generation();
        const std::uint32_t slot = free_listener_slots_.empty()
                                       ? static_cast<std::uint32_t>(listeners_.size())
                                       : free_listener_slots_.back();
        return listener_id{slot, generation};
    }

    [[nodiscard]] std::uint32_t next_listener_generation() noexcept {
        std::uint32_t generation = next_listener_generation_++;
        if (generation == 0U) {
            generation = next_listener_generation_++;
        }
        return generation;
    }

    [[nodiscard]] bool acquire_listener_slot(listener_id id) {
        if (!id.valid()) {
            return false;
        }
        if (id.slot >= listeners_.size()) {
            listeners_.resize(static_cast<std::size_t>(id.slot) + 1U);
        }
        if (listeners_[id.slot] != nullptr && listeners_[id.slot]->occupied()) {
            return false;
        }
        listeners_[id.slot] = std::make_unique<listener_record>();
        erase_free_listener_slot(id.slot);
        return true;
    }

    void release_listener_slot(std::uint32_t slot) noexcept {
        if (slot >= listeners_.size() || listeners_[slot] == nullptr ||
            !listeners_[slot]->occupied()) {
            return;
        }
        listeners_[slot].reset();
        --listener_count_;
        try {
            free_listener_slots_.push_back(slot);
        } catch (...) {
        }
    }

    void erase_free_listener_slot(std::uint32_t slot) noexcept {
        for (std::size_t i = 0; i < free_listener_slots_.size(); ++i) {
            if (free_listener_slots_[i] != slot) {
                continue;
            }
            free_listener_slots_[i] = free_listener_slots_.back();
            free_listener_slots_.pop_back();
            return;
        }
    }

    [[nodiscard]] listener_record *find_record(tcp_listener_handle handle) noexcept {
        if (!handle.valid() || handle.slot() >= listeners_.size()) {
            return nullptr;
        }
        if (listeners_[handle.slot()] == nullptr) {
            return nullptr;
        }
        listener_record &record = *listeners_[handle.slot()];
        if (!record.occupied() || record.id != handle.id) {
            return nullptr;
        }
        return &record;
    }

    [[nodiscard]] const listener_record *find_record(tcp_listener_handle handle) const noexcept {
        if (!handle.valid() || handle.slot() >= listeners_.size()) {
            return nullptr;
        }
        if (listeners_[handle.slot()] == nullptr) {
            return nullptr;
        }
        const listener_record &record = *listeners_[handle.slot()];
        if (!record.occupied() || record.id != handle.id) {
            return nullptr;
        }
        return &record;
    }

    af::runtime *owner_{nullptr};
    tcp_server_config config_;
    af::thread_ref control_thread_{};
    std::vector<std::unique_ptr<listener_record>> listeners_;
    std::vector<std::uint32_t> free_listener_slots_;
    std::vector<shard_record> shards_;
    std::uint32_t next_listener_generation_{1};
    std::size_t listener_count_{0};
    bool running_{false};
};

} // namespace af::net

#include "af/net/detail/tcp_connection_handle_runtime_impl.hpp"
