#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "af/detail/config.hpp"
#include "af/net/tcp_endpoint.hpp"
#include "af/net/tcp_listener.hpp"
#include "af/net/tcp_types.hpp"
#include "af/runtime.hpp"
#include "af/thread_kind.hpp"

#include <sys/socket.h>
#include <unistd.h>

namespace af::net {

class tcp_server {
public:
    explicit tcp_server(af::runtime &owner, tcp_server_config config = {})
        : owner_(&owner), config_(std::move(config)) {}

    tcp_server(const tcp_server &) = delete;
    tcp_server &operator=(const tcp_server &) = delete;

    ~tcp_server() {
        if (running_) {
            AF_ASSERT(on_owner_io_thread() &&
                      "tcp_server must be stopped on the owner reactor thread before destruction");
            if (on_owner_io_thread()) {
                static_cast<void>(stop());
            }
        }
    }

    [[nodiscard]] listener_result add_listener(tcp_listener_config config,
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
            listener_entry &entry = entries_[id.slot];
            entry.id = id;
            entry.state = listener_state::configured;
            entry.config = std::move(config);
            entry.callbacks = callbacks;
            entry.owner_thread = owner_thread;
            ++listener_count_;
        } catch (...) {
            return listener_result::failure(ENOMEM);
        }

        listener_entry &entry = entries_[id.slot];
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
        bool ok = true;
        for (listener_entry &entry : entries_) {
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

        bool ok = true;
        for (listener_entry &entry : entries_) {
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
    struct listener_entry {
        listener_id id{};
        listener_state state{listener_state::removed};
        tcp_listener_config config;
        tcp_listener_callbacks callbacks{};
        std::unique_ptr<tcp_listener> listener;
        af::thread_ref owner_thread{};
        int last_error{0};

        [[nodiscard]] bool occupied() const noexcept {
            return id.valid();
        }
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
            entries_[slot] = listener_entry{};
            return listener_id{slot, generation};
        }
        const std::uint32_t slot = static_cast<std::uint32_t>(entries_.size());
        entries_.push_back(listener_entry{});
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
        if (slot >= entries_.size() || !entries_[slot].occupied()) {
            return;
        }
        listener_entry &entry = entries_[slot];
        entry.listener.reset();
        entry = listener_entry{};
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
        if (entry != nullptr && entry->callbacks.on_accept != nullptr) {
            entry->callbacks.on_accept(entry->callbacks.owner, listener, fd, peer, peer_size);
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
        if (entry->callbacks.on_error != nullptr) {
            entry->callbacks.on_error(entry->callbacks.owner, listener, entry->last_error);
        }
    }

    [[nodiscard]] listener_entry *find_entry(tcp_listener_handle handle) noexcept {
        if (!handle.valid() || handle.slot() >= entries_.size()) {
            return nullptr;
        }
        listener_entry &entry = entries_[handle.slot()];
        if (!entry.occupied() || entry.id != handle.id) {
            return nullptr;
        }
        return &entry;
    }

    [[nodiscard]] const listener_entry *find_entry(tcp_listener_handle handle) const noexcept {
        if (!handle.valid() || handle.slot() >= entries_.size()) {
            return nullptr;
        }
        const listener_entry &entry = entries_[handle.slot()];
        if (!entry.occupied() || entry.id != handle.id) {
            return nullptr;
        }
        return &entry;
    }

    af::runtime *owner_{nullptr};
    tcp_server_config config_;
    std::vector<listener_entry> entries_;
    std::vector<std::uint32_t> free_listener_slots_;
    std::uint32_t next_listener_generation_{1};
    std::size_t listener_count_{0};
    bool running_{false};
};

} // namespace af::net
