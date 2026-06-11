#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "af/net/tcp_endpoint.hpp"
#include "af/net/detail/tcp_server_shard.hpp"
#include "af/net/tcp_types.hpp"
#include "af/runtime.hpp"
#include "af/thread_kind.hpp"

namespace af::net {

class tcp_server {
public:
    explicit tcp_server(af::runtime &owner, tcp_server_config config = {})
        : owner_(&owner), config_(normalize_config(std::move(config))) {}

    tcp_server(const tcp_server &) = delete;
    tcp_server &operator=(const tcp_server &) = delete;

    ~tcp_server() {
        if (running_ && on_owner_io_thread()) {
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
        if (!on_owner_io_thread()) {
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
        if (!on_owner_io_thread()) {
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
        if (!on_owner_io_thread()) {
            return false;
        }
        listener_record *record = find_record(handle);
        if (record == nullptr) {
            return false;
        }

        bool ok = true;
        for (const af::thread_ref thread : record->threads) {
            std::shared_ptr<detail::tcp_server_shard> shard = find_shard(thread);
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
        const std::shared_ptr<detail::tcp_server_shard> shard = find_current_shard();
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
        const std::shared_ptr<detail::tcp_server_shard> shard = find_current_shard();
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
        const std::shared_ptr<detail::tcp_server_shard> shard = find_current_shard();
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
        std::shared_ptr<detail::tcp_server_shard> shard;
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
        if (!on_owner_io_thread()) {
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
                config.accept_strategy == tcp_accept_strategy::single_acceptor) {
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
        std::shared_ptr<detail::tcp_server_shard> shard = ensure_shard(thread);
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

    [[nodiscard]] std::shared_ptr<detail::tcp_server_shard> ensure_shard(af::thread_ref thread) {
        std::shared_ptr<detail::tcp_server_shard> shard = find_shard(thread);
        if (shard != nullptr) {
            return shard;
        }
        try {
            shard = std::make_shared<detail::tcp_server_shard>(*owner_, config_);
            shards_.push_back(shard_record{thread, shard});
            return shard;
        } catch (...) {
            return nullptr;
        }
    }

    [[nodiscard]] std::shared_ptr<detail::tcp_server_shard>
    find_shard(af::thread_ref thread) noexcept {
        for (auto &record : shards_) {
            if (record.thread == thread) {
                return record.shard;
            }
        }
        return nullptr;
    }

    [[nodiscard]] std::shared_ptr<detail::tcp_server_shard>
    find_shard(af::thread_ref thread) const noexcept {
        for (const auto &record : shards_) {
            if (record.thread == thread) {
                return record.shard;
            }
        }
        return nullptr;
    }

    [[nodiscard]] std::shared_ptr<detail::tcp_server_shard> find_current_shard() const noexcept {
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
    std::vector<std::unique_ptr<listener_record>> listeners_;
    std::vector<std::uint32_t> free_listener_slots_;
    std::vector<shard_record> shards_;
    std::uint32_t next_listener_generation_{1};
    std::size_t listener_count_{0};
    bool running_{false};
};

} // namespace af::net

#include "af/net/detail/tcp_connection_handle_runtime_impl.hpp"
