#pragma once

#include <atomic>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

#include "af/detail/config.hpp"
#include "af/detail/memory/object_pool.hpp"
#include "af/runtime/work.hpp"
#include "af/task.hpp"

namespace af {

class runtime;
class runtime_task;

using runtime_task_id = std::uint64_t;
inline constexpr runtime_task_id runtime_invalid_task_id = 0;

template <typename TaskT> class runtime_task_handle;

template <typename TaskT, typename... Args>
[[nodiscard]] runtime_task_handle<TaskT> make_task(runtime &owner, Args &&...args);

template <typename TaskT, typename... Args>
[[nodiscard]] runtime_task_handle<TaskT> try_make_task(runtime &owner, Args &&...args) noexcept;

class runtime_task : public runtime_work {
public:
    using task_id_type = runtime_task_id;
    using destroy_fn = void (*)(runtime_task *) noexcept;
    static constexpr task_id_type invalid_task_id = runtime_invalid_task_id;

    class factory_token {
    public:
        factory_token(const factory_token &) noexcept = default;
        factory_token &operator=(const factory_token &) = delete;

    private:
        constexpr factory_token() noexcept = default;

        template <typename TaskT, typename... Args>
        friend runtime_task_handle<TaskT> make_task(runtime &owner, Args &&...args);

        template <typename TaskT, typename... Args>
        friend runtime_task_handle<TaskT> try_make_task(runtime &owner, Args &&...args) noexcept;
    };

    runtime_task() = delete;
    runtime_task(const runtime_task &) = delete;
    runtime_task &operator=(const runtime_task &) = delete;
    ~runtime_task() override = default;

    [[nodiscard]] task_id_type task_id() const noexcept {
        return task_id_;
    }

    [[nodiscard]] runtime &owner() noexcept {
        return *owner_;
    }

    [[nodiscard]] const runtime &owner() const noexcept {
        return *owner_;
    }

protected:
    explicit runtime_task(factory_token, runtime &owner) noexcept;

    [[nodiscard]] bool schedule(std::uint16_t thread) noexcept {
        return schedule_to(thread);
    }

    [[nodiscard]] bool schedule_to(std::uint16_t thread) noexcept;

    task_result pending_to(std::uint16_t thread) noexcept {
        return schedule_to(thread) ? task_result::pending : task_result::cancelled;
    }

    task_result pending(std::uint16_t thread) noexcept {
        return pending_to(thread);
    }

    [[nodiscard]] static constexpr task_result done() noexcept {
        return task_result::done;
    }

    [[nodiscard]] static constexpr task_result pending() noexcept {
        return task_result::pending;
    }

    [[nodiscard]] static constexpr task_result again() noexcept {
        return task_result::again;
    }

    [[nodiscard]] static constexpr task_result failed() noexcept {
        return task_result::failed;
    }

    [[nodiscard]] static constexpr task_result cancelled() noexcept {
        return task_result::cancelled;
    }

private:
    static constexpr std::uint32_t no_requested_thread = std::numeric_limits<std::uint32_t>::max();
    static constexpr task_id_type task_id_block_size = 1024;

    virtual task_result run_task() noexcept = 0;

    void run(runtime &owner) noexcept final;
    void set_destroy_fn(destroy_fn destroy) noexcept {
        destroy_ = destroy;
    }

    void release_handle() noexcept {
        release_lifetime_ref();
    }

    void add_lifetime_ref() noexcept {
        lifetime_refs_.fetch_add(1, std::memory_order_relaxed);
    }

    void release_lifetime_ref() noexcept {
        if (lifetime_refs_.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
            AF_ASSERT(destroy_ != nullptr);
            destroy_(this);
        }
    }

    [[nodiscard]] bool request_schedule_after_running(std::uint16_t thread) noexcept;
    [[nodiscard]] bool enqueue_from_state(task_state previous, std::uint16_t thread) noexcept;
    [[nodiscard]] bool enqueue_next_from_running(std::uint16_t thread) noexcept;
    void finish_after_run(task_result result) noexcept;

    [[nodiscard]] static task_id_type allocate_task_id() noexcept {
        thread_local task_id_type next_local_task_id = invalid_task_id;
        thread_local task_id_type local_task_id_limit = invalid_task_id;
        if (next_local_task_id == local_task_id_limit) [[unlikely]] {
            next_local_task_id =
                next_task_id_.fetch_add(task_id_block_size, std::memory_order_relaxed);
            local_task_id_limit = next_local_task_id + task_id_block_size;
        }
        return next_local_task_id++;
    }

    runtime *owner_{nullptr};
    std::atomic<task_state> state_{task_state::created};
    std::atomic<std::uint32_t> requested_thread_{no_requested_thread};
    std::atomic<std::uint32_t> lifetime_refs_{1};
    alignas(detail::hardware_cache_line_size) static inline std::atomic<task_id_type> next_task_id_{
        1};
    task_id_type task_id_{invalid_task_id};
    destroy_fn destroy_{nullptr};

    template <typename TaskT> friend class runtime_task_handle;

    template <typename TaskT, typename... Args>
    friend runtime_task_handle<TaskT> make_task(runtime &owner, Args &&...args);

    template <typename TaskT, typename... Args>
    friend runtime_task_handle<TaskT> try_make_task(runtime &owner, Args &&...args) noexcept;
};

template <typename TaskT> class [[nodiscard]] runtime_task_handle {
public:
    runtime_task_handle() noexcept = default;
    explicit runtime_task_handle(TaskT *task) noexcept : task_(task) {}

    runtime_task_handle(const runtime_task_handle &) = delete;
    runtime_task_handle &operator=(const runtime_task_handle &) = delete;

    runtime_task_handle(runtime_task_handle &&other) noexcept
        : task_(std::exchange(other.task_, nullptr)) {}

    runtime_task_handle &operator=(runtime_task_handle &&other) noexcept {
        if (this != &other) {
            reset();
            task_ = std::exchange(other.task_, nullptr);
        }
        return *this;
    }

    ~runtime_task_handle() {
        reset();
    }

    [[nodiscard]] TaskT *get() const noexcept {
        return task_;
    }

    [[nodiscard]] TaskT &operator*() const noexcept {
        AF_ASSERT(task_ != nullptr);
        return *task_;
    }

    [[nodiscard]] TaskT *operator->() const noexcept {
        AF_ASSERT(task_ != nullptr);
        return task_;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return task_ != nullptr;
    }

    void reset() noexcept {
        if (task_ != nullptr) {
            task_->release_handle();
            task_ = nullptr;
        }
    }

private:
    TaskT *task_{nullptr};
};

namespace detail {

template <typename TaskT> using RuntimeTaskPool = ObjectPool<TaskT, 4096, 64, false, 1, 4, 256>;

template <typename TaskT> [[nodiscard]] RuntimeTaskPool<TaskT> &runtime_task_pool() {
    static RuntimeTaskPool<TaskT> pool;
    return pool;
}

template <typename TaskT> void destroy_runtime_task(runtime_task *task) noexcept {
    runtime_task_pool<TaskT>().destroy(static_cast<TaskT *>(task));
}

} // namespace detail

template <typename TaskT, typename... Args>
runtime_task_handle<TaskT> make_task(runtime &owner, Args &&...args) {
    static_assert(std::is_base_of_v<runtime_task, TaskT>,
                  "TaskT must derive from af::runtime_task");
    auto *task = detail::runtime_task_pool<TaskT>().create(runtime_task::factory_token{}, owner,
                                                           std::forward<Args>(args)...);
    task->set_destroy_fn(&detail::destroy_runtime_task<TaskT>);
    return runtime_task_handle<TaskT>(task);
}

template <typename TaskT, typename... Args>
runtime_task_handle<TaskT> try_make_task(runtime &owner, Args &&...args) noexcept {
    static_assert(std::is_base_of_v<runtime_task, TaskT>,
                  "TaskT must derive from af::runtime_task");
    auto *task = detail::runtime_task_pool<TaskT>().try_create(runtime_task::factory_token{}, owner,
                                                               std::forward<Args>(args)...);
    if (task == nullptr) {
        return runtime_task_handle<TaskT>();
    }
    task->set_destroy_fn(&detail::destroy_runtime_task<TaskT>);
    return runtime_task_handle<TaskT>(task);
}

} // namespace af
