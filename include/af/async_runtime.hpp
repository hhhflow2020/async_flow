#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/detail/bounded_queues.hpp"
#include "af/detail/config.hpp"
#include "af/detail/io_uring_support.hpp"
#include "af/detail/object_pool.hpp"
#include "af/detail/runtime_traits.hpp"
#include "af/task.hpp"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#endif

#if defined(__linux__)
#include <algorithm>
#include <linux/openat2.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <unistd.h>
#endif

#if !defined(__linux__)
struct open_how;
struct statx;
#endif

namespace af {

enum class ParallelMode : std::uint8_t {
    NonEmptyOnly,
    AllShards,
};

enum class OrderedBatchReplayPolicy : std::uint8_t {
    Strict,
    SkipAlreadyApplied,
};

struct OrderedBatchOptions {
    OrderedBatchReplayPolicy replay_policy{OrderedBatchReplayPolicy::Strict};
};

inline constexpr OrderedBatchOptions retryable_ordered_batch_options{
    OrderedBatchReplayPolicy::SkipAlreadyApplied};

template <typename Op>
struct ShardedOps {
    std::vector<std::vector<Op>> shards;

    explicit ShardedOps(std::uint16_t shard_count = 0) : shards(shard_count) {}

    [[nodiscard]] std::uint16_t shard_count() const noexcept {
        return static_cast<std::uint16_t>(shards.size());
    }
};

template <typename TraitsT>
class AsyncRuntime {
public:
    using Traits = TraitsT;
    using Thread = typename Traits::Thread;
    using Task = BasicTask<AsyncRuntime<Traits>>;
    using TraitConfig = detail::RuntimeTraitsConfig<Traits>;

    template <typename TaskT>
    class [[nodiscard]] TaskHandle {
    public:
        TaskHandle() noexcept = default;
        explicit TaskHandle(TaskT* task) noexcept : task_(task) {}

        TaskHandle(const TaskHandle&) = delete;
        TaskHandle& operator=(const TaskHandle&) = delete;

        TaskHandle(TaskHandle&& other) noexcept : task_(std::exchange(other.task_, nullptr)) {}

        TaskHandle& operator=(TaskHandle&& other) noexcept {
            if (this != &other) {
                reset();
                task_ = std::exchange(other.task_, nullptr);
            }
            return *this;
        }

        ~TaskHandle() {
            reset();
        }

        [[nodiscard]] TaskT* get() const noexcept {
            return task_;
        }

        [[nodiscard]] TaskT& operator*() const noexcept {
            AF_ASSERT(task_ != nullptr);
            return *task_;
        }

        [[nodiscard]] TaskT* operator->() const noexcept {
            AF_ASSERT(task_ != nullptr);
            return task_;
        }

        [[nodiscard]] explicit operator bool() const noexcept {
            return task_ != nullptr;
        }

        [[nodiscard]] bool scheduled() const noexcept {
            return task_ != nullptr && !AsyncRuntime::is_task_created(task_);
        }

        void reset() noexcept {
            if (task_ != nullptr) {
                AsyncRuntime::release_task_handle(task_);
                task_ = nullptr;
            }
        }

    private:
        TaskT* task_{nullptr};
    };

    static constexpr std::uint16_t thread_count = Traits::thread_count;
    static constexpr std::uint16_t invalid_thread_index = thread_count;
    static_assert(thread_count > 0, "AsyncRuntime requires at least one fixed thread");

    static constexpr std::size_t spsc_queue_capacity = TraitConfig::spsc_queue_capacity;
    static constexpr std::size_t external_queue_capacity = TraitConfig::external_queue_capacity;
    static constexpr QueueFullPolicy queue_full_policy = TraitConfig::queue_full_policy;
    static constexpr ShutdownPolicy shutdown_policy = TraitConfig::shutdown_policy;
    static constexpr bool task_registry_enabled = TraitConfig::task_registry_enabled;
    static constexpr unsigned io_uring_entries = TraitConfig::io_uring_entries;
    static constexpr unsigned io_uring_submit_batch_threshold =
        TraitConfig::io_uring_submit_batch_threshold;
    static constexpr unsigned io_uring_cq_entries = TraitConfig::io_uring_cq_entries;
    static constexpr unsigned io_uring_setup_flags = TraitConfig::io_uring_setup_flags;
    static constexpr bool io_uring_setup_sqpoll = TraitConfig::io_uring_setup_sqpoll;
    static constexpr unsigned io_uring_sqpoll_idle_ms =
        TraitConfig::io_uring_sqpoll_idle_ms;
    static constexpr int io_uring_sqpoll_cpu = TraitConfig::io_uring_sqpoll_cpu;
    static constexpr bool io_uring_setup_submit_all = TraitConfig::io_uring_setup_submit_all;
    static constexpr bool io_uring_setup_coop_taskrun =
        TraitConfig::io_uring_setup_coop_taskrun;
    static constexpr bool io_uring_setup_single_issuer =
        TraitConfig::io_uring_setup_single_issuer;
    static constexpr bool io_uring_setup_defer_taskrun =
        TraitConfig::io_uring_setup_defer_taskrun;
    static constexpr std::size_t io_wait_reserve = TraitConfig::io_wait_reserve;
    static constexpr std::size_t io_deferred_delete_reserve =
        TraitConfig::io_deferred_delete_reserve;
    static constexpr std::size_t io_uring_provided_buffer_group_reserve =
        TraitConfig::io_uring_provided_buffer_group_reserve;
    static_assert(spsc_queue_capacity > 0, "spsc_queue_capacity must be greater than zero");
    static_assert(external_queue_capacity > 0, "external_queue_capacity must be greater than zero");
    static_assert(io_uring_entries > 0, "io_uring_entries must be greater than zero");
    static_assert(
        std::has_single_bit(io_uring_entries),
        "io_uring_entries must be a power of two");
    static_assert(
        io_uring_submit_batch_threshold > 0,
        "io_uring_submit_batch_threshold must be greater than zero");
    static_assert(
        io_uring_submit_batch_threshold <= io_uring_entries,
        "io_uring_submit_batch_threshold must not exceed io_uring_entries");
    static_assert(
        io_uring_cq_entries == 0U || std::has_single_bit(io_uring_cq_entries),
        "io_uring_cq_entries must be zero or a power of two");
    static_assert(
        io_uring_cq_entries == 0U || io_uring_cq_entries >= io_uring_entries,
        "io_uring_cq_entries must be zero or not less than io_uring_entries");
    static_assert(
        !(io_uring_setup_sqpoll || io_uring_sqpoll_cpu >= 0) || io_uring_sqpoll_idle_ms > 0U,
        "io_uring_sqpoll_idle_ms must be greater than zero when SQPOLL is enabled");

    [[nodiscard]] static constexpr ThreadKind thread_kind(Thread thread) noexcept {
        if constexpr (requires { Traits::thread_kind(thread); }) {
            return Traits::thread_kind(thread);
        } else {
            static_cast<void>(thread);
            return ThreadKind::Worker;
        }
    }

    AsyncRuntime() = delete;

    static void init() {
        RuntimeStatus expected = RuntimeStatus::Stopped;
        if (!status_.compare_exchange_strong(
                expected,
                RuntimeStatus::Starting,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return;
        }

        ordered_batch_state_.assign(thread_count, OrderedBatchState{});
        generation_.fetch_add(1, std::memory_order_acq_rel);
        reset_task_registry();
        init_queues();
        executors_.clear();
        executors_.reserve(thread_count);
        for (std::uint16_t i = 0; i < thread_count; ++i) {
            executors_.push_back(std::make_unique<Executor>(i));
        }
        for (auto& executor : executors_) {
            executor->start();
        }
        status_.store(RuntimeStatus::Running, std::memory_order_release);
    }

    static void shutdown() {
        AF_ASSERT(!is_runtime_thread() && "shutdown must be called from a non-runtime thread");
        if (is_runtime_thread()) {
            return;
        }

        RuntimeStatus expected = RuntimeStatus::Running;
        if (!status_.compare_exchange_strong(
                expected,
                RuntimeStatus::Stopping,
                std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return;
        }

        wait_for_external_posts();
        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            wait_for_idle();
        }

        for (auto& executor : executors_) {
            executor->request_stop();
        }
        for (auto& executor : executors_) {
            executor->join();
        }
        if constexpr (shutdown_policy == ShutdownPolicy::StopImmediately) {
            cancel_registered_tasks();
        } else {
            reset_task_registry();
        }

        executors_.clear();
        spsc_queues_.clear();
        external_queues_.clear();
        ordered_batch_state_.clear();
        unfinished_tasks_.store(0, std::memory_order_release);
        unfinished_tasks_.notify_all();
        status_.store(RuntimeStatus::Stopped, std::memory_order_release);
    }

    static void wait_for_idle() noexcept {
        for (;;) {
            const std::uint32_t count = unfinished_tasks_.load(std::memory_order_acquire);
            if (count == 0) {
                return;
            }
            unfinished_tasks_.wait(count, std::memory_order_acquire);
        }
    }

    [[nodiscard]] static std::uint32_t unfinished_task_count() noexcept {
        return unfinished_tasks_.load(std::memory_order_acquire);
    }

    template <typename TaskT, typename... CtorArgs>
    [[nodiscard]] static TaskHandle<TaskT> make_task(CtorArgs&&... ctor_args) {
        static_assert(std::is_base_of_v<Task, TaskT>, "TaskT must derive from Runtime::Task");
        auto* task = allocate_task<TaskT>(std::forward<CtorArgs>(ctor_args)...);
        task->attach_start_handle();
        return TaskHandle<TaskT>(task);
    }

    template <typename TaskT, typename... CtorArgs>
    [[nodiscard]] static TaskHandle<TaskT> create_task(CtorArgs&&... ctor_args) {
        return make_task<TaskT>(std::forward<CtorArgs>(ctor_args)...);
    }

    template <typename TaskT, typename... Args>
    [[nodiscard]] static bool start_task(Args&&... args) {
        static_assert(std::is_base_of_v<Task, TaskT>, "TaskT must derive from Runtime::Task");
        auto task = make_task<TaskT>();
        using DoItResult = decltype(task.get()->do_it(std::declval<Args>()...));

        if constexpr (std::is_void_v<DoItResult>) {
            task->do_it(std::forward<Args>(args)...);
            return task.scheduled();
        } else {
            const bool ok = static_cast<bool>(task->do_it(std::forward<Args>(args)...));
            return ok && task.scheduled();
        }
    }

    static bool post(Thread thread, Task* task) noexcept {
        if (task == nullptr) {
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= thread_count) {
            AF_ASSERT(false && "invalid thread index");
            return false;
        }

        if (!try_enter_post(index)) {
            return false;
        }

        const detail::ScheduleRequest request = task->request_schedule(index);
        if (request.action == detail::ScheduleAction::Enqueue) {
            const bool first_schedule = request.previous == TaskState::Created;
            if (first_schedule) {
                on_task_started(task);
            }
            const bool enqueued = enqueue_ready_by_policy(index, task);
            if (!enqueued) {
                if (first_schedule) {
                    on_task_finished(task);
                }
                task->cancel_schedule(request.previous);
            }
            leave_post(index);
            return enqueued;
        }
        const bool deferred = request.action == detail::ScheduleAction::Deferred;
        leave_post(index);
        return deferred;
    }

    [[nodiscard]] static Thread current_thread() noexcept {
        return thread_from_index(current_thread_index_);
    }

    [[nodiscard]] static bool is_runtime_thread() noexcept {
        return current_thread_index_ < thread_count;
    }

    [[nodiscard]] static bool is_stopping() noexcept {
        return status_.load(std::memory_order_acquire) == RuntimeStatus::Stopping;
    }

    [[nodiscard]] static std::uint16_t current_thread_index() noexcept {
        return current_thread_index_;
    }

    [[nodiscard]] static constexpr std::uint16_t thread_index(Thread thread) noexcept {
        using Underlying = std::underlying_type_t<Thread>;
        return static_cast<std::uint16_t>(static_cast<Underlying>(thread));
    }

    [[nodiscard]] static constexpr Thread thread_from_index(std::uint16_t index) noexcept {
        return static_cast<Thread>(index);
    }

    template <Thread Begin, std::uint16_t Count, typename Key>
    [[nodiscard]] static constexpr Thread shard_by(Key key) noexcept {
        static_assert(Count > 0);
        const auto begin = thread_index(Begin);
        const auto value = static_cast<std::uint64_t>(key);
        const auto shard = static_cast<std::uint16_t>(value % Count);
        return thread_from_index(static_cast<std::uint16_t>(begin + shard));
    }

    template <typename Op, typename KeyFn>
    [[nodiscard]] static ShardedOps<Op> split_by_shard(
        std::vector<Op>&& ops,
        std::uint16_t shard_count,
        KeyFn&& key_fn) {
        AF_ASSERT(shard_count > 0);
        ShardedOps<Op> sharded(shard_count);
        if (shard_count == 0) {
            return sharded;
        }

        std::vector<std::size_t> shard_sizes(shard_count, 0);
        std::vector<std::uint16_t> shard_indexes;
        shard_indexes.reserve(ops.size());
        for (auto& op : ops) {
            const auto key = static_cast<std::uint64_t>(key_fn(op));
            const std::uint16_t shard = static_cast<std::uint16_t>(key % shard_count);
            shard_indexes.push_back(shard);
            ++shard_sizes[shard];
        }

        for (std::uint16_t shard = 0; shard < shard_count; ++shard) {
            sharded.shards[shard].reserve(shard_sizes[shard]);
        }

        std::size_t index = 0;
        for (auto& op : ops) {
            sharded.shards[shard_indexes[index++]].push_back(std::move(op));
        }
        return sharded;
    }

    template <typename Op, typename Handler>
    static void parallel_shards(
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl(
            std::bool_constant<false>{},
            shard_begin,
            sharded_ops,
            mode,
            0,
            OrderedBatchOptions{},
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards(
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        Task* owner,
        Handler&& handler) {
        parallel_shards(
            thread_from_index(0),
            sharded_ops,
            mode,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        std::uint64_t batch_id,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl(
            std::bool_constant<true>{},
            shard_begin,
            sharded_ops,
            ParallelMode::AllShards,
            batch_id,
            OrderedBatchOptions{},
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        std::uint64_t batch_id,
        OrderedBatchOptions options,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl(
            std::bool_constant<true>{},
            shard_begin,
            sharded_ops,
            ParallelMode::AllShards,
            batch_id,
            options,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(
        ShardedOps<Op>& sharded_ops,
        std::uint64_t batch_id,
        Task* owner,
        Handler&& handler) {
        parallel_shards_ordered(
            thread_from_index(0),
            sharded_ops,
            batch_id,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(
        ShardedOps<Op>& sharded_ops,
        std::uint64_t batch_id,
        OrderedBatchOptions options,
        Task* owner,
        Handler&& handler) {
        parallel_shards_ordered(
            thread_from_index(0),
            sharded_ops,
            batch_id,
            options,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards(
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        std::uint64_t batch_id,
        Task* owner,
        Handler&& handler) {
        AF_ASSERT(mode == ParallelMode::AllShards);
        static_cast<void>(mode);
        parallel_shards_ordered(
            thread_from_index(0),
            sharded_ops,
            batch_id,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename StreamTag, typename ApplyTaskT, typename Batch>
    [[nodiscard]] static bool start_ordered_task(Thread sequencer_thread, Batch&& batch) {
        using BatchT = std::decay_t<Batch>;
        auto* task = allocate_task<OrderedStartTask<StreamTag, ApplyTaskT, BatchT>>();
        const bool ok = task->do_it(sequencer_thread, std::forward<Batch>(batch));
        if (!ok && task->is_created()) {
            task->release_lifetime_ref();
        }
        return ok;
    }

    [[nodiscard]] static std::uint64_t ordered_last_applied_batch_id(Thread thread) noexcept {
        const std::uint16_t index = thread_index(thread);
        if (index >= ordered_batch_state_.size()) {
            AF_ASSERT(false && "invalid ordered batch thread");
            return 0;
        }
        return ordered_batch_state_[index].last_applied_batch_id;
    }

    [[nodiscard]] static bool io_backend_available(Thread thread) noexcept {
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            return false;
        }
        return executors_[index]->io_backend_available();
    }

    [[nodiscard]] static bool io_uring_backend_available(Thread thread) noexcept {
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            return false;
        }
        return executors_[index]->io_uring_backend_available();
    }

    [[nodiscard]] static bool io_uring_poll_available(Thread thread) noexcept {
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            return false;
        }
        return executors_[index]->io_uring_poll_available();
    }

#if !defined(_WIN32)
    [[nodiscard]] static bool io_register_buffers(
        Thread thread,
        const iovec* buffers,
        unsigned buffer_count,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (buffers == nullptr || buffer_count == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->register_io_uring_buffers(buffers, buffer_count, error);
    }

    [[nodiscard]] static bool io_unregister_buffers(
        Thread thread,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->unregister_io_uring_buffers(error);
    }
#endif

    [[nodiscard]] static bool io_register_provided_buffer_ring(
        Thread thread,
        void* ring,
        unsigned ring_entries,
        std::uint16_t buffer_group,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (ring == nullptr || ring_entries == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->register_io_uring_provided_buffer_ring(
            ring,
            ring_entries,
            buffer_group,
            error);
    }

    [[nodiscard]] static bool io_unregister_provided_buffer_ring(
        Thread thread,
        std::uint16_t buffer_group,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->unregister_io_uring_provided_buffer_ring(
            buffer_group,
            error);
    }

    [[nodiscard]] static bool io_register_files(
        Thread thread,
        const int* files,
        unsigned file_count,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (files == nullptr || file_count == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->register_io_uring_files(files, file_count, error);
    }

    [[nodiscard]] static bool io_update_registered_files(
        Thread thread,
        unsigned offset,
        const int* files,
        unsigned file_count,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }
        if (files == nullptr || file_count == 0U) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->update_io_uring_files(offset, files, file_count, error);
    }

    [[nodiscard]] static bool io_unregister_files(
        Thread thread,
        int* error = nullptr) noexcept {
        if (error != nullptr) {
            *error = 0;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            if (error != nullptr) {
                *error = EINVAL;
            }
            return false;
        }
        return executors_[index]->unregister_io_uring_files(error);
    }

    [[nodiscard]] static bool io_wait(
        Thread thread,
        int fd,
        std::uint32_t events,
        Task* task,
        IoResult* result,
        bool prefer_rearm = false) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || events == 0U) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->register_io_wait(fd, events, task, result, prefer_rearm);
    }

    [[nodiscard]] static bool cancel_io(Thread thread, IoOpState& state) noexcept {
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            state.wait.fd = -1;
            state.wait.events = io_error;
            state.wait.error = EINVAL;
            state.wait.result = -EINVAL;
            return false;
        }
        return executors_[index]->cancel_io(state);
    }

    [[nodiscard]] static bool io_submit_timeout(
        Thread thread,
        std::chrono::nanoseconds timeout,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || timeout.count() <= 0) {
            if (result != nullptr) {
                result->fd = -1;
                result->events = io_error;
                result->error = EINVAL;
                result->result = -EINVAL;
                result->completion_token = nullptr;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = -1;
            result->events = io_error;
            result->error = EINVAL;
            result->result = -EINVAL;
            result->completion_token = nullptr;
            return false;
        }
        return executors_[index]->submit_io_uring_timeout(timeout, task, result);
    }

    [[nodiscard]] static bool io_submit_read_at(
        Thread thread,
        int fd,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_read(fd, data, size, offset, task, result);
    }

    [[nodiscard]] static bool io_submit_write_at(
        Thread thread,
        int fd,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_write(fd, data, size, offset, task, result);
    }

    [[nodiscard]] static bool io_submit_read_fixed_file_at(
        Thread thread,
        int file_index,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_read_fixed_file(
            file_index,
            data,
            size,
            offset,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_write_fixed_file_at(
        Thread thread,
        int file_index,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_write_fixed_file(
            file_index,
            data,
            size,
            offset,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_fsync_fixed_file(
        Thread thread,
        int file_index,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_fsync_fixed_file(
            file_index,
            flags,
            task,
            result);
    }

#if !defined(_WIN32)
    [[nodiscard]] static bool io_submit_readv_fixed_file_at(
        Thread thread,
        int file_index,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_readv_fixed_file(
            file_index,
            iov,
            iov_count,
            offset,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_writev_fixed_file_at(
        Thread thread,
        int file_index,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_writev_fixed_file(
            file_index,
            iov,
            iov_count,
            offset,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_read_fixed_file_at(
        Thread thread,
        int file_index,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_read_fixed_file(
            file_index,
            data,
            size,
            offset,
            buffer_index,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_write_fixed_file_at(
        Thread thread,
        int file_index,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_write_fixed_file(
            file_index,
            data,
            size,
            offset,
            buffer_index,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_read_fixed_at(
        Thread thread,
        int fd,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_read_fixed(
            fd,
            data,
            size,
            offset,
            buffer_index,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_write_fixed_at(
        Thread thread,
        int fd,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_write_fixed(
            fd,
            data,
            size,
            offset,
            buffer_index,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_readv_at(
        Thread thread,
        int fd,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_readv(fd, iov, iov_count, offset, task, result);
    }

    [[nodiscard]] static bool io_submit_writev_at(
        Thread thread,
        int fd,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_writev(fd, iov, iov_count, offset, task, result);
    }
#endif

    [[nodiscard]] static bool io_submit_fsync(
        Thread thread,
        int fd,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_fsync(fd, flags, task, result);
    }

    [[nodiscard]] static bool io_submit_openat(
        Thread thread,
        int dir_fd,
        const char* path,
        int flags,
        std::uint32_t mode,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr) {
            if (result != nullptr) {
                result->fd = dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_openat(
            dir_fd,
            path,
            flags,
            mode,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_socket(
        Thread thread,
        int domain,
        int type,
        int protocol,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr) {
            if (result != nullptr) {
                result->fd = -1;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = -1;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_socket(
            domain,
            type,
            protocol,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_openat_direct(
        Thread thread,
        int dir_fd,
        const char* path,
        int flags,
        std::uint32_t mode,
        int file_index,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr || file_index < 0) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
                result->result = -result->error;
                result->completion_token = nullptr;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            result->result = -EINVAL;
            result->completion_token = nullptr;
            return false;
        }
        return executors_[index]->submit_io_uring_openat_direct(
            dir_fd,
            path,
            flags,
            mode,
            file_index,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_openat2(
        Thread thread,
        int dir_fd,
        const char* path,
        const struct open_how* how,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr || how == nullptr) {
            if (result != nullptr) {
                result->fd = dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_openat2(
            dir_fd,
            path,
            how,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_close(
        Thread thread,
        int fd,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_close(fd, task, result);
    }

    [[nodiscard]] static bool io_submit_shutdown(
        Thread thread,
        int fd,
        int how,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_shutdown(fd, how, task, result);
    }

    [[nodiscard]] static bool io_submit_statx(
        Thread thread,
        int dir_fd,
        const char* path,
        int flags,
        std::uint32_t mask,
        struct statx* output,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr || output == nullptr) {
            if (result != nullptr) {
                result->fd = dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_statx(
            dir_fd,
            path,
            flags,
            mask,
            output,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_fallocate(
        Thread thread,
        int fd,
        int mode,
        std::uint64_t offset,
        std::uint64_t length,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_fallocate(
            fd,
            mode,
            offset,
            length,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_renameat(
        Thread thread,
        int old_dir_fd,
        const char* old_path,
        int new_dir_fd,
        const char* new_path,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || old_path == nullptr || new_path == nullptr) {
            if (result != nullptr) {
                result->fd = old_dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = old_dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_renameat(
            old_dir_fd,
            old_path,
            new_dir_fd,
            new_path,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_unlinkat(
        Thread thread,
        int dir_fd,
        const char* path,
        int flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr) {
            if (result != nullptr) {
                result->fd = dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_unlinkat(
            dir_fd,
            path,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_mkdirat(
        Thread thread,
        int dir_fd,
        const char* path,
        std::uint32_t mode,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || path == nullptr) {
            if (result != nullptr) {
                result->fd = dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_mkdirat(dir_fd, path, mode, task, result);
    }

    [[nodiscard]] static bool io_submit_symlinkat(
        Thread thread,
        const char* target,
        int new_dir_fd,
        const char* link_path,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || target == nullptr || link_path == nullptr) {
            if (result != nullptr) {
                result->fd = new_dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = new_dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_symlinkat(
            target,
            new_dir_fd,
            link_path,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_linkat(
        Thread thread,
        int old_dir_fd,
        const char* old_path,
        int new_dir_fd,
        const char* new_path,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || old_path == nullptr || new_path == nullptr) {
            if (result != nullptr) {
                result->fd = old_dir_fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = old_dir_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_linkat(
            old_dir_fd,
            old_path,
            new_dir_fd,
            new_path,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_ftruncate(
        Thread thread,
        int fd,
        std::uint64_t length,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_ftruncate(fd, length, task, result);
    }

    [[nodiscard]] static bool io_submit_splice(
        Thread thread,
        int in_fd,
        std::int64_t off_in,
        int out_fd,
        std::int64_t off_out,
        std::size_t count,
        unsigned int flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || in_fd < 0 || out_fd < 0) {
            if (result != nullptr) {
                result->fd = out_fd;
                result->events = io_error;
                result->error = in_fd < 0 || out_fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = out_fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_splice(
            in_fd,
            off_in,
            out_fd,
            off_out,
            count,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_recv(
        Thread thread,
        int fd,
        void* data,
        std::size_t size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recv(fd, data, size, flags, task, result);
    }

    [[nodiscard]] static bool io_submit_recv_fixed_file(
        Thread thread,
        int file_index,
        void* data,
        std::size_t size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recv_fixed_file(
            file_index,
            data,
            size,
            flags,
            task,
            result);
    }

#if defined(__linux__)
    [[nodiscard]] static bool io_submit_recv_multishot(
        Thread thread,
        int fd,
        std::uint16_t buffer_group,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recv_multishot(
            fd,
            buffer_group,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_recvmsg_multishot(
        Thread thread,
        int fd,
        std::uint16_t buffer_group,
        socklen_t name_capacity,
        std::size_t control_capacity,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recvmsg_multishot(
            fd,
            buffer_group,
            name_capacity,
            control_capacity,
            flags,
            task,
            result);
    }
#endif

    [[nodiscard]] static bool io_submit_send(
        Thread thread,
        int fd,
        const void* data,
        std::size_t size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_send(fd, data, size, flags, task, result);
    }

    [[nodiscard]] static bool io_submit_send_fixed_file(
        Thread thread,
        int file_index,
        const void* data,
        std::size_t size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_send_fixed_file(
            file_index,
            data,
            size,
            flags,
            task,
            result);
    }

#if defined(__linux__)
    [[nodiscard]] static bool io_submit_send_zc(
        Thread thread,
        int fd,
        const void* data,
        std::size_t size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_send_zc(fd, data, size, flags, task, result);
    }

    [[nodiscard]] static bool io_submit_sendmsg_zc(
        Thread thread,
        int fd,
        const void* data,
        std::size_t size,
        const sockaddr* address,
        socklen_t address_size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_sendmsg_zc(
            fd,
            data,
            size,
            address,
            address_size,
            flags,
            task,
            result);
    }

    [[nodiscard]] static bool io_submit_sendmsg_zc_iov(
        Thread thread,
        int fd,
        const iovec* iov,
        int iov_count,
        const sockaddr* address,
        socklen_t address_size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_sendmsg_zc_iov(
            fd,
            iov,
            iov_count,
            address,
            address_size,
            flags,
            task,
            result);
    }
#endif

#if !defined(_WIN32)
    [[nodiscard]] static bool io_submit_recvmsg_fixed_file_iov(
        Thread thread,
        int file_index,
        const iovec* iov,
        int iov_count,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recvmsg_fixed_file_iov(
            file_index,
            iov,
            iov_count,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(flags);
        result->fd = file_index;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_recvmsg_iov(
        Thread thread,
        int fd,
        const iovec* iov,
        int iov_count,
        sockaddr* address,
        socklen_t* address_size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recvmsg_iov(
            fd,
            iov,
            iov_count,
            address,
            address_size,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        result->fd = fd;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_sendmsg_fixed_file_iov(
        Thread thread,
        int file_index,
        const iovec* iov,
        int iov_count,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || file_index < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = file_index < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_sendmsg_fixed_file_iov(
            file_index,
            iov,
            iov_count,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(flags);
        result->fd = file_index;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_sendmsg_iov(
        Thread thread,
        int fd,
        const iovec* iov,
        int iov_count,
        const sockaddr* address,
        socklen_t address_size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || iov == nullptr || iov_count <= 0) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_sendmsg_iov(
            fd,
            iov,
            iov_count,
            address,
            address_size,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        result->fd = fd;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_recvmsg(
        Thread thread,
        int fd,
        void* data,
        std::size_t size,
        sockaddr* address,
        socklen_t* address_size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_recvmsg(
            fd,
            data,
            size,
            address,
            address_size,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(size);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        result->fd = fd;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_sendmsg(
        Thread thread,
        int fd,
        const void* data,
        std::size_t size,
        const sockaddr* address,
        socklen_t address_size,
        std::uint32_t flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || data == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_sendmsg(
            fd,
            data,
            size,
            address,
            address_size,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(size);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        result->fd = fd;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_accept(
        Thread thread,
        int fd,
        sockaddr* address,
        socklen_t* address_size,
        int flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 ||
            ((address == nullptr) != (address_size == nullptr))) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_accept(
            fd,
            address,
            address_size,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        result->fd = fd;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_accept_direct(
        Thread thread,
        int fd,
        sockaddr* address,
        socklen_t* address_size,
        int flags,
        int file_index,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || file_index < 0 ||
            ((address == nullptr) != (address_size == nullptr))) {
            if (result != nullptr) {
                result->fd = file_index;
                result->events = io_error;
                result->error = fd < 0 || file_index < 0 ? EBADF : EINVAL;
                result->result = -result->error;
                result->completion_token = nullptr;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = file_index;
            result->events = io_error;
            result->error = EINVAL;
            result->result = -EINVAL;
            result->completion_token = nullptr;
            return false;
        }
        return executors_[index]->submit_io_uring_accept_direct(
            fd,
            address,
            address_size,
            flags,
            file_index,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        result->fd = file_index;
        result->events = io_error;
        result->error = ENOSYS;
        result->result = -ENOSYS;
        result->completion_token = nullptr;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_accept_multishot(
        Thread thread,
        int fd,
        sockaddr* address,
        socklen_t* address_size,
        int flags,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 ||
            address != nullptr || address_size != nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_accept_multishot(
            fd,
            address,
            address_size,
            flags,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        result->fd = fd;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }

    [[nodiscard]] static bool io_submit_connect(
        Thread thread,
        int fd,
        const sockaddr* address,
        socklen_t address_size,
        Task* task,
        IoResult* result) noexcept {
        if (task == nullptr || result == nullptr || fd < 0 || address == nullptr || address_size == 0U) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = fd < 0 ? EBADF : EINVAL;
            }
            return false;
        }

#if defined(__linux__)
        const std::uint16_t index = thread_index(thread);
        if (index >= executors_.size()) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return false;
        }
        return executors_[index]->submit_io_uring_connect(
            fd,
            address,
            address_size,
            task,
            result);
#else
        static_cast<void>(thread);
        static_cast<void>(address);
        static_cast<void>(address_size);
        result->fd = fd;
        result->events = io_error;
        result->error = ENOSYS;
        return false;
#endif
    }
#endif

private:
    // These fragments are included inside AsyncRuntime to keep templates visible and inlineable.
#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_common_fragment.hpp"
#include "af/detail/runtime_parallel_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

    class alignas(detail::hardware_cache_line_size) Executor {
#if defined(__linux__)
        struct IoWaitRegistration;
        struct IoUringOperation;
#endif

    public:
#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_control_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

        [[nodiscard]] bool io_backend_available() const noexcept {
#if defined(__linux__)
            return io_epoll_fd_ >= 0;
#else
            return false;
#endif
        }

        [[nodiscard]] bool io_uring_backend_available() const noexcept {
#if defined(__linux__)
            return io_uring_fd_ >= 0;
#else
            return false;
#endif
        }

        [[nodiscard]] bool io_uring_poll_available() const noexcept {
#if defined(__linux__)
            return io_uring_fd_ >= 0 && io_uring_poll_add_available_;
#else
            return false;
#endif
        }

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_resource_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_wait_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_file_data_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_fd_lifecycle_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_socket_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_task_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

    private:
#if defined(__linux__)
        struct IoWaitRegistration {
            int fd{-1};
            Task* task{nullptr};
            IoResult* result{nullptr};
            IoUringOperation* poll_operation{nullptr};
        };

        struct IoUringOperation {
            Task* task{nullptr};
            IoResult* result{nullptr};
            IoUringOperation* prev{nullptr};
            IoUringOperation* next{nullptr};
            detail::IoUringMessage* msg{nullptr};
            union {
                detail::IoUringSocketAddress* socket_address;
                __kernel_timespec timeout;
            };
            IoWaitRegistration* wait_registration{nullptr};
            std::uint32_t complete_events{0};
            int direct_file_index{-1};
            std::uint8_t opcode{0};
            bool cancel_requested{false};
            bool multishot{false};
            bool poll_wait{false};
            bool zero_copy_send{false};
            bool zero_copy_primary_done{false};
            bool zero_copy_notification_done{false};
        };

        enum class IoUringPollSubmitResult : std::uint8_t {
            Submitted,
            Fallback,
            Failed,
            BackendClosed,
        };
#endif

        [[nodiscard]] bool io_thread() const noexcept {
            return kind_ == ThreadKind::Epoll || kind_ == ThreadKind::IoUring;
        }

        [[nodiscard]] bool io_uring_thread() const noexcept {
            return kind_ == ThreadKind::IoUring;
        }

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_submit_core_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_backend_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#if defined(__linux__)
        void init_io_uring_backend() noexcept {
            if (io_uring_fd_ >= 0 || io_wake_fd_ < 0) {
                return;
            }

            io_uring_params params{};
            unsigned requested_setup_flags = io_uring_setup_flags;
            if constexpr (io_uring_setup_sqpoll || io_uring_sqpoll_cpu >= 0) {
                requested_setup_flags |= IORING_SETUP_SQPOLL;
            }
            if constexpr (io_uring_setup_submit_all) {
                requested_setup_flags |= IORING_SETUP_SUBMIT_ALL;
            }
            if constexpr (io_uring_setup_coop_taskrun) {
                requested_setup_flags |= IORING_SETUP_COOP_TASKRUN;
            }
            if constexpr (io_uring_setup_single_issuer || io_uring_setup_defer_taskrun) {
                requested_setup_flags |= IORING_SETUP_SINGLE_ISSUER;
            }
            if constexpr (io_uring_setup_defer_taskrun) {
                requested_setup_flags |= IORING_SETUP_DEFER_TASKRUN;
            }
            detail::configure_io_uring_params(
                params,
                detail::IoUringSetupRequest{
                    requested_setup_flags,
                    io_uring_cq_entries,
                    io_uring_sqpoll_idle_ms,
                    io_uring_sqpoll_cpu});
            io_uring_fd_ = detail::sys_io_uring_setup(io_uring_entries, &params);
            if (io_uring_fd_ < 0) {
                return;
            }

            const std::size_t sq_ring_size =
                params.sq_off.array + static_cast<std::size_t>(params.sq_entries) * sizeof(std::uint32_t);
            const std::size_t cq_ring_size =
                params.cq_off.cqes + static_cast<std::size_t>(params.cq_entries) * sizeof(io_uring_cqe);

            if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0U) {
                io_uring_sq_ring_size_ = std::max(sq_ring_size, cq_ring_size);
                io_uring_cq_ring_size_ = io_uring_sq_ring_size_;
                io_uring_sq_ring_ = static_cast<std::byte*>(::mmap(
                    nullptr,
                    io_uring_sq_ring_size_,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE,
                    io_uring_fd_,
                    IORING_OFF_SQ_RING));
                io_uring_cq_ring_ = io_uring_sq_ring_;
            } else {
                io_uring_sq_ring_size_ = sq_ring_size;
                io_uring_cq_ring_size_ = cq_ring_size;
                io_uring_sq_ring_ = static_cast<std::byte*>(::mmap(
                    nullptr,
                    io_uring_sq_ring_size_,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE,
                    io_uring_fd_,
                    IORING_OFF_SQ_RING));
                io_uring_cq_ring_ = static_cast<std::byte*>(::mmap(
                    nullptr,
                    io_uring_cq_ring_size_,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE,
                    io_uring_fd_,
                    IORING_OFF_CQ_RING));
            }

            io_uring_sqes_size_ = static_cast<std::size_t>(params.sq_entries) * sizeof(io_uring_sqe);
            io_uring_sqes_ = static_cast<io_uring_sqe*>(::mmap(
                nullptr,
                io_uring_sqes_size_,
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_POPULATE,
                io_uring_fd_,
                IORING_OFF_SQES));

            if (io_uring_sq_ring_ == MAP_FAILED ||
                io_uring_cq_ring_ == MAP_FAILED ||
                io_uring_sqes_ == MAP_FAILED) {
                close_io_uring_backend();
                return;
            }

            io_uring_sq_head_ = ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.head);
            io_uring_sq_tail_ = ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.tail);
            io_uring_sq_ring_mask_ = ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.ring_mask);
            io_uring_sq_ring_entries_ = ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.ring_entries);
            io_uring_sq_array_ = ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.array);
            io_uring_cq_head_ = ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.head);
            io_uring_cq_tail_ = ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.tail);
            io_uring_cq_ring_mask_ = ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.ring_mask);
            io_uring_cqes_ = ptr_at<io_uring_cqe>(io_uring_cq_ring_, params.cq_off.cqes);
            detect_io_uring_features();

            if (detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_REGISTER_EVENTFD,
                    &io_wake_fd_,
                    1) != 0) {
                close_io_uring_backend();
            }
        }

        template <typename T>
        [[nodiscard]] static T* ptr_at(std::byte* base, std::uint32_t offset) noexcept {
            return reinterpret_cast<T*>(base + offset);
        }

        void detect_io_uring_features() noexcept {
            io_uring_send_zc_available_ = false;
            io_uring_sendmsg_zc_available_ = false;
            io_uring_poll_add_available_ = false;
            io_uring_socket_available_ = false;

            constexpr unsigned probe_count = 64;
            std::array<
                std::byte,
                sizeof(io_uring_probe) + probe_count * sizeof(io_uring_probe_op)>
                storage{};
            auto* probe = reinterpret_cast<io_uring_probe*>(storage.data());
            if (detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_REGISTER_PROBE,
                    probe,
                    probe_count) != 0) {
                return;
            }

            const auto* ops = reinterpret_cast<const io_uring_probe_op*>(
                storage.data() + sizeof(io_uring_probe));
            const unsigned op_count = std::min<unsigned>(probe->ops_len, probe_count);
            for (unsigned i = 0; i < op_count; ++i) {
                if (ops[i].op == detail::io_uring_op_send_zc &&
                    (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_send_zc_available_ = true;
                } else if (ops[i].op == detail::io_uring_op_sendmsg_zc &&
                           (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_sendmsg_zc_available_ = true;
                } else if (ops[i].op == IORING_OP_POLL_ADD &&
                           (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_poll_add_available_ = true;
                } else if (ops[i].op == detail::io_uring_op_socket &&
                           (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
                    io_uring_socket_available_ = true;
                }
            }
        }

        void close_io_uring_backend() noexcept {
            clear_io_uring_operations();
            if (io_uring_fd_ >= 0 && io_uring_files_registered_) {
                static_cast<void>(detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_UNREGISTER_FILES,
                    nullptr,
                    0));
            }
            if (io_uring_fd_ >= 0 && io_uring_buffers_registered_) {
                static_cast<void>(detail::sys_io_uring_register(
                    io_uring_fd_,
                    IORING_UNREGISTER_BUFFERS,
                    nullptr,
                    0));
            }
            if (io_uring_sqes_ != nullptr && io_uring_sqes_ != MAP_FAILED) {
                ::munmap(io_uring_sqes_, io_uring_sqes_size_);
            }
            if (io_uring_sq_ring_ != nullptr && io_uring_sq_ring_ != MAP_FAILED) {
                ::munmap(io_uring_sq_ring_, io_uring_sq_ring_size_);
            }
            if (io_uring_cq_ring_ != nullptr &&
                io_uring_cq_ring_ != MAP_FAILED &&
                io_uring_cq_ring_ != io_uring_sq_ring_) {
                ::munmap(io_uring_cq_ring_, io_uring_cq_ring_size_);
            }
            if (io_uring_fd_ >= 0) {
                ::close(io_uring_fd_);
            }

            io_uring_fd_ = -1;
            io_uring_sq_ring_ = nullptr;
            io_uring_cq_ring_ = nullptr;
            io_uring_sqes_ = nullptr;
            io_uring_sq_ring_size_ = 0;
            io_uring_cq_ring_size_ = 0;
            io_uring_sqes_size_ = 0;
            io_uring_sq_head_ = nullptr;
            io_uring_sq_tail_ = nullptr;
            io_uring_sq_ring_mask_ = nullptr;
            io_uring_sq_ring_entries_ = nullptr;
            io_uring_sq_array_ = nullptr;
            io_uring_cq_head_ = nullptr;
            io_uring_cq_tail_ = nullptr;
            io_uring_cq_ring_mask_ = nullptr;
            io_uring_cqes_ = nullptr;
            io_uring_pending_submissions_ = 0;
            io_uring_send_zc_available_ = false;
            io_uring_sendmsg_zc_available_ = false;
            io_uring_poll_add_available_ = false;
            io_uring_socket_available_ = false;
            io_uring_buffers_registered_ = false;
            io_uring_registered_buffer_count_ = 0;
            io_uring_provided_buffer_groups_.clear();
            io_uring_files_registered_ = false;
            io_uring_registered_file_count_ = 0;
        }

        [[nodiscard]] io_uring_sqe* reserve_io_uring_sqe(int& error) noexcept {
            error = 0;
            if (io_uring_fd_ < 0 || io_uring_sq_tail_ == nullptr || io_uring_sq_head_ == nullptr) {
                error = ENOSYS;
                return nullptr;
            }

            std::uint32_t head = __atomic_load_n(io_uring_sq_head_, __ATOMIC_ACQUIRE);
            std::uint32_t tail = __atomic_load_n(io_uring_sq_tail_, __ATOMIC_RELAXED);
            if (tail - head >= *io_uring_sq_ring_entries_ && io_uring_pending_submissions_ != 0U) {
                const int submit_error = flush_io_uring_submissions();
                if (submit_error != 0) {
                    error = submit_error;
                    fail_io_uring_backend(submit_error, nullptr);
                    return nullptr;
                }
                head = __atomic_load_n(io_uring_sq_head_, __ATOMIC_ACQUIRE);
                tail = __atomic_load_n(io_uring_sq_tail_, __ATOMIC_RELAXED);
            }
            if (tail - head >= *io_uring_sq_ring_entries_) {
                error = EBUSY;
                return nullptr;
            }

            const std::uint32_t index = tail & *io_uring_sq_ring_mask_;
            io_uring_sq_array_[index] = index;
            __atomic_store_n(io_uring_sq_tail_, tail + 1U, __ATOMIC_RELEASE);
            ++io_uring_pending_submissions_;
            return &io_uring_sqes_[index];
        }

        void reserve_io_backend_storage() noexcept {
            try {
                if constexpr (io_wait_reserve != 0U) {
                    io_waits_.reserve(io_wait_reserve);
                }
                if constexpr (io_deferred_delete_reserve != 0U) {
                    io_deferred_deletes_.reserve(io_deferred_delete_reserve);
                }
                if constexpr (io_uring_provided_buffer_group_reserve != 0U) {
                    io_uring_provided_buffer_groups_.reserve(
                        io_uring_provided_buffer_group_reserve);
                }
            } catch (...) {
            }
        }

        [[nodiscard]] int flush_io_uring_submissions() noexcept {
            if (io_uring_pending_submissions_ == 0U) {
                return 0;
            }

            unsigned remaining = io_uring_pending_submissions_;
            while (remaining != 0U) {
                const int submitted = detail::sys_io_uring_enter(io_uring_fd_, remaining, 0, 0);
                if (submitted > 0) {
                    const auto submitted_count = static_cast<unsigned>(submitted);
                    if (submitted_count > remaining) {
                        return EIO;
                    }
                    remaining -= submitted_count;
                    continue;
                }
                if (submitted == 0) {
                    return EIO;
                }
                if (errno == EINTR) {
                    continue;
                }
                return errno == 0 ? EIO : errno;
            }

            io_uring_pending_submissions_ = 0;
            return 0;
        }

        [[nodiscard]] bool flush_io_uring_submissions_or_fail() noexcept {
            const int submit_error = flush_io_uring_submissions();
            if (submit_error == 0) {
                return false;
            }
            fail_io_uring_backend(submit_error, nullptr);
            return true;
        }

        [[nodiscard]] bool poll_io_uring_completions() noexcept {
            if (io_uring_fd_ < 0 || io_uring_cq_head_ == nullptr || io_uring_cq_tail_ == nullptr) {
                return false;
            }

            bool did_work = false;
            std::uint32_t head = __atomic_load_n(io_uring_cq_head_, __ATOMIC_ACQUIRE);
            const std::uint32_t tail = __atomic_load_n(io_uring_cq_tail_, __ATOMIC_ACQUIRE);
            while (head != tail) {
                io_uring_cqe& cqe = io_uring_cqes_[head & *io_uring_cq_ring_mask_];
                auto* operation = reinterpret_cast<IoUringOperation*>(cqe.user_data);
                if (operation != nullptr) {
                    const bool yield_to_task = complete_io_uring_operation(
                        operation,
                        cqe.res,
                        cqe.flags);
                    did_work = true;
                    ++head;
                    if (yield_to_task) {
                        break;
                    }
                    continue;
                }
                ++head;
            }
            __atomic_store_n(io_uring_cq_head_, head, __ATOMIC_RELEASE);
            return did_work;
        }

        [[nodiscard]] bool complete_io_uring_operation(
            IoUringOperation* operation,
            int result,
            std::uint32_t cqe_flags) noexcept {
            if (operation->poll_wait) {
                complete_io_uring_poll_wait(operation, result);
                return false;
            }

            if (operation->zero_copy_send && (cqe_flags & IORING_CQE_F_NOTIF) != 0U) {
                operation->zero_copy_notification_done = true;
                if (operation->zero_copy_primary_done) {
                    untrack_io_uring_operation(operation);
                    destroy_io_uring_operation(operation);
                }
                return false;
            }

            const bool more =
                operation->multishot &&
                !operation->cancel_requested &&
                result >= 0 &&
                (cqe_flags & IORING_CQE_F_MORE) != 0U;
            const bool zero_copy_waits_for_notification =
                operation->zero_copy_send && (cqe_flags & IORING_CQE_F_MORE) != 0U;
            if (!more && !zero_copy_waits_for_notification) {
                untrack_io_uring_operation(operation);
            }
            operation->result->result = result;
            if (operation->cancel_requested) {
                if (result >= 0) {
                    if (io_uring_operation_result_is_fd(operation)) {
                        ::close(result);
                    } else {
                        clear_direct_io_uring_file_slot(operation);
                    }
                }
                operation->result->events = io_error;
                operation->result->error = ECANCELED;
                operation->result->result = -ECANCELED;
            } else if (result < 0) {
                operation->result->events = io_error;
                operation->result->error = -result;
            } else {
                std::uint32_t events = operation->complete_events | (more ? io_more : 0U);
                if ((cqe_flags & IORING_CQE_F_BUFFER) != 0U) {
                    events |= io_buffer_selected |
                              ((cqe_flags >> io_buffer_id_shift) << io_buffer_id_shift);
                }
                operation->result->events = events;
                operation->result->error = 0;
                if (operation->msg != nullptr && operation->msg->address_size != nullptr) {
                    *operation->msg->address_size = operation->msg->header.msg_namelen;
                }
            if (operation->opcode != IORING_OP_TIMEOUT &&
                operation->socket_address != nullptr &&
                operation->socket_address->output_size != nullptr) {
                const socklen_t actual_size = operation->socket_address->size;
                    if (operation->socket_address->output != nullptr &&
                        operation->socket_address->output_capacity != 0U) {
                        const auto copy_size = static_cast<std::size_t>(
                            std::min(actual_size, operation->socket_address->output_capacity));
                        std::memcpy(
                            operation->socket_address->output,
                            &operation->socket_address->storage,
                            copy_size);
                    }
                    *operation->socket_address->output_size = actual_size;
                }
            }
            enqueue_pending_blocking(index_, operation->task);
            if (more) {
                return true;
            }
            if (zero_copy_waits_for_notification) {
                operation->zero_copy_primary_done = true;
                clear_io_uring_result_token(operation);
                operation->task = nullptr;
                operation->result = nullptr;
                if (operation->zero_copy_notification_done) {
                    untrack_io_uring_operation(operation);
                    destroy_io_uring_operation(operation);
                }
                return true;
            }
            destroy_io_uring_operation(operation);
            return false;
        }

        void complete_io_uring_poll_wait(
            IoUringOperation* operation,
            int result) noexcept {
            IoWaitRegistration* registration = operation->wait_registration;
            if (registration == nullptr || operation->task == nullptr || operation->result == nullptr) {
                untrack_io_uring_operation(operation);
                destroy_io_uring_operation(operation);
                return;
            }

            const int fd = registration->fd;
            auto it = io_waits_.find(fd);
            if (it != io_waits_.end() && it->second == registration) {
                io_waits_.erase(it);
            }

            registration->result->fd = fd;
            registration->result->result = result;
            if (operation->cancel_requested) {
                registration->result->events = io_error;
                registration->result->error = ECANCELED;
                registration->result->result = -ECANCELED;
            } else if (result < 0) {
                registration->result->events = io_error;
                registration->result->error = -result;
            } else {
                registration->result->events = io_events_from_poll(static_cast<std::uint32_t>(result));
                registration->result->error = 0;
            }

            enqueue_pending_blocking(index_, registration->task);
            registration->poll_operation = nullptr;
            operation->wait_registration = nullptr;
            untrack_io_uring_operation(operation);
            destroy_io_uring_operation(operation);
            io_wait_pool_.destroy(registration);
        }

        [[nodiscard]] static bool io_uring_result_is_fd(std::uint8_t opcode) noexcept {
            return opcode == IORING_OP_OPENAT ||
                   opcode == IORING_OP_ACCEPT ||
                   opcode == detail::io_uring_op_openat2 ||
                   opcode == detail::io_uring_op_socket;
        }

        [[nodiscard]] static bool io_uring_operation_result_is_fd(
            const IoUringOperation* operation) noexcept {
            return operation != nullptr &&
                   operation->direct_file_index < 0 &&
                   io_uring_result_is_fd(operation->opcode);
        }

        void clear_direct_io_uring_file_slot(const IoUringOperation* operation) noexcept {
            if (operation == nullptr ||
                operation->direct_file_index < 0 ||
                !io_uring_result_is_fd(operation->opcode) ||
                io_uring_fd_ < 0 ||
                !io_uring_files_registered_) {
                return;
            }
            const int invalid_fd = -1;
            io_uring_files_update update{};
            update.offset = static_cast<unsigned>(operation->direct_file_index);
            update.fds = reinterpret_cast<std::uint64_t>(&invalid_fd);
            static_cast<void>(detail::sys_io_uring_register(
                io_uring_fd_,
                IORING_REGISTER_FILES_UPDATE,
                &update,
                1));
        }

        [[nodiscard]] int submit_io_uring_cancel(IoUringOperation* operation) noexcept {
            int reserve_error = 0;
            io_uring_sqe* sqe = reserve_io_uring_sqe(reserve_error);
            if (sqe == nullptr) {
                return reserve_error == 0 ? EBUSY : reserve_error;
            }

            *sqe = io_uring_sqe{};
            sqe->opcode = IORING_OP_ASYNC_CANCEL;
            sqe->fd = -1;
            sqe->addr = reinterpret_cast<std::uint64_t>(operation);
            sqe->cancel_flags = 0;
            sqe->user_data = 0;

            const int submit_error = flush_io_uring_submissions();
            if (submit_error != 0) {
                fail_io_uring_backend(submit_error, nullptr);
            }
            return submit_error;
        }

        void track_io_uring_operation(IoUringOperation* operation) noexcept {
            operation->prev = nullptr;
            operation->next = io_uring_operations_;
            if (io_uring_operations_ != nullptr) {
                io_uring_operations_->prev = operation;
            }
            io_uring_operations_ = operation;
        }

        void untrack_io_uring_operation(IoUringOperation* operation) noexcept {
            if (operation->prev != nullptr) {
                operation->prev->next = operation->next;
            } else if (io_uring_operations_ == operation) {
                io_uring_operations_ = operation->next;
            }
            if (operation->next != nullptr) {
                operation->next->prev = operation->prev;
            }
            operation->prev = nullptr;
            operation->next = nullptr;
        }

        void clear_io_uring_operations() noexcept {
            IoUringOperation* operation = io_uring_operations_;
            io_uring_operations_ = nullptr;
            while (operation != nullptr) {
                IoUringOperation* next = operation->next;
                operation->prev = nullptr;
                operation->next = nullptr;
                close_pending_io_uring_fd_result(operation);
                destroy_io_uring_operation(operation);
                operation = next;
            }
        }

        void fail_io_uring_backend(int error, IoUringOperation* running_operation) noexcept {
            clear_or_fail_io_uring_operations(error, running_operation);
            close_io_uring_backend();
        }

        void clear_or_fail_io_uring_operations(
            int error,
            IoUringOperation* running_operation) noexcept {
            IoUringOperation* operation = io_uring_operations_;
            io_uring_operations_ = nullptr;
            while (operation != nullptr) {
                IoUringOperation* next = operation->next;
                operation->prev = nullptr;
                operation->next = nullptr;
                if (operation == running_operation) {
                    close_pending_io_uring_fd_result(operation);
                    destroy_io_uring_operation(operation);
                    operation = next;
                    continue;
                }

                close_pending_io_uring_fd_result(operation);
                if (operation->task == nullptr || operation->result == nullptr) {
                    destroy_io_uring_operation(operation);
                    operation = next;
                    continue;
                }
                operation->result->events = io_error;
                operation->result->error = operation->cancel_requested ? ECANCELED : error;
                operation->result->result = operation->cancel_requested ? -ECANCELED : -error;
                enqueue_pending_blocking(index_, operation->task);
                destroy_io_uring_operation(operation);
                operation = next;
            }
        }

        void close_pending_io_uring_fd_result(IoUringOperation* operation) noexcept {
            if (operation == nullptr ||
                operation->result == nullptr ||
                !io_uring_operation_result_is_fd(operation) ||
                operation->result->error != 0 ||
                (operation->result->events & operation->complete_events) == 0U ||
                operation->result->result < 0) {
                return;
            }
            ::close(static_cast<int>(operation->result->result));
            operation->result->events = io_error;
            operation->result->error = ECANCELED;
            operation->result->result = -ECANCELED;
        }

        static void clear_io_uring_result_token(IoUringOperation* operation) noexcept {
            if (operation != nullptr &&
                operation->result != nullptr &&
                operation->result->completion_token == operation) {
                operation->result->completion_token = nullptr;
            }
        }

        void destroy_io_uring_operation(IoUringOperation* operation) noexcept {
            clear_io_uring_result_token(operation);
            if (operation->msg != nullptr) {
                io_uring_msg_pool_.destroy(operation->msg);
                operation->msg = nullptr;
            }
            if (operation->opcode != IORING_OP_TIMEOUT && operation->socket_address != nullptr) {
                io_uring_address_pool_.destroy(operation->socket_address);
                operation->socket_address = nullptr;
            }
            io_uring_op_pool_.destroy(operation);
        }
#endif

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_poll_helpers_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_core_state_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE
    };

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_dispatch_fragment.hpp"
#include "af/detail/runtime_lifecycle_fragment.hpp"
#include "af/detail/runtime_state_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE
};

} // namespace af
