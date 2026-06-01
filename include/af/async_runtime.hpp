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

#include "absl/container/flat_hash_map.h"
#include "af/detail/queue/bounded_queues.hpp"
#include "af/detail/config.hpp"
#include "af/detail/io/uring/io_uring_support.hpp"
#include "af/detail/memory/object_pool.hpp"
#include "af/detail/runtime/runtime_common_state.hpp"
#include "af/detail/runtime/runtime_config.hpp"
#include "af/detail/runtime/runtime_public_io.hpp"
#include "af/detail/runtime/runtime_ready_source_set.hpp"
#include "af/detail/runtime/runtime_task_handle.hpp"
#include "af/task.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#endif

#if AF_DETAIL_HAS_EPOLL
#include <algorithm>
#include <linux/openat2.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <unistd.h>
#endif

#if AF_DETAIL_HAS_KQUEUE
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>
#endif

#if !defined(__linux__)
struct open_how;
struct statx;
#endif

namespace af {

namespace detail {
template <typename RuntimeT, typename TraitsT> class Executor;
} // namespace detail

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

template <typename Op> struct ShardedOps {
    std::vector<std::vector<Op>> shards;

    explicit ShardedOps(std::uint16_t shard_count = 0) : shards(shard_count) {}

    [[nodiscard]] std::uint16_t shard_count() const noexcept {
        return static_cast<std::uint16_t>(shards.size());
    }
};

template <typename TraitsT>
class AsyncRuntime : public detail::RuntimePublicIo<AsyncRuntime<TraitsT>, TraitsT> {
public:
    using Traits = TraitsT;
    using Thread = typename Traits::Thread;
    using Task = BasicTask<AsyncRuntime<Traits>>;
    using Config = detail::RuntimeConfig<Traits>;

    template <typename TaskT> using TaskHandle = detail::RuntimeTaskHandle<AsyncRuntime, TaskT>;

    template <typename RuntimeT, typename TaskT> friend class detail::RuntimeTaskHandle;

    template <typename RuntimeT> friend struct detail::RuntimeParallelGroup;

    template <typename RuntimeT, typename TraitsForRuntime> friend struct detail::RuntimePublicIo;

    template <typename RuntimeT, typename TraitsForRuntime> friend class detail::Executor;

    static constexpr std::uint16_t thread_count = Config::thread_count;
    static constexpr std::uint16_t invalid_thread_index = Config::invalid_thread_index;
    static constexpr std::size_t spsc_queue_capacity = Config::spsc_queue_capacity;
    static constexpr std::size_t external_queue_capacity = Config::external_queue_capacity;
    static constexpr QueueFullPolicy queue_full_policy = Config::queue_full_policy;
    static constexpr ShutdownPolicy shutdown_policy = Config::shutdown_policy;
    static constexpr bool task_registry_enabled = Config::task_registry_enabled;
    static constexpr unsigned io_uring_entries = Config::io_uring_entries;
    static constexpr unsigned io_uring_submit_batch_threshold =
        Config::io_uring_submit_batch_threshold;
    static constexpr unsigned io_uring_cq_entries = Config::io_uring_cq_entries;
    static constexpr unsigned io_uring_setup_flags = Config::io_uring_setup_flags;
    static constexpr bool io_uring_setup_sqpoll = Config::io_uring_setup_sqpoll;
    static constexpr unsigned io_uring_sqpoll_idle_ms = Config::io_uring_sqpoll_idle_ms;
    static constexpr int io_uring_sqpoll_cpu = Config::io_uring_sqpoll_cpu;
    static constexpr bool io_uring_setup_submit_all = Config::io_uring_setup_submit_all;
    static constexpr bool io_uring_setup_coop_taskrun = Config::io_uring_setup_coop_taskrun;
    static constexpr bool io_uring_setup_single_issuer = Config::io_uring_setup_single_issuer;
    static constexpr bool io_uring_setup_defer_taskrun = Config::io_uring_setup_defer_taskrun;
    static constexpr std::size_t io_wait_reserve = Config::io_wait_reserve;
    static constexpr std::size_t io_uring_provided_buffer_group_reserve =
        Config::io_uring_provided_buffer_group_reserve;

    [[nodiscard]] static constexpr ThreadKind thread_kind(Thread thread) noexcept {
        return Config::thread_kind(thread);
    }

    AsyncRuntime() = delete;

    static void init();
    static void shutdown();
    static void wait_for_idle() noexcept;
    [[nodiscard]] static std::uint32_t unfinished_task_count() noexcept;

    template <typename TaskT, typename... CtorArgs>
    [[nodiscard]] static TaskHandle<TaskT> make_task(CtorArgs &&...ctor_args);

    template <typename TaskT, typename... CtorArgs>
    [[nodiscard]] static TaskHandle<TaskT> create_task(CtorArgs &&...ctor_args);

    template <typename TaskT, typename... Args>
    [[nodiscard]] static bool start_task(Args &&...args);

    static bool post(Thread thread, Task *task) noexcept;

    [[nodiscard]] static Thread current_thread() noexcept;
    [[nodiscard]] static bool is_runtime_thread() noexcept;
    [[nodiscard]] static bool is_stopping() noexcept;
    [[nodiscard]] static std::uint16_t current_thread_index() noexcept;
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
    [[nodiscard]] static ShardedOps<Op> split_by_shard(std::vector<Op> &&ops,
                                                       std::uint16_t shard_count, KeyFn &&key_fn);

    template <typename Op, typename Handler>
    static void parallel_shards(Thread shard_begin, ShardedOps<Op> &sharded_ops, ParallelMode mode,
                                Task *owner, Handler &&handler);

    template <typename Op, typename Handler>
    static void parallel_shards(ShardedOps<Op> &sharded_ops, ParallelMode mode, Task *owner,
                                Handler &&handler);

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(Thread shard_begin, ShardedOps<Op> &sharded_ops,
                                        std::uint64_t batch_id, Task *owner, Handler &&handler);

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(Thread shard_begin, ShardedOps<Op> &sharded_ops,
                                        std::uint64_t batch_id, OrderedBatchOptions options,
                                        Task *owner, Handler &&handler);

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(ShardedOps<Op> &sharded_ops, std::uint64_t batch_id,
                                        Task *owner, Handler &&handler);

    template <typename Op, typename Handler>
    static void parallel_shards_ordered(ShardedOps<Op> &sharded_ops, std::uint64_t batch_id,
                                        OrderedBatchOptions options, Task *owner,
                                        Handler &&handler);

    template <typename Op, typename Handler>
    static void parallel_shards(ShardedOps<Op> &sharded_ops, ParallelMode mode,
                                std::uint64_t batch_id, Task *owner, Handler &&handler);

    template <typename StreamTag, typename ApplyTaskT, typename Batch>
    [[nodiscard]] static bool start_ordered_task(Thread sequencer_thread, Batch &&batch);

    [[nodiscard]] static std::uint64_t ordered_last_applied_batch_id(Thread thread) noexcept;

private:
    using RuntimeStatus = detail::RuntimeStatus;
    template <typename T> using CacheLineAtomic = detail::CacheLineAtomic<T>;
    using OrderedBatchState = detail::OrderedBatchState;
    using ExternalPostCounter = detail::ExternalPostCounter;
    using ParallelGroup = detail::RuntimeParallelGroup<AsyncRuntime>;
    using SpscQueue = detail::BoundedSpscQueue<Task>;
    using ExternalQueue = detail::BoundedMpscQueue<Task>;
    template <typename TaskT>
    using TaskPool = detail::ObjectPool<
        TaskT, Config::task_pool_chunk_size, Config::task_pool_remote_release_batch_size,
        Config::task_pool_cache_slot_index, Config::task_pool_local_cache_set_size,
        Config::task_pool_direct_release_set_size, Config::task_pool_local_cache_capacity>;
    using ParallelGroupPool = detail::ObjectPool<
        ParallelGroup, Config::task_pool_chunk_size, Config::task_pool_remote_release_batch_size,
        Config::task_pool_cache_slot_index, Config::task_pool_local_cache_set_size,
        Config::task_pool_direct_release_set_size, Config::task_pool_local_cache_capacity>;

    enum class ReadyQueueRoute : std::uint8_t {
        Local,
        Spsc,
    };

    static void set_task_parallel_failures(Task *task, std::uint32_t failures) noexcept {
        task->set_last_parallel_failures(failures);
    }

    template <typename Op, typename Handler, bool Ordered>
    static void parallel_shards_impl(std::bool_constant<Ordered> ordered, Thread shard_begin,
                                     ShardedOps<Op> &sharded_ops, ParallelMode mode,
                                     std::uint64_t batch_id, OrderedBatchOptions options,
                                     Task *owner, Handler &&handler);

    enum class OrderedGuardDecision : std::uint8_t {
        Run,
        SkipAlreadyApplied,
        Fail,
    };

    [[nodiscard]] static OrderedGuardDecision
    check_order_guard(std::uint64_t batch_id, OrderedBatchOptions options) noexcept;
    static void commit_order_guard(std::uint64_t batch_id) noexcept;

    template <typename StreamTag, typename ApplyTaskT, typename BatchT> struct OrderedStartState;

    template <typename StreamTag, typename ApplyTaskT, typename BatchT>
    [[nodiscard]] static OrderedStartState<StreamTag, ApplyTaskT, BatchT> &ordered_start_state();

    template <typename StreamTag, typename ApplyTaskT, typename Batch> class OrderedStartTask;

    template <typename Op, typename Handler, bool Ordered>
    [[nodiscard]] static bool run_parallel_shard(std::uint16_t shard_index, std::uint64_t batch_id,
                                                 OrderedBatchOptions options, std::vector<Op> &ops,
                                                 Handler &handler) noexcept;

    template <typename Op, typename Handler, bool Ordered> class ShardTask;

    template <typename TaskT> [[nodiscard]] static TaskPool<TaskT> &task_pool();
    [[nodiscard]] static ParallelGroupPool &parallel_group_pool();
    [[nodiscard]] static ParallelGroup *
    create_parallel_group(std::uint32_t target_count, Task *owner, std::uint16_t resume_thread);
    static void destroy_parallel_group(ParallelGroup *group) noexcept;

    template <typename TaskT, typename... Args>
    [[nodiscard]] static TaskT *allocate_task(Args &&...args);

    template <typename TaskT> static void destroy_task(Task *task) noexcept;

    [[nodiscard]] static bool is_task_created(Task *task) noexcept;
    static void release_task_handle(Task *task) noexcept;

    static void reset_task_registry() noexcept;
    static void register_task(Task *task) noexcept;
    static void unregister_task(Task *task) noexcept;
    static void cancel_registered_tasks() noexcept;
    static void cancel_registered_task(Task *task) noexcept;
    static void on_task_started(Task *task) noexcept;
    static void on_task_finished(Task *task) noexcept;

    static void init_queues();
    [[nodiscard]] static SpscQueue &spsc_queue(std::uint16_t source, std::uint16_t target) noexcept;

    [[nodiscard]] static constexpr ReadyQueueRoute
    ready_route_from_runtime_thread(std::uint16_t source, std::uint16_t target) noexcept;

    static bool try_enqueue_ready(std::uint16_t index, Task *task) noexcept;
    static bool try_enqueue_ready_from_runtime_thread(std::uint16_t source, std::uint16_t target,
                                                      Task *task) noexcept;
    static bool try_enqueue_local_from_runtime_thread(std::uint16_t target, Task *task) noexcept;
    static bool try_enqueue_cross_thread_spsc(std::uint16_t source, std::uint16_t target,
                                              Task *task) noexcept;
    static bool try_enqueue_external_mpsc(std::uint16_t target, Task *task) noexcept;
    static void mark_source_ready(std::uint16_t source, std::uint16_t target) noexcept;

    static void enqueue_ready_blocking(std::uint16_t index, Task *task) noexcept;
    static void enqueue_ready_blocking_from_runtime_thread(std::uint16_t source,
                                                           std::uint16_t target,
                                                           Task *task) noexcept;
    static void enqueue_local_from_runtime_thread_blocking(std::uint16_t target,
                                                           Task *task) noexcept;
    static void enqueue_cross_thread_spsc_blocking(std::uint16_t source, std::uint16_t target,
                                                   Task *task) noexcept;
    static void enqueue_external_mpsc_blocking(std::uint16_t target, Task *task) noexcept;

    static void post_blocking(Thread thread, Task *task) noexcept;
    static bool enqueue_ready_by_policy(std::uint16_t index, Task *task) noexcept;
    static void enqueue_pending_blocking(std::uint16_t index, Task *task) noexcept;

    [[nodiscard]] static bool try_enter_post(std::uint16_t target) noexcept;
    static void leave_post(std::uint16_t target) noexcept;
    static void wait_for_external_posts() noexcept;

    using Executor = detail::Executor<AsyncRuntime, Traits>;

    static inline CacheLineAtomic<RuntimeStatus> status_{RuntimeStatus::Stopped};
    static inline std::array<ExternalPostCounter, thread_count> active_external_posts_{};
    static inline CacheLineAtomic<std::uint32_t> unfinished_tasks_{0};
    static inline CacheLineAtomic<std::uint64_t> generation_{0};
    static inline std::vector<std::unique_ptr<Executor>> executors_;
    static inline std::vector<std::unique_ptr<SpscQueue>> spsc_queues_;
    static inline std::vector<std::unique_ptr<ExternalQueue>> external_queues_;
    static inline std::vector<OrderedBatchState> ordered_batch_state_;
    static inline std::mutex task_registry_mutex_;
    static inline Task *task_registry_head_{nullptr};
    static inline thread_local std::uint16_t current_thread_index_ = invalid_thread_index;
};

} // namespace af

#define AF_ASYNC_RUNTIME_IMPL_INCLUDE 1
#include "af/detail/runtime/runtime_dispatch.hpp"
#include "af/detail/runtime/runtime_executor.hpp"
#include "af/detail/runtime/runtime_parallel.hpp"
#include "af/detail/runtime/runtime_public_api.hpp"
#include "af/detail/runtime/runtime_task_lifecycle.hpp"
#undef AF_ASYNC_RUNTIME_IMPL_INCLUDE
