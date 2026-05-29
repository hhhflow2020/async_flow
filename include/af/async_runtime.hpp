#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstdint>
#include <memory>
#include <mutex>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/detail/bounded_queues.hpp"
#include "af/detail/config.hpp"
#include "af/detail/object_pool.hpp"
#include "af/task.hpp"
#include "absl/container/flat_hash_map.h"

#if defined(__linux__)
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
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

    static constexpr std::size_t spsc_queue_capacity = [] {
        if constexpr (requires { Traits::spsc_queue_capacity; }) {
            return static_cast<std::size_t>(Traits::spsc_queue_capacity);
        } else {
            return static_cast<std::size_t>(1024);
        }
    }();
    static constexpr std::size_t external_queue_capacity = [] {
        if constexpr (requires { Traits::external_queue_capacity; }) {
            return static_cast<std::size_t>(Traits::external_queue_capacity);
        } else {
            return spsc_queue_capacity;
        }
    }();
    static constexpr QueueFullPolicy queue_full_policy = [] {
        if constexpr (requires { Traits::queue_full_policy; }) {
            return Traits::queue_full_policy;
        } else {
            return QueueFullPolicy::Reject;
        }
    }();
    static constexpr ShutdownPolicy shutdown_policy = [] {
        if constexpr (requires { Traits::shutdown_policy; }) {
            return Traits::shutdown_policy;
        } else {
            return ShutdownPolicy::WaitForTasks;
        }
    }();
    static constexpr bool task_registry_enabled = [] {
        if constexpr (requires { Traits::enable_task_registry; }) {
            return static_cast<bool>(Traits::enable_task_registry);
        } else {
            return false;
        }
    }();

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

    [[nodiscard]] static bool io_wait(
        Thread thread,
        int fd,
        std::uint32_t events,
        Task* task,
        IoResult* result) noexcept {
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
        return executors_[index]->register_io_wait(fd, events, task, result);
    }

private:
    enum class RuntimeStatus : std::uint8_t {
        Stopped,
        Starting,
        Running,
        Stopping,
    };

    struct alignas(detail::hardware_cache_line_size) OrderedBatchState {
        std::uint64_t last_applied_batch_id{0};
    };

    struct ParallelGroup {
        std::atomic<std::uint32_t> pending{0};
        Task* owner{nullptr};
        std::uint16_t resume_thread{invalid_thread_index};
        std::atomic<std::uint32_t> failed{0};

        void init(std::uint32_t target_count, Task* group_owner, std::uint16_t group_resume_thread) noexcept {
            pending.store(target_count, std::memory_order_relaxed);
            owner = group_owner;
            resume_thread = group_resume_thread;
            failed.store(0, std::memory_order_relaxed);
        }

        void complete(bool ok, bool resume_owner = true) noexcept {
            if (!ok) {
                failed.fetch_add(1, std::memory_order_relaxed);
            }
            if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
                if (resume_owner && owner != nullptr && resume_thread < thread_count) {
                    owner->set_last_parallel_failures(failed.load(std::memory_order_acquire));
                    post_blocking(thread_from_index(resume_thread), owner);
                }
                destroy_parallel_group(this);
            }
        }
    };

    struct alignas(detail::hardware_cache_line_size) ExternalPostCounter {
        std::atomic<std::uint32_t> value{0};
    };

    enum class OrderedGuardDecision : std::uint8_t {
        Run,
        SkipAlreadyApplied,
        Fail,
    };

    template <typename StreamTag, typename ApplyTaskT, typename BatchT>
    struct OrderedStartState {
        std::uint64_t next_batch_id{1};
        std::uint64_t generation{0};
        absl::flat_hash_map<std::uint64_t, BatchT> pending;

        void reset(std::uint64_t runtime_generation) {
            next_batch_id = 1;
            generation = runtime_generation;
            pending.clear();
        }

        [[nodiscard]] bool submit(BatchT batch) {
            const std::uint64_t batch_id = batch.batch_id;
            if (batch_id < next_batch_id) {
                return true;
            }

            if (batch_id > next_batch_id) {
                pending.emplace(batch_id, std::move(batch));
                return true;
            }

            if (!start_ready(std::move(batch))) {
                return false;
            }
            return drain_ready();
        }

        [[nodiscard]] bool start_ready(BatchT batch) {
            const bool ok = AsyncRuntime::start_task<ApplyTaskT>(std::move(batch));
            if (!ok) {
                return false;
            }
            ++next_batch_id;
            return true;
        }

        [[nodiscard]] bool drain_ready() {
            for (;;) {
                auto it = pending.find(next_batch_id);
                if (it == pending.end()) {
                    return true;
                }

                BatchT batch = std::move(it->second);
                pending.erase(it);
                if (!start_ready(std::move(batch))) {
                    return false;
                }
            }
        }
    };

    template <typename StreamTag, typename ApplyTaskT, typename BatchT>
    [[nodiscard]] static OrderedStartState<StreamTag, ApplyTaskT, BatchT>& ordered_start_state() {
        static std::vector<OrderedStartState<StreamTag, ApplyTaskT, BatchT>> states(thread_count);
        const std::uint16_t index = current_thread_index();
        AF_ASSERT(index < states.size());
        auto& state = states[index];
        const std::uint64_t runtime_generation = generation_.load(std::memory_order_acquire);
        if (state.generation != runtime_generation) {
            state.reset(runtime_generation);
        }
        return state;
    }

    template <typename StreamTag, typename ApplyTaskT, typename BatchT>
    class OrderedStartTask final : public Task {
    public:
        explicit OrderedStartTask(typename Task::FactoryToken token) : Task(token) {}

        bool do_it(Thread sequencer_thread, BatchT batch) {
            batch_ = std::move(batch);
            return this->schedule(sequencer_thread);
        }

    private:
        TaskResult run() override {
            const bool ok = ordered_start_state<StreamTag, ApplyTaskT, BatchT>().submit(
                std::move(batch_));
            return ok ? this->done() : this->failed();
        }

        BatchT batch_{};
    };

    template <typename Op, typename Handler, bool Ordered>
    class ShardTask final : public Task {
    public:
        ShardTask(
            typename Task::FactoryToken token,
            ParallelGroup* group,
            std::uint16_t shard_index,
            std::uint64_t batch_id,
            OrderedBatchOptions options,
            std::vector<Op>&& ops,
            Handler handler)
            : Task(token),
              group_(group),
              shard_index_(shard_index),
              batch_id_(batch_id),
              options_(options),
              ops_(std::move(ops)),
              handler_(std::move(handler)) {}

    private:
        TaskResult run() override {
            const bool ok = run_parallel_shard<Op, Handler, Ordered>(
                shard_index_,
                batch_id_,
                options_,
                ops_,
                handler_);
            group_->complete(ok);
            return this->done();
        }

        void on_runtime_cancel() noexcept override {
            group_->complete(false, false);
        }

        ParallelGroup* group_;
        std::uint16_t shard_index_;
        std::uint64_t batch_id_;
        OrderedBatchOptions options_;
        std::vector<Op> ops_;
        Handler handler_;
    };

    template <typename Op, typename Handler, bool Ordered>
    [[nodiscard]] static bool run_parallel_shard(
        std::uint16_t shard_index,
        std::uint64_t batch_id,
        OrderedBatchOptions options,
        std::vector<Op>& ops,
        Handler& handler) noexcept {
        bool ok = true;
        bool skip_handler = false;
        if constexpr (Ordered) {
            const OrderedGuardDecision decision = check_order_guard(batch_id, options);
            ok = decision != OrderedGuardDecision::Fail;
            skip_handler = decision == OrderedGuardDecision::SkipAlreadyApplied;
        }

        if (ok && !skip_handler) {
            try {
                if constexpr (Ordered) {
                    using HandlerResult =
                        std::invoke_result_t<Handler&, std::uint16_t, std::vector<Op>&, std::uint64_t>;
                    if constexpr (std::is_same_v<HandlerResult, bool>) {
                        ok = handler(shard_index, ops, batch_id);
                    } else {
                        handler(shard_index, ops, batch_id);
                    }
                } else {
                    using HandlerResult =
                        std::invoke_result_t<Handler&, std::uint16_t, std::vector<Op>&>;
                    if constexpr (std::is_same_v<HandlerResult, bool>) {
                        ok = handler(shard_index, ops);
                    } else {
                        handler(shard_index, ops);
                    }
                }
            } catch (...) {
                AF_ASSERT(false && "parallel shard handler must not throw");
                ok = false;
            }
        }

        if constexpr (Ordered) {
            if (ok && !skip_handler) {
                commit_order_guard(batch_id);
            }
        }
        return ok;
    }

    [[nodiscard]] static OrderedGuardDecision check_order_guard(
        std::uint64_t batch_id,
        OrderedBatchOptions options) noexcept {
        const std::uint16_t thread = AsyncRuntime::current_thread_index();
        AF_ASSERT(thread < ordered_batch_state_.size());
        auto& state = ordered_batch_state_[thread];
        if (batch_id == state.last_applied_batch_id + 1U) {
            return OrderedGuardDecision::Run;
        }
        if (options.replay_policy == OrderedBatchReplayPolicy::SkipAlreadyApplied &&
            batch_id == state.last_applied_batch_id) {
            return OrderedGuardDecision::SkipAlreadyApplied;
        }
        const bool ok = false;
        AF_ASSERT(ok && "ordered batch id must be contiguous per shard");
        static_cast<void>(ok);
        return OrderedGuardDecision::Fail;
    }

    static void commit_order_guard(std::uint64_t batch_id) noexcept {
        const std::uint16_t thread = AsyncRuntime::current_thread_index();
        ordered_batch_state_[thread].last_applied_batch_id = batch_id;
    }

    template <typename Op, typename Handler>
    static void parallel_shards_impl(
        std::bool_constant<false>,
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        std::uint64_t batch_id,
        OrderedBatchOptions ordered_options,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl_typed<Op, Handler, false>(
            shard_begin,
            sharded_ops,
            mode,
            batch_id,
            ordered_options,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler>
    static void parallel_shards_impl(
        std::bool_constant<true>,
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        std::uint64_t batch_id,
        OrderedBatchOptions ordered_options,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl_typed<Op, Handler, true>(
            shard_begin,
            sharded_ops,
            mode,
            batch_id,
            ordered_options,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler, bool Ordered>
    static void parallel_shards_impl_typed(
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        std::uint64_t batch_id,
        OrderedBatchOptions ordered_options,
        Task* owner,
        Handler&& handler) {
        AF_ASSERT(owner != nullptr);
        AF_ASSERT(is_runtime_thread() && "parallel_shards must be called from a runtime thread");

        const std::uint16_t begin = thread_index(shard_begin);
        const std::uint16_t shard_count = sharded_ops.shard_count();
        AF_ASSERT(begin + shard_count <= thread_count);

        std::uint32_t target_count = shard_count;
        if (mode == ParallelMode::NonEmptyOnly) {
            target_count = 0;
            for (std::uint16_t i = 0; i < shard_count; ++i) {
                if (!sharded_ops.shards[i].empty()) {
                    ++target_count;
                }
            }
        }

        if (target_count == 0) {
            post_blocking(current_thread(), owner);
            return;
        }

        auto* group = create_parallel_group(target_count, owner, current_thread_index());

        using HandlerT = std::decay_t<Handler>;
        for (std::uint16_t i = 0; i < shard_count; ++i) {
            if (mode == ParallelMode::NonEmptyOnly && sharded_ops.shards[i].empty()) {
                continue;
            }

            const Thread thread = thread_from_index(static_cast<std::uint16_t>(begin + i));
            if (thread_index(thread) == current_thread_index()) {
                auto ops = std::move(sharded_ops.shards[i]);
                HandlerT local_handler(handler);
                const bool ok = run_parallel_shard<Op, HandlerT, Ordered>(
                    i,
                    batch_id,
                    ordered_options,
                    ops,
                    local_handler);
                group->complete(ok);
                continue;
            }

            Task* shard_task = nullptr;
            if constexpr (Ordered) {
                shard_task = allocate_task<ShardTask<Op, HandlerT, true>>(
                    group,
                    i,
                    batch_id,
                    ordered_options,
                    std::move(sharded_ops.shards[i]),
                    HandlerT(handler));
            } else {
                shard_task = allocate_task<ShardTask<Op, HandlerT, false>>(
                    group,
                    i,
                    0,
                    OrderedBatchOptions{},
                    std::move(sharded_ops.shards[i]),
                    HandlerT(handler));
            }

            post_blocking(thread, shard_task);
        }
    }

    class alignas(detail::hardware_cache_line_size) Executor {
    public:
        explicit Executor(std::uint16_t index)
            : index_(index),
              kind_(thread_kind(thread_from_index(index))),
              local_queue_(detail::next_power_of_two(spsc_queue_capacity < 2 ? 2 : spsc_queue_capacity)) {}
        Executor(const Executor&) = delete;
        Executor& operator=(const Executor&) = delete;

        ~Executor() {
            close_io_backend();
        }

        void start() {
            init_io_backend();
            worker_ = std::thread([this] {
                run_loop();
            });
        }

        void request_stop() noexcept {
            stop_requested_.store(true, std::memory_order_release);
            notify_force();
        }

        void join() {
            if (worker_.joinable()) {
                worker_.join();
            }
        }

        void notify() noexcept {
            if (!sleeping_.load(std::memory_order_acquire)) {
                return;
            }

            bool expected = true;
            if (sleeping_.compare_exchange_strong(
                    expected,
                    false,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                notify_force();
            }
        }

        [[nodiscard]] bool io_backend_available() const noexcept {
#if defined(__linux__)
            return io_epoll_fd_ >= 0;
#else
            return false;
#endif
        }

        [[nodiscard]] bool register_io_wait(
            int fd,
            std::uint32_t events,
            Task* task,
            IoResult* result) noexcept {
            AF_ASSERT(current_thread_index_ == index_ && "io_wait must be called from its IO thread");
            if (current_thread_index_ != index_ || task == nullptr || result == nullptr) {
                if (result != nullptr) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = EINVAL;
                }
                return false;
            }

#if defined(__linux__)
            if (io_epoll_fd_ < 0 || fd < 0 || events == 0U || io_waits_.find(fd) != io_waits_.end()) {
                result->fd = fd;
                result->events = io_error;
                if (fd < 0) {
                    result->error = EBADF;
                } else if (events == 0U) {
                    result->error = EINVAL;
                } else if (io_epoll_fd_ < 0) {
                    result->error = ENOSYS;
                } else {
                    result->error = EALREADY;
                }
                return false;
            }

            std::uint32_t native_events = EPOLLERR | EPOLLHUP | EPOLLONESHOT;
            if ((events & io_readable) != 0U) {
                native_events |= EPOLLIN;
            }
            if ((events & io_writable) != 0U) {
                native_events |= EPOLLOUT;
            }

            epoll_event event{};
            event.events = native_events;
            event.data.fd = fd;

            if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, fd, &event) != 0) {
                if (errno != EEXIST ||
                    ::epoll_ctl(io_epoll_fd_, EPOLL_CTL_MOD, fd, &event) != 0) {
                    result->fd = fd;
                    result->events = io_error;
                    result->error = errno;
                    return false;
                }
            }

            *result = IoResult{fd, 0, 0};
            io_waits_.emplace(fd, IoWaitRegistration{task, result});
            return true;
#else
            static_cast<void>(fd);
            static_cast<void>(events);
            static_cast<void>(task);
            result->error = ENOSYS;
            return false;
#endif
        }

        void mark_ready(std::uint16_t source) noexcept {
            if constexpr (thread_count <= 64U) {
                const std::uint64_t bit = 1ULL << source;
                std::uint64_t mask = ready_sources_.load(std::memory_order_acquire);
                while ((mask & bit) == 0U &&
                       !ready_sources_.compare_exchange_weak(
                           mask,
                           mask | bit,
                           std::memory_order_release,
                           std::memory_order_acquire)) {
                }
            } else {
                static_cast<void>(source);
            }
        }

        void notify_external_ready() noexcept {
            if (!external_ready_.load(std::memory_order_acquire)) {
                external_ready_.store(true, std::memory_order_release);
                notify_force();
                return;
            }
            notify();
        }

        [[nodiscard]] bool try_push_local(Task* task) noexcept {
            if (local_size_ == local_queue_.size()) {
                return false;
            }

            local_queue_[local_tail_ & (local_queue_.size() - 1U)] = task;
            ++local_tail_;
            ++local_size_;
            return true;
        }

        [[nodiscard]] Task* try_pop_local() noexcept {
            if (local_size_ == 0) {
                return nullptr;
            }

            Task* task = local_queue_[local_head_ & (local_queue_.size() - 1U)];
            ++local_head_;
            --local_size_;
            return task;
        }

        void execute(Task* task) noexcept {
            const TaskState previous = task->state_.exchange(
                TaskState::Running,
                std::memory_order_acq_rel);
            AF_ASSERT(previous == TaskState::Queued);
            if (previous != TaskState::Queued) {
                return;
            }

            TaskResult result = TaskResult::Done;
            try {
                result = task->run();
            } catch (...) {
                AF_ASSERT(false && "task::run must not throw");
                result = TaskResult::Done;
            }

            switch (result) {
            case TaskResult::Done:
                finish_done(task);
                break;
            case TaskResult::Pending:
                finish_pending(task);
                break;
            case TaskResult::Again:
                finish_again(task);
                break;
            case TaskResult::Failed:
                finish_done(task);
                break;
            case TaskResult::Cancelled:
                finish_done(task);
                break;
            }
        }

    private:
        [[nodiscard]] bool io_thread() const noexcept {
            return kind_ == ThreadKind::Epoll;
        }

        void notify_force() noexcept {
#if defined(__linux__)
            if (io_epoll_fd_ >= 0) {
                bool expected = false;
                if (io_wake_pending_.compare_exchange_strong(
                        expected,
                        true,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    const std::uint64_t value = 1;
                    const auto written = ::write(io_wake_fd_, &value, sizeof(value));
                    static_cast<void>(written);
                }
                return;
            }
#endif
            wake_epoch_.fetch_add(1, std::memory_order_release);
            wake_epoch_.notify_one();
        }

        void run_loop() noexcept {
            current_thread_index_ = index_;

            for (;;) {
                bool did_work = false;
                while (Task* task = pop_one()) {
                    did_work = true;
                    execute(task);
                }

                if (stop_requested_.load(std::memory_order_acquire)) {
                    if (!did_work) {
                        break;
                    }
                    continue;
                }

                if (poll_io(0)) {
                    continue;
                }

                const std::uint32_t observed = wake_epoch_.load(std::memory_order_acquire);
                sleeping_.store(true, std::memory_order_release);
                if (stop_requested_.load(std::memory_order_acquire)) {
                    sleeping_.store(false, std::memory_order_relaxed);
                    continue;
                }

                if (Task* task = pop_one()) {
                    sleeping_.store(false, std::memory_order_relaxed);
                    execute(task);
                } else if (poll_io(0)) {
                    sleeping_.store(false, std::memory_order_relaxed);
                } else {
                    if (io_thread() && io_backend_available()) {
                        static_cast<void>(poll_io(-1));
                    } else if (wake_epoch_.load(std::memory_order_acquire) == observed) {
                        wake_epoch_.wait(observed, std::memory_order_acquire);
                    }
                    sleeping_.store(false, std::memory_order_relaxed);
                }
            }

            current_thread_index_ = invalid_thread_index;
        }

        void init_io_backend() noexcept {
#if defined(__linux__)
            if (!io_thread() || io_epoll_fd_ >= 0) {
                return;
            }

            io_epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
            if (io_epoll_fd_ < 0) {
                return;
            }

            io_wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
            if (io_wake_fd_ < 0) {
                close_io_backend();
                return;
            }

            epoll_event event{};
            event.events = EPOLLIN;
            event.data.fd = io_wake_fd_;
            if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, io_wake_fd_, &event) != 0) {
                close_io_backend();
            }
#endif
        }

        void close_io_backend() noexcept {
#if defined(__linux__)
            io_waits_.clear();
            if (io_wake_fd_ >= 0) {
                ::close(io_wake_fd_);
                io_wake_fd_ = -1;
            }
            if (io_epoll_fd_ >= 0) {
                ::close(io_epoll_fd_);
                io_epoll_fd_ = -1;
            }
            io_wake_pending_.store(false, std::memory_order_relaxed);
#endif
        }

        [[nodiscard]] bool poll_io(int timeout_ms) noexcept {
#if defined(__linux__)
            if (io_epoll_fd_ < 0) {
                return false;
            }

            std::array<epoll_event, 64> events{};
            const int count = ::epoll_wait(
                io_epoll_fd_,
                events.data(),
                static_cast<int>(events.size()),
                timeout_ms);
            if (count <= 0) {
                return false;
            }

            bool did_work = false;
            for (int i = 0; i < count; ++i) {
                const int fd = events[static_cast<std::size_t>(i)].data.fd;
                if (fd == io_wake_fd_) {
                    drain_io_wake();
                    did_work = true;
                    continue;
                }

                auto it = io_waits_.find(fd);
                if (it == io_waits_.end()) {
                    continue;
                }

                IoWaitRegistration registration = it->second;
                io_waits_.erase(it);
                static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));

                registration.result->fd = fd;
                registration.result->events = io_events_from_native(
                    events[static_cast<std::size_t>(i)].events);
                registration.result->error = 0;
                enqueue_pending_blocking(index_, registration.task);
                did_work = true;
            }
            return did_work;
#else
            static_cast<void>(timeout_ms);
            return false;
#endif
        }

#if defined(__linux__)
        void drain_io_wake() noexcept {
            std::uint64_t value = 0;
            while (::read(io_wake_fd_, &value, sizeof(value)) == sizeof(value)) {
            }
            io_wake_pending_.store(false, std::memory_order_release);
        }

        [[nodiscard]] static std::uint32_t io_events_from_native(std::uint32_t events) noexcept {
            std::uint32_t result = 0;
            if ((events & EPOLLIN) != 0U) {
                result |= io_readable;
            }
            if ((events & EPOLLOUT) != 0U) {
                result |= io_writable;
            }
            if ((events & EPOLLERR) != 0U) {
                result |= io_error;
            }
            if ((events & EPOLLHUP) != 0U) {
                result |= io_hangup;
            }
            return result;
        }
#endif

        Task* pop_one() noexcept {
            if (Task* task = try_pop_local()) {
                return task;
            }

            if constexpr (thread_count <= 64U) {
                std::uint64_t mask = ready_sources_.load(std::memory_order_acquire);
                while (mask != 0U) {
                    const auto source = static_cast<std::uint16_t>(std::countr_zero(mask));
                    const std::uint64_t bit = 1ULL << source;
                    if (Task* task = spsc_queue(source, index_).try_pop()) {
                        return task;
                    }

                    ready_sources_.fetch_and(~bit, std::memory_order_acq_rel);
                    if (Task* task = spsc_queue(source, index_).try_pop()) {
                        mark_ready(source);
                        return task;
                    }

                    mask &= ~bit;
                }
            } else {
                for (std::uint16_t checked = 0; checked < thread_count; ++checked) {
                    const std::uint16_t source =
                        static_cast<std::uint16_t>((next_source_ + checked) % thread_count);
                    if (Task* task = spsc_queue(source, index_).try_pop()) {
                        next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
                        return task;
                    }
                }
            }

            if (external_ready_.load(std::memory_order_acquire)) {
                if (Task* task = external_queues_[index_]->try_pop()) {
                    return task;
                }

                external_ready_.store(false, std::memory_order_release);
                if (Task* task = external_queues_[index_]->try_pop()) {
                    external_ready_.store(true, std::memory_order_release);
                    return task;
                }
            }

            return external_queues_[index_]->try_pop();
        }

        void finish_done(Task* task) noexcept {
            const std::uint16_t requested = task->take_requested_thread();
            AF_ASSERT(requested == invalid_thread_index);
            task->state_.store(TaskState::Done, std::memory_order_release);
            on_task_finished(task);
            task->release_lifetime_ref();
        }

        void finish_pending(Task* task) noexcept {
            task->state_.store(TaskState::Pending, std::memory_order_release);
            const std::uint16_t requested = task->take_requested_thread();
            if (requested != invalid_thread_index) {
                enqueue_pending_blocking(requested, task);
            }
        }

        void finish_again(Task* task) noexcept {
            const std::uint16_t requested = task->take_requested_thread();
            AF_ASSERT(requested == invalid_thread_index);
            task->state_.store(TaskState::Queued, std::memory_order_release);
            enqueue_ready_blocking_from(index_, index_, task);
        }

        std::uint16_t index_;
        ThreadKind kind_{ThreadKind::Worker};
        std::uint16_t next_source_{0};
        std::vector<Task*> local_queue_;
        std::size_t local_head_{0};
        std::size_t local_tail_{0};
        std::size_t local_size_{0};
        alignas(detail::hardware_cache_line_size) std::atomic<std::uint64_t> ready_sources_{0};
        alignas(detail::hardware_cache_line_size) std::atomic<bool> external_ready_{false};
        alignas(detail::hardware_cache_line_size) std::atomic<std::uint32_t> wake_epoch_{0};
        std::atomic<bool> sleeping_{false};
        std::atomic<bool> stop_requested_{false};
#if defined(__linux__)
        struct IoWaitRegistration {
            Task* task{nullptr};
            IoResult* result{nullptr};
        };

        int io_epoll_fd_{-1};
        int io_wake_fd_{-1};
        absl::flat_hash_map<int, IoWaitRegistration> io_waits_;
        alignas(detail::hardware_cache_line_size) std::atomic<bool> io_wake_pending_{false};
#endif
        std::thread worker_;
    };

    using SpscQueue = detail::BoundedSpscQueue<Task>;
    using ExternalQueue = detail::BoundedMpscQueue<Task>;

    template <typename TaskT>
    using TaskPool = detail::ObjectPool<TaskT>;

    using ParallelGroupPool = detail::ObjectPool<ParallelGroup>;

    template <typename TaskT>
    [[nodiscard]] static TaskPool<TaskT>& task_pool() {
        static TaskPool<TaskT> pool;
        return pool;
    }

    [[nodiscard]] static ParallelGroupPool& parallel_group_pool() {
        static ParallelGroupPool pool;
        return pool;
    }

    [[nodiscard]] static ParallelGroup* create_parallel_group(
        std::uint32_t target_count,
        Task* owner,
        std::uint16_t resume_thread) {
        auto* group = parallel_group_pool().create();
        group->init(target_count, owner, resume_thread);
        return group;
    }

    static void destroy_parallel_group(ParallelGroup* group) noexcept {
        parallel_group_pool().destroy(group);
    }

    template <typename TaskT, typename... Args>
    [[nodiscard]] static TaskT* allocate_task(Args&&... args) {
        auto* task = task_pool<TaskT>().create(
            typename Task::FactoryToken{},
            std::forward<Args>(args)...);
        task->set_destroy_fn(&destroy_task<TaskT>);
        return task;
    }

    template <typename TaskT>
    static void destroy_task(Task* task) noexcept {
        task_pool<TaskT>().destroy(static_cast<TaskT*>(task));
    }

    [[nodiscard]] static bool is_task_created(Task* task) noexcept {
        return task != nullptr && task->is_created();
    }

    static void release_task_handle(Task* task) noexcept {
        if (task == nullptr) {
            return;
        }

        const TaskState state = task->state_.load(std::memory_order_acquire);
        task->release_lifetime_ref();
        if (state == TaskState::Created) {
            task->release_lifetime_ref();
        }
    }

    static void init_queues() {
        spsc_queues_.clear();
        spsc_queues_.reserve(static_cast<std::size_t>(thread_count) * thread_count);
        for (std::uint16_t source = 0; source < thread_count; ++source) {
            for (std::uint16_t target = 0; target < thread_count; ++target) {
                static_cast<void>(target);
                spsc_queues_.push_back(std::make_unique<SpscQueue>(spsc_queue_capacity));
            }
        }

        external_queues_.clear();
        external_queues_.reserve(thread_count);
        for (std::uint16_t target = 0; target < thread_count; ++target) {
            static_cast<void>(target);
            external_queues_.push_back(std::make_unique<ExternalQueue>(external_queue_capacity));
        }
    }

    static void post_blocking(Thread thread, Task* task) noexcept {
        const std::uint16_t index = thread_index(thread);
        AF_ASSERT(index < thread_count);

        const detail::ScheduleRequest request = task->request_schedule(index);
        switch (request.action) {
        case detail::ScheduleAction::Enqueue:
            if (request.previous == TaskState::Created) {
                on_task_started(task);
            }
            enqueue_ready_blocking(index, task);
            return;
        case detail::ScheduleAction::Deferred:
            return;
        case detail::ScheduleAction::Rejected:
            AF_ASSERT(false && "failed to schedule task");
            return;
        }
    }

    static bool enqueue_ready_by_policy(std::uint16_t index, Task* task) noexcept {
        if constexpr (queue_full_policy == QueueFullPolicy::Yield) {
            enqueue_ready_blocking(index, task);
            return true;
        } else {
            return try_enqueue_ready(index, task);
        }
    }

    static bool try_enqueue_ready(std::uint16_t index, Task* task) noexcept {
        if (current_thread_index_ < thread_count) {
            return try_enqueue_ready_from(current_thread_index_, index, task);
        }

        const bool ok = external_queues_[index]->try_push(task);
        if (ok) {
            executors_[index]->notify_external_ready();
        }
        return ok;
    }

    static bool try_enqueue_ready_from(
        std::uint16_t source,
        std::uint16_t target,
        Task* task) noexcept {
        if (source == target) {
            return executors_[target]->try_push_local(task);
        }

        const bool ok = spsc_queue(source, target).try_push(task);
        if (ok) {
            mark_source_ready(source, target);
            executors_[target]->notify();
        }
        return ok;
    }

    static void enqueue_ready_blocking(std::uint16_t index, Task* task) noexcept {
        if (current_thread_index_ < thread_count) {
            enqueue_ready_blocking_from(current_thread_index_, index, task);
            return;
        }

        while (!external_queues_[index]->try_push(task)) {
            std::this_thread::yield();
        }
        executors_[index]->notify_external_ready();
    }

    static void enqueue_ready_blocking_from(
        std::uint16_t source,
        std::uint16_t target,
        Task* task) noexcept {
        if (source == target) {
            Executor& executor = *executors_[target];
            while (!executor.try_push_local(task)) {
                if (Task* ready = executor.try_pop_local()) {
                    executor.execute(ready);
                } else {
                    std::this_thread::yield();
                }
            }
            return;
        }

        while (!spsc_queue(source, target).try_push(task)) {
            std::this_thread::yield();
        }
        mark_source_ready(source, target);
        executors_[target]->notify();
    }

    [[nodiscard]] static SpscQueue& spsc_queue(
        std::uint16_t source,
        std::uint16_t target) noexcept {
        return *spsc_queues_[static_cast<std::size_t>(source) * thread_count + target];
    }

    static void mark_source_ready(std::uint16_t source, std::uint16_t target) noexcept {
        if constexpr (thread_count <= 64U) {
            executors_[target]->mark_ready(source);
        } else {
            static_cast<void>(source);
            static_cast<void>(target);
        }
    }

    static void enqueue_pending_blocking(std::uint16_t index, Task* task) noexcept {
        TaskState expected = TaskState::Pending;
        [[maybe_unused]] const bool ok = task->state_.compare_exchange_strong(
                expected,
                TaskState::Queued,
                std::memory_order_acq_rel,
                std::memory_order_acquire);
        AF_ASSERT(ok);
        enqueue_ready_blocking(index, task);
    }

    [[nodiscard]] static bool try_enter_post(std::uint16_t target) noexcept {
        const RuntimeStatus status = status_.load(std::memory_order_acquire);
        if (current_thread_index_ < thread_count) {
            if (status == RuntimeStatus::Running) {
                return true;
            }

            if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
                return status == RuntimeStatus::Stopping;
            }

            return false;
        }

        if (status == RuntimeStatus::Running) {
            active_external_posts_[target].value.fetch_add(1, std::memory_order_acq_rel);
            if (status_.load(std::memory_order_acquire) == RuntimeStatus::Running) {
                return true;
            }

            leave_post(target);
            return false;
        }

        return false;
    }

    static void leave_post(std::uint16_t target) noexcept {
        if (current_thread_index_ < thread_count) {
            return;
        }

        AF_ASSERT(target < thread_count);
        if (active_external_posts_[target].value.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
            active_external_posts_[target].value.notify_all();
        }
    }

    static void wait_for_external_posts() noexcept {
        for (auto& counter : active_external_posts_) {
            for (;;) {
                const std::uint32_t count = counter.value.load(std::memory_order_acquire);
                if (count == 0) {
                    break;
                }
                counter.value.wait(count, std::memory_order_acquire);
            }
        }
    }

    static void reset_task_registry() noexcept {
        if constexpr (task_registry_enabled) {
            std::lock_guard<std::mutex> lock(task_registry_mutex_);
            task_registry_head_ = nullptr;
        }
    }

    static void register_task(Task* task) noexcept {
        if constexpr (task_registry_enabled) {
            std::lock_guard<std::mutex> lock(task_registry_mutex_);
            AF_ASSERT(!task->registry_.registered);
            task->registry_.prev = nullptr;
            task->registry_.next = task_registry_head_;
            if (task_registry_head_ != nullptr) {
                task_registry_head_->registry_.prev = task;
            }
            task_registry_head_ = task;
            task->registry_.registered = true;
        } else {
            static_cast<void>(task);
        }
    }

    static void unregister_task(Task* task) noexcept {
        if constexpr (task_registry_enabled) {
            std::lock_guard<std::mutex> lock(task_registry_mutex_);
            if (!task->registry_.registered) {
                AF_ASSERT(false && "task was not registered");
                return;
            }

            if (task->registry_.prev != nullptr) {
                task->registry_.prev->registry_.next = task->registry_.next;
            } else {
                task_registry_head_ = task->registry_.next;
            }
            if (task->registry_.next != nullptr) {
                task->registry_.next->registry_.prev = task->registry_.prev;
            }

            task->registry_.prev = nullptr;
            task->registry_.next = nullptr;
            task->registry_.registered = false;
        } else {
            static_cast<void>(task);
        }
    }

    static void cancel_registered_tasks() noexcept {
        if constexpr (task_registry_enabled) {
            Task* task = nullptr;
            {
                std::lock_guard<std::mutex> lock(task_registry_mutex_);
                task = task_registry_head_;
                task_registry_head_ = nullptr;
            }

            while (task != nullptr) {
                Task* next = task->registry_.next;
                task->registry_.prev = nullptr;
                task->registry_.next = nullptr;
                task->registry_.registered = false;
                cancel_registered_task(task);
                task = next;
            }
        }
    }

    static void cancel_registered_task(Task* task) noexcept {
        for (;;) {
            TaskState state = task->state_.load(std::memory_order_acquire);
            switch (state) {
            case TaskState::Pending:
            case TaskState::Queued: {
                TaskState expected = state;
                if (task->state_.compare_exchange_weak(
                        expected,
                        TaskState::Done,
                        std::memory_order_acq_rel,
                        std::memory_order_acquire)) {
                    static_cast<void>(task->take_requested_thread());
                    task->on_runtime_cancel();
                    task->release_lifetime_ref();
                    return;
                }
                break;
            }

            case TaskState::Created:
            case TaskState::Running:
                AF_ASSERT(false && "registered task cannot be cancelled in this state");
                return;

            case TaskState::Done:
                return;
            }
        }
    }

    static void on_task_started(Task* task) noexcept {
        register_task(task);
        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            unfinished_tasks_.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    static void on_task_finished(Task* task) noexcept {
        unregister_task(task);
        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            if (unfinished_tasks_.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
                unfinished_tasks_.notify_all();
            }
        }
    }

    alignas(detail::hardware_cache_line_size) static inline std::atomic<RuntimeStatus> status_{
        RuntimeStatus::Stopped};
    static inline std::array<ExternalPostCounter, thread_count> active_external_posts_{};
    alignas(detail::hardware_cache_line_size) static inline std::atomic<std::uint32_t> unfinished_tasks_{
        0};
    alignas(detail::hardware_cache_line_size) static inline std::atomic<std::uint64_t> generation_{
        0};
    static inline std::vector<std::unique_ptr<Executor>> executors_;
    static inline std::vector<std::unique_ptr<SpscQueue>> spsc_queues_;
    static inline std::vector<std::unique_ptr<ExternalQueue>> external_queues_;
    static inline std::vector<OrderedBatchState> ordered_batch_state_;
    static inline std::mutex task_registry_mutex_;
    static inline Task* task_registry_head_{nullptr};
    static inline thread_local std::uint16_t current_thread_index_ = invalid_thread_index;
};

} // namespace af
