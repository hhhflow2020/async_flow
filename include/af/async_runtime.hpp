#pragma once

#include <atomic>
#include <bit>
#include <cstdint>
#include <map>
#include <memory>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "af/detail/bounded_queues.hpp"
#include "af/detail/config.hpp"
#include "af/detail/object_pool.hpp"
#include "af/task.hpp"

namespace af {

enum class ParallelMode : std::uint8_t {
    NonEmptyOnly,
    AllShards,
};

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

        wait_for_active_posts();
        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            wait_for_idle();
        }

        for (auto& executor : executors_) {
            executor->request_stop();
        }
        for (auto& executor : executors_) {
            executor->join();
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
        if (task == nullptr || !try_enter_post()) {
            return false;
        }

        const std::uint16_t index = thread_index(thread);
        if (index >= thread_count) {
            AF_ASSERT(false && "invalid thread index");
            leave_post();
            return false;
        }

        const detail::ScheduleRequest request = task->request_schedule(index);
        if (request.action == detail::ScheduleAction::Enqueue) {
            const bool first_schedule = request.previous == TaskState::Created;
            if (first_schedule) {
                on_task_started();
            }
            const bool enqueued = enqueue_ready_by_policy(index, task);
            if (!enqueued) {
                if (first_schedule) {
                    on_task_finished();
                }
                task->cancel_schedule(request.previous);
            }
            leave_post();
            return enqueued;
        }
        const bool deferred = request.action == detail::ScheduleAction::Deferred;
        leave_post();
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

        for (auto& op : ops) {
            const auto key = static_cast<std::uint64_t>(key_fn(op));
            const std::uint16_t shard = static_cast<std::uint16_t>(key % shard_count);
            sharded.shards[shard].push_back(std::move(op));
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

        void complete(bool ok) noexcept {
            if (!ok) {
                failed.fetch_add(1, std::memory_order_relaxed);
            }
            if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
                if (owner != nullptr && resume_thread < thread_count) {
                    owner->set_last_parallel_failures(failed.load(std::memory_order_acquire));
                    post_blocking(thread_from_index(resume_thread), owner);
                }
                destroy_parallel_group(this);
            }
        }
    };

    template <typename StreamTag, typename ApplyTaskT, typename BatchT>
    struct OrderedStartState {
        std::uint64_t next_batch_id{1};
        std::uint64_t generation{0};
        std::map<std::uint64_t, BatchT> pending;

        void reset(std::uint64_t runtime_generation) {
            next_batch_id = 1;
            generation = runtime_generation;
            pending.clear();
        }

        void submit(BatchT batch) {
            const std::uint64_t batch_id = batch.batch_id;
            if (batch_id < next_batch_id) {
                return;
            }

            if (batch_id > next_batch_id) {
                pending.emplace(batch_id, std::move(batch));
                return;
            }

            start_ready(std::move(batch));
            drain_ready();
        }

        void start_ready(BatchT batch) {
            [[maybe_unused]] const bool ok = AsyncRuntime::start_task<ApplyTaskT>(std::move(batch));
            AF_ASSERT(ok);
            ++next_batch_id;
        }

        void drain_ready() {
            for (;;) {
                auto it = pending.find(next_batch_id);
                if (it == pending.end()) {
                    return;
                }

                BatchT batch = std::move(it->second);
                pending.erase(it);
                start_ready(std::move(batch));
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
            ordered_start_state<StreamTag, ApplyTaskT, BatchT>().submit(std::move(batch_));
            return this->done();
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
            std::vector<Op>&& ops,
            Handler handler)
            : Task(token),
              group_(group),
              shard_index_(shard_index),
              batch_id_(batch_id),
              ops_(std::move(ops)),
              handler_(std::move(handler)) {}

    private:
        TaskResult run() override {
            bool ok = true;
            if constexpr (Ordered) {
                ok = check_order_guard();
            }

            if (ok) {
                try {
                    if constexpr (Ordered) {
                        using HandlerResult =
                            std::invoke_result_t<Handler&, std::uint16_t, std::vector<Op>&, std::uint64_t>;
                        if constexpr (std::is_same_v<HandlerResult, bool>) {
                            ok = handler_(shard_index_, ops_, batch_id_);
                        } else {
                            handler_(shard_index_, ops_, batch_id_);
                        }
                    } else {
                        using HandlerResult =
                            std::invoke_result_t<Handler&, std::uint16_t, std::vector<Op>&>;
                        if constexpr (std::is_same_v<HandlerResult, bool>) {
                            ok = handler_(shard_index_, ops_);
                        } else {
                            handler_(shard_index_, ops_);
                        }
                    }
                } catch (...) {
                    AF_ASSERT(false && "parallel shard handler must not throw");
                    ok = false;
                }
            }

            if constexpr (Ordered) {
                if (ok) {
                    commit_order_guard();
                }
            }

            group_->complete(ok);
            return this->done();
        }

        bool check_order_guard() noexcept {
            const std::uint16_t thread = AsyncRuntime::current_thread_index();
            AF_ASSERT(thread < ordered_batch_state_.size());
            auto& state = ordered_batch_state_[thread];
            const bool ok = batch_id_ == state.last_applied_batch_id + 1U;
            AF_ASSERT(ok && "ordered batch id must be contiguous per shard");
            return ok;
        }

        void commit_order_guard() noexcept {
            const std::uint16_t thread = AsyncRuntime::current_thread_index();
            ordered_batch_state_[thread].last_applied_batch_id = batch_id_;
        }

        ParallelGroup* group_;
        std::uint16_t shard_index_;
        std::uint64_t batch_id_;
        std::vector<Op> ops_;
        Handler handler_;
    };

    template <typename Op, typename Handler>
    static void parallel_shards_impl(
        std::bool_constant<false>,
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        std::uint64_t batch_id,
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl_typed<Op, Handler, false>(
            shard_begin,
            sharded_ops,
            mode,
            batch_id,
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
        Task* owner,
        Handler&& handler) {
        parallel_shards_impl_typed<Op, Handler, true>(
            shard_begin,
            sharded_ops,
            mode,
            batch_id,
            owner,
            std::forward<Handler>(handler));
    }

    template <typename Op, typename Handler, bool Ordered>
    static void parallel_shards_impl_typed(
        Thread shard_begin,
        ShardedOps<Op>& sharded_ops,
        ParallelMode mode,
        std::uint64_t batch_id,
        Task* owner,
        Handler&& handler) {
        AF_ASSERT(owner != nullptr);
        AF_ASSERT(is_runtime_thread() && "parallel_shards must be called from a runtime thread");

        const std::uint16_t begin = thread_index(shard_begin);
        const std::uint16_t shard_count = sharded_ops.shard_count();
        AF_ASSERT(begin + shard_count <= thread_count);

        std::uint32_t target_count = 0;
        for (std::uint16_t i = 0; i < shard_count; ++i) {
            if (mode == ParallelMode::AllShards || !sharded_ops.shards[i].empty()) {
                ++target_count;
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

            Task* shard_task = nullptr;
            if constexpr (Ordered) {
                shard_task = allocate_task<ShardTask<Op, HandlerT, true>>(
                    group,
                    i,
                    batch_id,
                    std::move(sharded_ops.shards[i]),
                    HandlerT(handler));
            } else {
                shard_task = allocate_task<ShardTask<Op, HandlerT, false>>(
                    group,
                    i,
                    0,
                    std::move(sharded_ops.shards[i]),
                    HandlerT(handler));
            }

            const Thread thread = thread_from_index(static_cast<std::uint16_t>(begin + i));
            post_blocking(thread, shard_task);
        }
    }

    class alignas(detail::hardware_cache_line_size) Executor {
    public:
        explicit Executor(std::uint16_t index)
            : index_(index),
              local_queue_(detail::next_power_of_two(spsc_queue_capacity < 2 ? 2 : spsc_queue_capacity)) {}
        Executor(const Executor&) = delete;
        Executor& operator=(const Executor&) = delete;

        void start() {
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
            if (sleeping_.exchange(false, std::memory_order_acq_rel)) {
                notify_force();
            }
        }

        void mark_ready(std::uint16_t source) noexcept {
            if constexpr (thread_count <= 64U) {
                ready_sources_.fetch_or(1ULL << source, std::memory_order_release);
            } else {
                static_cast<void>(source);
            }
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
        void notify_force() noexcept {
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

                const std::uint32_t observed = wake_epoch_.load(std::memory_order_acquire);
                sleeping_.store(true, std::memory_order_release);
                if (Task* task = pop_one()) {
                    sleeping_.store(false, std::memory_order_relaxed);
                    execute(task);
                } else {
                    wake_epoch_.wait(observed, std::memory_order_acquire);
                }
            }

            current_thread_index_ = invalid_thread_index;
        }

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
                        ready_sources_.fetch_or(bit, std::memory_order_release);
                        return task;
                    }

                    ready_sources_.fetch_and(~bit, std::memory_order_acq_rel);
                    if (Task* task = spsc_queue(source, index_).try_pop()) {
                        ready_sources_.fetch_or(bit, std::memory_order_release);
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

            if (Task* task = external_queues_[index_]->try_pop()) {
                return task;
            }

            return nullptr;
        }

        void finish_done(Task* task) noexcept {
            const std::uint16_t requested = task->take_requested_thread();
            AF_ASSERT(requested == invalid_thread_index);
            task->state_.store(TaskState::Done, std::memory_order_release);
            on_task_finished();
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
        std::uint16_t next_source_{0};
        std::vector<Task*> local_queue_;
        std::size_t local_head_{0};
        std::size_t local_tail_{0};
        std::size_t local_size_{0};
        alignas(detail::hardware_cache_line_size) std::atomic<std::uint64_t> ready_sources_{0};
        alignas(detail::hardware_cache_line_size) std::atomic<std::uint32_t> wake_epoch_{0};
        std::atomic<bool> sleeping_{false};
        std::atomic<bool> stop_requested_{false};
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
                on_task_started();
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
            executors_[index]->notify();
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
        executors_[index]->notify();
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

    [[nodiscard]] static bool try_enter_post() noexcept {
        const RuntimeStatus status = status_.load(std::memory_order_acquire);
        if (status == RuntimeStatus::Running) {
            active_posts_.fetch_add(1, std::memory_order_acq_rel);
            if (status_.load(std::memory_order_acquire) == RuntimeStatus::Running) {
                return true;
            }

            if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
                if (current_thread_index_ < thread_count &&
                    status_.load(std::memory_order_acquire) == RuntimeStatus::Stopping) {
                    return true;
                }
            }

            leave_post();
            return false;
        }

        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            if (status == RuntimeStatus::Stopping && current_thread_index_ < thread_count) {
                active_posts_.fetch_add(1, std::memory_order_acq_rel);
                if (status_.load(std::memory_order_acquire) == RuntimeStatus::Stopping) {
                    return true;
                }
                leave_post();
            }
        }

        return false;
    }

    static void leave_post() noexcept {
        if (active_posts_.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
            active_posts_.notify_all();
        }
    }

    static void wait_for_active_posts() noexcept {
        for (;;) {
            const std::uint32_t count = active_posts_.load(std::memory_order_acquire);
            if (count == 0) {
                return;
            }
            active_posts_.wait(count, std::memory_order_acquire);
        }
    }

    static void on_task_started() noexcept {
        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            unfinished_tasks_.fetch_add(1, std::memory_order_acq_rel);
        }
    }

    static void on_task_finished() noexcept {
        if constexpr (shutdown_policy == ShutdownPolicy::WaitForTasks) {
            if (unfinished_tasks_.fetch_sub(1, std::memory_order_acq_rel) == 1U) {
                unfinished_tasks_.notify_all();
            }
        }
    }

    alignas(detail::hardware_cache_line_size) static inline std::atomic<RuntimeStatus> status_{
        RuntimeStatus::Stopped};
    alignas(detail::hardware_cache_line_size) static inline std::atomic<std::uint32_t> active_posts_{
        0};
    alignas(detail::hardware_cache_line_size) static inline std::atomic<std::uint32_t> unfinished_tasks_{
        0};
    alignas(detail::hardware_cache_line_size) static inline std::atomic<std::uint64_t> generation_{
        0};
    static inline std::vector<std::unique_ptr<Executor>> executors_;
    static inline std::vector<std::unique_ptr<SpscQueue>> spsc_queues_;
    static inline std::vector<std::unique_ptr<ExternalQueue>> external_queues_;
    static inline std::vector<OrderedBatchState> ordered_batch_state_;
    static inline thread_local std::uint16_t current_thread_index_ = invalid_thread_index;
};

} // namespace af
