#include <array>
#include <chrono>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <variant>

#include <unistd.h>

#include <gtest/gtest.h>

#include "af/async_runtime.hpp"
#include "af/platform.hpp"
#include "af/runtime.hpp"
#include "af/runtime_config.hpp"

namespace {

struct ConfigThreadTag;
struct ConfigLogicThreadTag;
struct ConfigIoThreadTag;
struct ConfigLogThreadTag;

struct AboveSixtyFourThreadTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<ConfigThreadTag, 257>());
};

using AboveSixtyFourRuntime = af::AsyncRuntime<AboveSixtyFourThreadTraits>;

static_assert(AboveSixtyFourRuntime::thread_count == 257U);
static_assert(AboveSixtyFourRuntime::invalid_thread_index == 257U);
static_assert(AboveSixtyFourRuntime::Config::task_pool_remote_release_batch_size == 64U);
static_assert(AboveSixtyFourRuntime::Config::task_pool_chunk_size == 256U);
static_assert(!AboveSixtyFourRuntime::Config::task_pool_cache_slot_index);
static_assert(AboveSixtyFourRuntime::Config::task_pool_local_cache_set_size == 1U);
static_assert(AboveSixtyFourRuntime::Config::task_pool_direct_release_set_size == 4U);
static_assert(AboveSixtyFourRuntime::Config::task_pool_local_cache_capacity == 64U);
static_assert(AboveSixtyFourRuntime::Config::timer_drain_budget == 256U);
static_assert(AboveSixtyFourRuntime::Config::timer_reserve == 1024U);
static_assert(AboveSixtyFourRuntime::Config::service_task_budget == 32U);

struct ThreadLayoutMetadataTraits {
    static constexpr auto threads =
        af::thread_layout(af::thread_group<ConfigLogicThreadTag, 3, af::thread_kind::cpu>("logic"),
                          af::thread_group<ConfigIoThreadTag, 2, af::thread_kind::io>("io"),
                          af::thread_group<ConfigLogThreadTag, 1, af::thread_kind::cpu>("log"));
};

using ThreadLayoutMetadataRuntime = af::AsyncRuntime<ThreadLayoutMetadataTraits>;
using ConfigLogicGroup =
    decltype(ThreadLayoutMetadataRuntime::thread_group<ConfigLogicThreadTag>());

static_assert(ThreadLayoutMetadataRuntime::thread_count == 6U);
static_assert(sizeof(ThreadLayoutMetadataRuntime::Thread) == sizeof(std::uint16_t));
static_assert(std::is_empty_v<ConfigLogicGroup>);

struct RemoteBatchOverrideTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<ConfigThreadTag, 1>());
    static constexpr std::size_t task_pool_remote_release_batch_size = 32;
    static constexpr std::size_t task_pool_chunk_size = 512;
    static constexpr bool task_pool_cache_slot_index = true;
    static constexpr std::size_t task_pool_local_cache_set_size = 16;
    static constexpr std::size_t task_pool_direct_release_set_size = 8;
    static constexpr std::size_t task_pool_local_cache_capacity = 128;
    static constexpr std::size_t timer_drain_budget = 64;
    static constexpr std::size_t timer_reserve = 2048;
    static constexpr std::size_t service_task_budget = 8;
};

using RemoteBatchOverrideRuntime = af::AsyncRuntime<RemoteBatchOverrideTraits>;

static_assert(RemoteBatchOverrideRuntime::Config::task_pool_remote_release_batch_size == 32U);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_chunk_size == 512U);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_cache_slot_index);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_local_cache_set_size == 16U);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_direct_release_set_size == 8U);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_local_cache_capacity == 128U);
static_assert(RemoteBatchOverrideRuntime::Config::timer_drain_budget == 64U);
static_assert(RemoteBatchOverrideRuntime::Config::timer_reserve == 2048U);
static_assert(RemoteBatchOverrideRuntime::Config::service_task_budget == 8U);
static_assert(af::supports_native_io_wait == (af::supports_epoll || af::supports_kqueue));
static_assert(af::supports_eventfd == af::platform_linux);
static_assert(af::supports_timerfd == af::platform_linux);
static_assert(af::supports_openat2 == af::platform_linux);
static_assert(af::supports_sendfile == af::platform_linux);
static_assert(af::supports_splice == af::platform_linux);
static_assert(af::supports_zero_copy_send == af::platform_linux);
static_assert(af::platform_posix != af::platform_windows);
static_assert(std::is_same_v<af::task_result, af::TaskResult>);
static_assert(std::is_same_v<af::schedule_mode, af::ScheduleMode>);
static_assert(std::is_same_v<af::shutdown_policy, af::ShutdownPolicy>);
static_assert(std::is_same_v<af::task_state, af::TaskState>);
static_assert(std::is_same_v<af::parallel_mode, af::ParallelMode>);
static_assert(std::is_same_v<af::ordered_batch_replay_policy, af::OrderedBatchReplayPolicy>);
static_assert(std::is_same_v<af::ordered_batch_options, af::OrderedBatchOptions>);
static_assert(std::is_same_v<af::sharded_ops<int>, af::ShardedOps<int>>);
static_assert(af::task_result::done == af::TaskResult::Done);
static_assert(af::task_result::pending == af::TaskResult::Pending);
static_assert(af::task_result::again == af::TaskResult::Again);
static_assert(af::task_result::failed == af::TaskResult::Failed);
static_assert(af::task_result::cancelled == af::TaskResult::Cancelled);
static_assert(af::schedule_mode::auto_select == af::ScheduleMode::Auto);
static_assert(af::schedule_mode::fast == af::ScheduleMode::Fast);
static_assert(af::schedule_mode::ordered == af::ScheduleMode::Ordered);
static_assert(af::shutdown_policy::wait_for_tasks == af::ShutdownPolicy::WaitForTasks);
static_assert(af::shutdown_policy::stop_immediately == af::ShutdownPolicy::StopImmediately);
static_assert(af::task_state::created == af::TaskState::Created);
static_assert(af::task_state::queued == af::TaskState::Queued);
static_assert(af::task_state::timer_arming == af::TaskState::TimerArming);
static_assert(af::task_state::timer_pending == af::TaskState::TimerPending);
static_assert(af::task_state::starting == af::TaskState::Starting);
static_assert(af::task_state::running == af::TaskState::Running);
static_assert(af::task_state::pending == af::TaskState::Pending);
static_assert(af::task_state::done == af::TaskState::Done);
static_assert(af::parallel_mode::non_empty_only == af::ParallelMode::NonEmptyOnly);
static_assert(af::parallel_mode::all_shards == af::ParallelMode::AllShards);
static_assert(af::ordered_batch_replay_policy::strict == af::OrderedBatchReplayPolicy::Strict);
static_assert(af::ordered_batch_replay_policy::skip_already_applied ==
              af::OrderedBatchReplayPolicy::SkipAlreadyApplied);
static_assert(std::is_same_v<af::log_ordering, af::LogOrdering>);
static_assert(std::is_same_v<af::log_overflow_policy, af::LogOverflowPolicy>);
static_assert(af::log_ordering::ordered == af::LogOrdering::Ordered);
static_assert(af::log_ordering::relaxed == af::LogOrdering::Relaxed);
static_assert(af::log_overflow_policy::drop_newest == af::LogOverflowPolicy::DropNewest);
static_assert(af::log_overflow_policy::block == af::LogOverflowPolicy::Block);

[[nodiscard]] bool wait_for_active_threads(af::runtime &runtime, std::uint16_t expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (runtime.active_thread_count() == expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return runtime.active_thread_count() == expected;
}

[[nodiscard]] bool wait_for_counter(std::atomic<int> &counter, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (counter.load(std::memory_order_acquire) == expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return counter.load(std::memory_order_acquire) == expected;
}

[[nodiscard]] bool wait_for_counter_at_least(std::atomic<int> &counter, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (counter.load(std::memory_order_acquire) >= expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return counter.load(std::memory_order_acquire) >= expected;
}

class PostedRuntimeWork final : public af::runtime_work {
public:
    PostedRuntimeWork(std::atomic<int> &counter, std::atomic<std::uint16_t> &thread,
                      std::atomic<bool> &owner_matches)
        : counter_(counter), thread_(thread), owner_matches_(owner_matches) {}

    void run(af::runtime &owner) noexcept override {
        thread_.store(af::runtime::current_thread_index(), std::memory_order_release);
        owner_matches_.store(af::runtime::current() == &owner, std::memory_order_release);
        counter_.fetch_add(1, std::memory_order_release);
    }

private:
    std::atomic<int> &counter_;
    std::atomic<std::uint16_t> &thread_;
    std::atomic<bool> &owner_matches_;
};

void expect_idle_wait_strategy_accepts_delayed_post(af::idle_wait_strategy strategy) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.scheduler.idle_wait = strategy;
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<std::uint16_t> observed_thread{af::runtime_invalid_thread_index};
    std::atomic<bool> owner_matches{false};
    PostedRuntimeWork work(counter, observed_thread, owner_matches);

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    ASSERT_TRUE(runtime.post(cpu_thread, &work));
    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_EQ(observed_thread.load(std::memory_order_acquire), cpu_thread);
    EXPECT_TRUE(owner_matches.load(std::memory_order_acquire));

    runtime.stop();
}

class StopRuntimeWork final : public af::runtime_work {
public:
    explicit StopRuntimeWork(std::atomic<int> &counter) : counter_(counter) {}

    void run(af::runtime &owner) noexcept override {
        owner.stop();
        counter_.fetch_add(1, std::memory_order_release);
    }

private:
    std::atomic<int> &counter_;
};

class CountingRuntimeService final : public af::detail::RuntimeServiceTask {
public:
    CountingRuntimeService(std::atomic<int> &counter, std::atomic<std::uint16_t> &observed_thread,
                           std::atomic<std::size_t> &observed_budget)
        : counter_(counter), observed_thread_(observed_thread), observed_budget_(observed_budget) {}

    void mark_pending() noexcept {
        pending_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool run_service(std::size_t budget) noexcept override {
        observed_budget_.store(budget, std::memory_order_release);
        if (!pending_.exchange(false, std::memory_order_acq_rel)) {
            return false;
        }
        observed_thread_.store(af::runtime::current_thread_index(), std::memory_order_release);
        counter_.fetch_add(1, std::memory_order_release);
        return true;
    }

private:
    std::atomic<int> &counter_;
    std::atomic<std::uint16_t> &observed_thread_;
    std::atomic<std::size_t> &observed_budget_;
    std::atomic<bool> pending_{false};
};

class ServiceTaskControlWork final : public af::runtime_work {
public:
    enum class action {
        register_service,
        unregister_service,
    };

    ServiceTaskControlWork(action op, std::uint16_t thread, af::detail::RuntimeServiceTask &service,
                           std::atomic<int> &counter, std::atomic<bool> &ok)
        : op_(op), thread_(thread), service_(service), counter_(counter), ok_(ok) {}

    void run(af::runtime &owner) noexcept override {
        const bool result = op_ == action::register_service
                                ? owner.register_service_task(thread_, &service_)
                                : owner.unregister_service_task(thread_, &service_);
        ok_.store(result, std::memory_order_release);
        counter_.fetch_add(1, std::memory_order_release);
    }

private:
    action op_;
    std::uint16_t thread_;
    af::detail::RuntimeServiceTask &service_;
    std::atomic<int> &counter_;
    std::atomic<bool> &ok_;
};

class RepostingRuntimeWork final : public af::runtime_work {
public:
    RepostingRuntimeWork(std::uint16_t thread, int limit, std::atomic<int> &run_counter,
                         std::atomic<bool> *first_run_gate = nullptr)
        : thread_(thread), limit_(limit), run_counter_(run_counter),
          first_run_gate_(first_run_gate) {}

    void run(af::runtime &owner) noexcept override {
        const int count = run_counter_.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (count < limit_) {
            static_cast<void>(owner.post(thread_, this));
        }
        if (count == 1 && first_run_gate_ != nullptr) {
            while (!first_run_gate_->load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        }
    }

private:
    std::uint16_t thread_;
    int limit_;
    std::atomic<int> &run_counter_;
    std::atomic<bool> *first_run_gate_{nullptr};
};

class TaskBudgetProbeService final : public af::detail::RuntimeServiceTask {
public:
    TaskBudgetProbeService(std::atomic<int> &service_counter, std::atomic<int> &task_counter,
                           std::atomic<int> &observed_task_count)
        : service_counter_(service_counter), task_counter_(task_counter),
          observed_task_count_(observed_task_count) {}

    void mark_pending() noexcept {
        pending_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool run_service(std::size_t budget) noexcept override {
        static_cast<void>(budget);
        if (!pending_.exchange(false, std::memory_order_acq_rel)) {
            return false;
        }
        observed_task_count_.store(task_counter_.load(std::memory_order_acquire),
                                   std::memory_order_release);
        service_counter_.fetch_add(1, std::memory_order_release);
        return true;
    }

private:
    std::atomic<int> &service_counter_;
    std::atomic<int> &task_counter_;
    std::atomic<int> &observed_task_count_;
    std::atomic<bool> pending_{false};
};

class TimerBudgetProbeService final : public af::detail::RuntimeServiceTask {
public:
    TimerBudgetProbeService(std::atomic<int> &service_counter, std::atomic<int> &timer_counter,
                            std::atomic<int> &observed_timer_count)
        : service_counter_(service_counter), timer_counter_(timer_counter),
          observed_timer_count_(observed_timer_count) {}

    void mark_pending() noexcept {
        pending_.store(true, std::memory_order_release);
    }

    [[nodiscard]] bool run_service(std::size_t budget) noexcept override {
        static_cast<void>(budget);
        if (!pending_.exchange(false, std::memory_order_acq_rel)) {
            return false;
        }
        const int previous = service_counter_.fetch_add(1, std::memory_order_acq_rel);
        if (previous == 0) {
            observed_timer_count_.store(timer_counter_.load(std::memory_order_acquire),
                                        std::memory_order_release);
        }
        return true;
    }

private:
    std::atomic<int> &service_counter_;
    std::atomic<int> &timer_counter_;
    std::atomic<int> &observed_timer_count_;
    std::atomic<bool> pending_{false};
};

class InstanceRuntimeTask final : public af::runtime_task {
public:
    InstanceRuntimeTask(af::runtime_task::factory_token token, af::runtime &owner,
                        std::atomic<int> &counter,
                        std::atomic<af::runtime_task_id> &observed_task_id,
                        std::atomic<af::runtime_task_id> &observed_current_task_id,
                        std::atomic<std::uint16_t> &observed_thread)
        : af::runtime_task(token, owner), counter_(counter), observed_task_id_(observed_task_id),
          observed_current_task_id_(observed_current_task_id), observed_thread_(observed_thread) {}

    [[nodiscard]] bool do_it(std::uint16_t thread) noexcept {
        return schedule_to(thread);
    }

private:
    af::task_result run_task() noexcept override {
        observed_task_id_.store(task_id(), std::memory_order_release);
        observed_current_task_id_.store(af::runtime::current_task_id(), std::memory_order_release);
        observed_thread_.store(af::runtime::current_thread_index(), std::memory_order_release);
        counter_.fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> &counter_;
    std::atomic<af::runtime_task_id> &observed_task_id_;
    std::atomic<af::runtime_task_id> &observed_current_task_id_;
    std::atomic<std::uint16_t> &observed_thread_;
};

class TimerBudgetRuntimeTask final : public af::runtime_task {
public:
    TimerBudgetRuntimeTask(af::runtime_task::factory_token token, af::runtime &owner,
                           std::atomic<int> &counter, TimerBudgetProbeService &service)
        : af::runtime_task(token, owner), counter_(counter), service_(service) {}

    [[nodiscard]] bool do_it(std::uint16_t thread) noexcept {
        return schedule_after(thread, std::chrono::nanoseconds(0));
    }

private:
    af::task_result run_task() noexcept override {
        counter_.fetch_add(1, std::memory_order_acq_rel);
        service_.mark_pending();
        return done();
    }

    std::atomic<int> &counter_;
    TimerBudgetProbeService &service_;
};

class TwoHopRuntimeTask final : public af::runtime_task {
public:
    TwoHopRuntimeTask(af::runtime_task::factory_token token, af::runtime &owner,
                      std::uint16_t second_thread, std::atomic<int> &counter,
                      std::atomic<std::uint16_t> &first_thread,
                      std::atomic<std::uint16_t> &second_observed_thread,
                      std::atomic<bool> &next_schedule_ok)
        : af::runtime_task(token, owner), second_thread_(second_thread), counter_(counter),
          first_thread_(first_thread), second_observed_thread_(second_observed_thread),
          next_schedule_ok_(next_schedule_ok) {}

    [[nodiscard]] bool do_it(std::uint16_t first_thread) noexcept {
        return schedule_to(first_thread);
    }

private:
    af::task_result run_task() noexcept override {
        const int phase = phase_.fetch_add(1, std::memory_order_acq_rel);
        if (phase == 0) {
            first_thread_.store(af::runtime::current_thread_index(), std::memory_order_release);
            next_schedule_ok_.store(schedule_to(second_thread_), std::memory_order_release);
            return pending();
        }

        second_observed_thread_.store(af::runtime::current_thread_index(),
                                      std::memory_order_release);
        counter_.fetch_add(1, std::memory_order_release);
        return done();
    }

    std::uint16_t second_thread_;
    std::atomic<int> &counter_;
    std::atomic<std::uint16_t> &first_thread_;
    std::atomic<std::uint16_t> &second_observed_thread_;
    std::atomic<bool> &next_schedule_ok_;
    std::atomic<int> phase_{0};
};

class DelayedRuntimeTask final : public af::runtime_task {
public:
    DelayedRuntimeTask(af::runtime_task::factory_token token, af::runtime &owner,
                       std::atomic<int> &counter, std::atomic<std::uint16_t> &observed_thread,
                       std::atomic<long long> &elapsed_ns)
        : af::runtime_task(token, owner), counter_(counter), observed_thread_(observed_thread),
          elapsed_ns_(elapsed_ns) {}

    template <typename Rep, typename Period>
    [[nodiscard]] bool do_it(std::uint16_t thread,
                             std::chrono::duration<Rep, Period> delay) noexcept {
        start_ = std::chrono::steady_clock::now();
        return schedule_after(thread, delay);
    }

private:
    af::task_result run_task() noexcept override {
        const auto elapsed = std::chrono::steady_clock::now() - start_;
        elapsed_ns_.store(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count(),
                          std::memory_order_release);
        observed_thread_.store(af::runtime::current_thread_index(), std::memory_order_release);
        counter_.fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> &counter_;
    std::atomic<std::uint16_t> &observed_thread_;
    std::atomic<long long> &elapsed_ns_;
    std::chrono::steady_clock::time_point start_{};
};

void expect_idle_wait_strategy_runs_delayed_task(af::idle_wait_strategy strategy) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.scheduler.idle_wait = strategy;
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<std::uint16_t> observed_thread{af::runtime_invalid_thread_index};
    std::atomic<long long> elapsed_ns{0};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    auto task = af::make_task<DelayedRuntimeTask>(runtime, counter, observed_thread, elapsed_ns);
    ASSERT_TRUE(task->do_it(cpu_thread, std::chrono::milliseconds(5)));
    task.reset();

    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_EQ(observed_thread.load(std::memory_order_acquire), cpu_thread);
    EXPECT_GE(elapsed_ns.load(std::memory_order_acquire), 1'000'000LL);

    runtime.stop();
}

class DelayedTwoHopRuntimeTask final : public af::runtime_task {
public:
    DelayedTwoHopRuntimeTask(af::runtime_task::factory_token token, af::runtime &owner,
                             std::uint16_t second_thread, std::atomic<int> &counter,
                             std::atomic<std::uint16_t> &first_thread,
                             std::atomic<std::uint16_t> &second_observed_thread,
                             std::atomic<bool> &next_schedule_ok)
        : af::runtime_task(token, owner), second_thread_(second_thread), counter_(counter),
          first_thread_(first_thread), second_observed_thread_(second_observed_thread),
          next_schedule_ok_(next_schedule_ok) {}

    [[nodiscard]] bool do_it(std::uint16_t first_thread) noexcept {
        return schedule_to(first_thread);
    }

private:
    af::task_result run_task() noexcept override {
        const int phase = phase_.fetch_add(1, std::memory_order_acq_rel);
        if (phase == 0) {
            first_thread_.store(af::runtime::current_thread_index(), std::memory_order_release);
            next_schedule_ok_.store(schedule_after(second_thread_, std::chrono::milliseconds(10)),
                                    std::memory_order_release);
            return pending();
        }

        second_observed_thread_.store(af::runtime::current_thread_index(),
                                      std::memory_order_release);
        counter_.fetch_add(1, std::memory_order_release);
        return done();
    }

    std::uint16_t second_thread_;
    std::atomic<int> &counter_;
    std::atomic<std::uint16_t> &first_thread_;
    std::atomic<std::uint16_t> &second_observed_thread_;
    std::atomic<bool> &next_schedule_ok_;
    std::atomic<int> phase_{0};
};

class CancelledDelayedRuntimeTask final : public af::runtime_task {
public:
    CancelledDelayedRuntimeTask(af::runtime_task::factory_token token, af::runtime &owner,
                                std::atomic<int> &destroyed)
        : af::runtime_task(token, owner), destroyed_(destroyed) {}

    ~CancelledDelayedRuntimeTask() override {
        destroyed_.fetch_add(1, std::memory_order_release);
    }

    [[nodiscard]] bool do_it(std::uint16_t thread) noexcept {
        return schedule_after(thread, std::chrono::hours(1));
    }

private:
    af::task_result run_task() noexcept override {
        return done();
    }

    std::atomic<int> &destroyed_;
};

class ThrowingRuntimeTask final : public af::runtime_task {
public:
    ThrowingRuntimeTask(af::runtime_task::factory_token token, af::runtime &owner,
                        bool throw_in_constructor)
        : af::runtime_task(token, owner) {
        if (throw_in_constructor) {
            throw std::runtime_error("constructor failed");
        }
    }

private:
    af::task_result run_task() noexcept override {
        return done();
    }
};

class PoolOnlyRuntimeTask final : public af::runtime_task {
public:
    PoolOnlyRuntimeTask(af::runtime_task::factory_token token, af::runtime &owner)
        : af::runtime_task(token, owner) {}

private:
    af::task_result run_task() noexcept override {
        return done();
    }
};

struct ReactorReadinessState {
    int read_fd{-1};
    int write_fd{-1};
    af::fd_event_source source;
    std::atomic<int> &counter;
    std::atomic<std::uint16_t> &thread;
    std::atomic<std::uint32_t> &events;
};

class ReactorReadinessTask final : public af::runtime_task {
public:
    ReactorReadinessTask(af::runtime_task::factory_token token, af::runtime &owner,
                         ReactorReadinessState &state)
        : af::runtime_task(token, owner), state_(state) {}

    [[nodiscard]] bool do_it(std::uint16_t thread) noexcept {
        return schedule_to(thread);
    }

private:
    static void on_event(void *owner, af::fd_event_source &source, std::uint32_t events) noexcept {
        auto &state = *static_cast<ReactorReadinessState *>(owner);
        std::array<char, 16> buffer{};
        static_cast<void>(::read(state.read_fd, buffer.data(), buffer.size()));
        if (af::reactor *reactor = af::runtime::current_reactor()) {
            static_cast<void>(reactor->del(&source));
        }
        state.thread.store(af::runtime::current_thread_index(), std::memory_order_release);
        state.events.store(events, std::memory_order_release);
        state.counter.fetch_add(1, std::memory_order_release);
    }

    af::task_result run_task() noexcept override {
        af::reactor *reactor = af::runtime::current_reactor();
        if (reactor == nullptr) {
            return failed();
        }

        state_.source.fd = state_.read_fd;
        state_.source.interests = af::reactor_readable;
        state_.source.owner = &state_;
        state_.source.on_event = &ReactorReadinessTask::on_event;
        if (!reactor->add(&state_.source)) {
            return failed();
        }

        const char value = 'x';
        if (::write(state_.write_fd, &value, sizeof(value)) != sizeof(value)) {
            static_cast<void>(reactor->del(&state_.source));
            return failed();
        }
        return done();
    }

    ReactorReadinessState &state_;
};

} // namespace

TEST(RuntimeConfigTests, PreservesThreadCountsAboveSixtyFour) {
    EXPECT_EQ(AboveSixtyFourRuntime::thread_count, 257U);
    EXPECT_EQ(AboveSixtyFourRuntime::invalid_thread_index, 257U);
}

TEST(RuntimeConfigTests, ThreadLayoutGroupsCarryKindNameAndIndexMetadata) {
    constexpr auto logic = ThreadLayoutMetadataRuntime::thread_group<ConfigLogicThreadTag>();
    constexpr auto io = ThreadLayoutMetadataRuntime::thread_group<ConfigIoThreadTag>();
    constexpr auto log = ThreadLayoutMetadataRuntime::thread_group<ConfigLogThreadTag>();

    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_index(logic.begin()), 0U);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_index(logic.template at<2>()), 2U);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_index(io.begin()), 3U);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_index(io.shard(5U)), 4U);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_index(log.begin()), 5U);
    EXPECT_TRUE(logic.contains(logic.template at<1>()));
    EXPECT_FALSE(logic.contains(io.template at<0>()));
    EXPECT_FALSE(log.contains(io.template at<0>()));
    EXPECT_EQ(logic.offset_of(logic.template at<2>()), 2U);

    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_kind(logic.template at<0>()),
              af::thread_kind::cpu);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_kind(io.template at<0>()), af::thread_kind::io);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_kind(log.template at<0>()), af::thread_kind::cpu);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_name(logic.template at<0>()), "logic");
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_name(io.template at<1>()), "io");
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_name(log.template at<0>()), "log");
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_group_offset(io.template at<1>()), 1U);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_group_offset(log.template at<0>()), 0U);
}

TEST(RuntimeConfigTests, DefaultsAndOverridesTaskPoolRemoteReleaseBatchSize) {
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_remote_release_batch_size, 64U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_chunk_size, 256U);
    EXPECT_FALSE(AboveSixtyFourRuntime::Config::task_pool_cache_slot_index);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_local_cache_set_size, 1U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_direct_release_set_size, 4U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_local_cache_capacity, 64U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::timer_drain_budget, 256U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::timer_reserve, 1024U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::service_task_budget, 32U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_remote_release_batch_size, 32U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_chunk_size, 512U);
    EXPECT_TRUE(RemoteBatchOverrideRuntime::Config::task_pool_cache_slot_index);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_local_cache_set_size, 16U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_direct_release_set_size, 8U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_local_cache_capacity, 128U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::timer_drain_budget, 64U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::timer_reserve, 2048U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::service_task_budget, 8U);
}

TEST(RuntimeConfigTests, RuntimeConfigUsesPlainStructDefaultsAndFactories) {
    af::runtime_config config;

    EXPECT_TRUE(config.threads.empty());
    EXPECT_EQ(config.scheduler.task_drain_budget, 256U);
    EXPECT_EQ(config.scheduler.service_task_budget, 32U);
    EXPECT_EQ(config.scheduler.max_task_run_slice.count(), 0);
    EXPECT_EQ(config.scheduler.idle_wait, af::idle_wait_strategy::futex);
    EXPECT_EQ(config.scheduler.wake, af::wake_policy::empty_to_non_empty);
    EXPECT_EQ(config.task_pool.local_cache_size, 256U);
    EXPECT_EQ(config.task_pool.slab_object_count, 4096U);
    EXPECT_EQ(config.task_pool.oom, af::oom_policy::fatal);
    EXPECT_EQ(config.timer.kind, af::timer_kind::min_heap);
    EXPECT_EQ(config.timer.tick, std::chrono::milliseconds(1));
    EXPECT_EQ(config.timer.drain_budget, 256U);
    EXPECT_EQ(config.reactor.backend, af::reactor_backend::auto_select);
    EXPECT_EQ(config.reactor.event_capacity, 1024U);
    EXPECT_EQ(config.logger.ordering, af::log_ordering::ordered);
    EXPECT_EQ(config.logger.consumer_thread.kind, af::thread_selector_kind::cpu);
    EXPECT_EQ(config.logger.consumer_thread.index, 0U);
    EXPECT_EQ(config.logger.overflow, af::log_overflow_policy::drop_newest);
    EXPECT_EQ(config.shutdown.drain_timeout, std::chrono::seconds(5));
    EXPECT_TRUE(config.diagnostics.enable_task_id);

    config.threads = {
        af::io_threads("io", 2),
        af::cpu_threads("logic", 4),
    };
    ASSERT_EQ(config.threads.size(), 2U);
    EXPECT_EQ(config.threads[0].name, "io");
    EXPECT_EQ(config.threads[0].kind, af::thread_kind::io);
    EXPECT_EQ(config.threads[0].count, 2U);
    EXPECT_TRUE(config.threads[0].set_os_thread_name);
    EXPECT_EQ(config.threads[1].name, "logic");
    EXPECT_EQ(config.threads[1].kind, af::thread_kind::cpu);
    EXPECT_EQ(config.threads[1].count, 4U);

    config.logger = af::log_config::relaxed();
    EXPECT_EQ(config.logger.ordering, af::log_ordering::relaxed);
    config.logger = af::log_config::ordered();
    config.logger.consumer_thread = af::thread_selector::io(1);
    config.logger.backends = {
        af::file_log_backend_config{"server.log"},
        af::udp_log_backend_config{"127.0.0.1", 9000},
        af::tcp_log_backend_config{"127.0.0.1", 9001},
    };
    ASSERT_EQ(config.logger.backends.size(), 3U);
    ASSERT_TRUE(std::holds_alternative<af::file_log_backend_config>(config.logger.backends[0]));
    ASSERT_TRUE(std::holds_alternative<af::udp_log_backend_config>(config.logger.backends[1]));
    ASSERT_TRUE(std::holds_alternative<af::tcp_log_backend_config>(config.logger.backends[2]));
    EXPECT_EQ(std::get<af::file_log_backend_config>(config.logger.backends[0]).path, "server.log");
    EXPECT_EQ(std::get<af::udp_log_backend_config>(config.logger.backends[1]).port, 9000U);
    EXPECT_EQ(std::get<af::tcp_log_backend_config>(config.logger.backends[2]).port, 9001U);
    EXPECT_EQ(config.logger.consumer_thread.kind, af::thread_selector_kind::io);
    EXPECT_EQ(config.logger.consumer_thread.index, 1U);
}

TEST(RuntimeConfigTests, ResolvesRuntimeConfigThreadMetadataAndSelectors) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 2),
        af::cpu_threads("logic", 3),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(2);
    config.logger.backends = {
        af::file_log_backend_config{"server.log"},
        af::udp_log_backend_config{"127.0.0.1", 9000},
    };

    auto resolution = af::resolve_runtime_config(config);
    ASSERT_TRUE(resolution);
    const auto &resolved = resolution.resolved;
    EXPECT_EQ(resolved.thread_count(), 5U);
    EXPECT_EQ(resolved.invalid_thread_index(), 5U);
    EXPECT_EQ(resolved.io_threads.size(), 2U);
    EXPECT_EQ(resolved.cpu_threads.size(), 3U);
    EXPECT_EQ(resolved.select_thread(af::thread_selector::any_io()), 0U);
    EXPECT_EQ(resolved.select_thread(af::thread_selector::io(1)), 1U);
    EXPECT_EQ(resolved.select_thread(af::thread_selector::any_cpu()), 2U);
    EXPECT_EQ(resolved.select_thread(af::thread_selector::cpu(2)), 4U);
    EXPECT_EQ(resolved.select_thread(af::thread_selector::thread(3)), 3U);
    EXPECT_EQ(resolved.select_thread(af::thread_selector::io(2)), resolved.invalid_thread_index());
    EXPECT_EQ(resolved.thread_name(0), "io");
    EXPECT_EQ(resolved.thread_name(3), "logic");
    EXPECT_EQ(resolved.thread_name(resolved.invalid_thread_index()), "invalid");
    EXPECT_EQ(resolved.thread_kind_of(0), af::thread_kind::io);
    EXPECT_EQ(resolved.thread_kind_of(4), af::thread_kind::cpu);
    EXPECT_EQ(resolved.thread_group_offset(0), 0U);
    EXPECT_EQ(resolved.thread_group_offset(1), 1U);
    EXPECT_EQ(resolved.thread_group_offset(4), 2U);
}

TEST(RuntimeConfigTests, RuntimeConfigValidationReportsInvalidFields) {
    af::runtime_config config;
    auto validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::no_threads);
    EXPECT_EQ(af::runtime_config_status_name(validation.status), "no_threads");

    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("bad", 0),
    };
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::thread_group_count_zero);
    EXPECT_EQ(validation.index, 1U);

    config.threads = {
        af::cpu_threads("too-many", std::numeric_limits<std::uint16_t>::max()),
        af::cpu_threads("overflow", 1),
    };
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::thread_count_overflow);
    EXPECT_EQ(validation.index, 1U);

    config.threads = {af::cpu_threads("logic", 1)};
    config.scheduler.task_drain_budget = 0;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::scheduler_task_drain_budget_zero);

    config.scheduler.task_drain_budget = 1;
    config.scheduler.max_task_run_slice = std::chrono::nanoseconds(-1);
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::scheduler_max_task_run_slice_negative);

    config.scheduler.max_task_run_slice = std::chrono::nanoseconds(0);
    config.timer.drain_budget = 0;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::timer_drain_budget_zero);

    config.timer.drain_budget = 1;
    config.timer.kind = af::timer_kind::hierarchical_wheel;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::timer_kind_unsupported);
    EXPECT_EQ(af::runtime_config_status_name(validation.status), "timer_kind_unsupported");

    config.timer.kind = af::timer_kind::min_heap;
    config.logger.consumer_thread = af::thread_selector::io(0);
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::log_consumer_thread_not_found);

    config.logger.consumer_thread = af::thread_selector::cpu(0);
    config.logger.backends = {af::udp_log_backend_config{"127.0.0.1", 9000}};
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::log_udp_backend_thread_not_found);
    EXPECT_EQ(validation.index, 0U);

    config.threads = {af::io_threads("io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);
    config.logger.backends = {af::file_log_backend_config{}};
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::log_file_backend_path_empty);
    EXPECT_EQ(validation.index, 0U);
}

TEST(RuntimeConfigTests, RuntimeInstanceRejectsInvalidConfig) {
    af::runtime_config config;

    try {
        af::runtime runtime(config);
        static_cast<void>(runtime);
        FAIL() << "runtime construction should reject an empty thread layout";
    } catch (const std::invalid_argument &error) {
        const std::string message = error.what();
        EXPECT_NE(message.find("no_threads"), std::string::npos);
    }
}

TEST(RuntimeConfigTests, RuntimeInstanceExposesResolvedThreadMetadata) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 2),
        af::cpu_threads("logic", 1),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);

    EXPECT_EQ(runtime.state(), af::runtime_state::stopped);
    EXPECT_EQ(runtime.thread_count(), 3U);
    EXPECT_EQ(runtime.invalid_thread_index(), 3U);
    EXPECT_TRUE(runtime.valid_thread(0));
    EXPECT_FALSE(runtime.valid_thread(3));
    EXPECT_EQ(runtime.select_thread(af::thread_selector::io(1)), 1U);
    EXPECT_EQ(runtime.select_thread(af::thread_selector::cpu(0)), 2U);
    EXPECT_EQ(runtime.thread_kind_of(0), af::thread_kind::io);
    EXPECT_EQ(runtime.thread_kind_of(2), af::thread_kind::cpu);
    EXPECT_EQ(runtime.thread_name(0), "io");
    EXPECT_EQ(runtime.thread_name(2), "logic");
    EXPECT_EQ(runtime.thread_group_offset(1), 1U);
    EXPECT_EQ(runtime.thread_group_offset(2), 0U);
    EXPECT_EQ(runtime.resolved_config().cpu_threads.size(), 1U);
    EXPECT_EQ(runtime.resolved_config().io_threads.size(), 2U);
}

TEST(RuntimeConfigTests, RuntimeInstanceStartStopRunsConfiguredThreads) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 2),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    ASSERT_TRUE(runtime.start());
    EXPECT_FALSE(runtime.start());
    EXPECT_EQ(runtime.state(), af::runtime_state::running);
    EXPECT_TRUE(wait_for_active_threads(runtime, 3));
    EXPECT_EQ(runtime.active_thread_count(), 3U);

    runtime.stop();
    EXPECT_EQ(runtime.state(), af::runtime_state::stopped);
    EXPECT_EQ(runtime.active_thread_count(), 0U);
    EXPECT_TRUE(runtime.start());
    EXPECT_TRUE(wait_for_active_threads(runtime, 3));
    runtime.stop();
    EXPECT_EQ(runtime.active_thread_count(), 0U);
}

TEST(RuntimeConfigTests, RuntimeInstancePostRunsWorkOnTargetThread) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 2),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<std::uint16_t> first_thread{af::runtime_invalid_thread_index};
    std::atomic<std::uint16_t> second_thread{af::runtime_invalid_thread_index};
    std::atomic<std::uint16_t> drain_thread{af::runtime_invalid_thread_index};
    std::atomic<bool> first_owner_matches{false};
    std::atomic<bool> second_owner_matches{false};
    std::atomic<bool> drain_owner_matches{false};
    PostedRuntimeWork first(counter, first_thread, first_owner_matches);
    PostedRuntimeWork second(counter, second_thread, second_owner_matches);
    PostedRuntimeWork drain(counter, drain_thread, drain_owner_matches);

    EXPECT_FALSE(runtime.post(runtime.select_thread(af::thread_selector::cpu(0)), &first));
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 3));

    const auto io_thread = runtime.select_thread(af::thread_selector::io(0));
    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(1));
    ASSERT_TRUE(runtime.post(io_thread, &first));
    ASSERT_TRUE(runtime.post(cpu_thread, &second));
    ASSERT_TRUE(wait_for_counter(counter, 2));
    EXPECT_EQ(first_thread.load(std::memory_order_acquire), io_thread);
    EXPECT_EQ(second_thread.load(std::memory_order_acquire), cpu_thread);
    EXPECT_TRUE(first_owner_matches.load(std::memory_order_acquire));
    EXPECT_TRUE(second_owner_matches.load(std::memory_order_acquire));

    ASSERT_TRUE(runtime.post(cpu_thread, &drain));
    runtime.stop();
    EXPECT_EQ(counter.load(std::memory_order_acquire), 3);
    EXPECT_EQ(drain_thread.load(std::memory_order_acquire), cpu_thread);
    EXPECT_TRUE(drain_owner_matches.load(std::memory_order_acquire));
    EXPECT_FALSE(runtime.post(cpu_thread, &first));
}

TEST(RuntimeConfigTests, RuntimeWakePolicyEmptyToNonEmptyDrainsQueuedPosts) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.scheduler.wake = af::wake_policy::empty_to_non_empty;
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<std::uint16_t> first_thread{af::runtime_invalid_thread_index};
    std::atomic<std::uint16_t> second_thread{af::runtime_invalid_thread_index};
    std::atomic<bool> first_owner_matches{false};
    std::atomic<bool> second_owner_matches{false};
    PostedRuntimeWork first(counter, first_thread, first_owner_matches);
    PostedRuntimeWork second(counter, second_thread, second_owner_matches);

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    ASSERT_TRUE(runtime.post(cpu_thread, &first));
    ASSERT_TRUE(runtime.post(cpu_thread, &second));
    ASSERT_TRUE(wait_for_counter(counter, 2));
    EXPECT_EQ(first_thread.load(std::memory_order_acquire), cpu_thread);
    EXPECT_EQ(second_thread.load(std::memory_order_acquire), cpu_thread);
    EXPECT_TRUE(first_owner_matches.load(std::memory_order_acquire));
    EXPECT_TRUE(second_owner_matches.load(std::memory_order_acquire));

    runtime.stop();
}

TEST(RuntimeConfigTests, RuntimeIdleWaitSpinAndYieldAcceptDelayedPosts) {
    expect_idle_wait_strategy_accepts_delayed_post(af::idle_wait_strategy::spin);
    expect_idle_wait_strategy_accepts_delayed_post(af::idle_wait_strategy::yield);
}

TEST(RuntimeConfigTests, RuntimeInstanceStopCanBeRequestedFromRuntimeThread) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    StopRuntimeWork work(counter);

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 2));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    ASSERT_TRUE(runtime.post(cpu_thread, &work));
    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_TRUE(wait_for_active_threads(runtime, 0));

    runtime.stop();
    EXPECT_EQ(runtime.state(), af::runtime_state::stopped);
    EXPECT_EQ(runtime.active_thread_count(), 0U);
}

TEST(RuntimeConfigTests, RuntimeInstanceRunsRegisteredServiceTasksOnTargetThread) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    config.scheduler.service_task_budget = 7;
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> service_counter{0};
    std::atomic<int> control_counter{0};
    std::atomic<std::uint16_t> observed_thread{af::runtime_invalid_thread_index};
    std::atomic<std::size_t> observed_budget{0};
    std::atomic<bool> register_ok{false};
    std::atomic<bool> unregister_ok{false};
    CountingRuntimeService service(service_counter, observed_thread, observed_budget);

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 2));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    service.mark_pending();
    ServiceTaskControlWork register_work(ServiceTaskControlWork::action::register_service,
                                         cpu_thread, service, control_counter, register_ok);
    ASSERT_TRUE(runtime.post(cpu_thread, &register_work));
    ASSERT_TRUE(wait_for_counter(control_counter, 1));
    EXPECT_TRUE(register_ok.load(std::memory_order_acquire));
    ASSERT_TRUE(wait_for_counter(service_counter, 1));
    EXPECT_EQ(observed_thread.load(std::memory_order_acquire), cpu_thread);
    EXPECT_EQ(observed_budget.load(std::memory_order_acquire), 7U);

    service.mark_pending();
    ASSERT_TRUE(runtime.wake_service_tasks(cpu_thread));
    ASSERT_TRUE(wait_for_counter(service_counter, 2));

    ServiceTaskControlWork unregister_work(ServiceTaskControlWork::action::unregister_service,
                                           cpu_thread, service, control_counter, unregister_ok);
    ASSERT_TRUE(runtime.post(cpu_thread, &unregister_work));
    ASSERT_TRUE(wait_for_counter(control_counter, 2));
    EXPECT_TRUE(unregister_ok.load(std::memory_order_acquire));

    service.mark_pending();
    ASSERT_TRUE(runtime.wake_service_tasks(cpu_thread));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(service_counter.load(std::memory_order_acquire), 2);

    runtime.stop();
}

TEST(RuntimeConfigTests, RuntimeTaskDrainBudgetLetsServiceTasksRunBetweenBursts) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.scheduler.task_drain_budget = 1;
    config.scheduler.service_task_budget = 1;
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> task_counter{0};
    std::atomic<int> service_counter{0};
    std::atomic<int> observed_task_count{-1};
    std::atomic<int> control_counter{0};
    std::atomic<bool> register_ok{false};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    TaskBudgetProbeService service(service_counter, task_counter, observed_task_count);
    ServiceTaskControlWork register_work(ServiceTaskControlWork::action::register_service,
                                         cpu_thread, service, control_counter, register_ok);
    ASSERT_TRUE(runtime.post(cpu_thread, &register_work));
    ASSERT_TRUE(wait_for_counter(control_counter, 1));
    ASSERT_TRUE(register_ok.load(std::memory_order_acquire));

    constexpr int task_limit = 1000000;
    RepostingRuntimeWork work(cpu_thread, task_limit, task_counter);
    ASSERT_TRUE(runtime.post(cpu_thread, &work));
    ASSERT_TRUE(wait_for_counter_at_least(task_counter, 16));

    service.mark_pending();
    ASSERT_TRUE(runtime.wake_service_tasks(cpu_thread));
    ASSERT_TRUE(wait_for_counter(service_counter, 1));

    EXPECT_GT(observed_task_count.load(std::memory_order_acquire), 0);
    EXPECT_LT(observed_task_count.load(std::memory_order_acquire), task_limit);

    runtime.stop();
}

TEST(RuntimeConfigTests, RuntimeTimerDrainBudgetLetsServiceTasksRunBetweenDueTimers) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.timer.drain_budget = 1;
    config.scheduler.service_task_budget = 1;
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> timer_counter{0};
    std::atomic<int> service_counter{0};
    std::atomic<int> observed_timer_count{-1};
    std::atomic<int> control_counter{0};
    std::atomic<bool> register_ok{false};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    TimerBudgetProbeService service(service_counter, timer_counter, observed_timer_count);
    ServiceTaskControlWork register_work(ServiceTaskControlWork::action::register_service,
                                         cpu_thread, service, control_counter, register_ok);
    ASSERT_TRUE(runtime.post(cpu_thread, &register_work));
    ASSERT_TRUE(wait_for_counter(control_counter, 1));
    ASSERT_TRUE(register_ok.load(std::memory_order_acquire));

    auto task0 = af::make_task<TimerBudgetRuntimeTask>(runtime, timer_counter, service);
    auto task1 = af::make_task<TimerBudgetRuntimeTask>(runtime, timer_counter, service);
    auto task2 = af::make_task<TimerBudgetRuntimeTask>(runtime, timer_counter, service);
    auto task3 = af::make_task<TimerBudgetRuntimeTask>(runtime, timer_counter, service);
    ASSERT_TRUE(task0->do_it(cpu_thread));
    ASSERT_TRUE(task1->do_it(cpu_thread));
    ASSERT_TRUE(task2->do_it(cpu_thread));
    ASSERT_TRUE(task3->do_it(cpu_thread));
    task0.reset();
    task1.reset();
    task2.reset();
    task3.reset();

    ASSERT_TRUE(wait_for_counter_at_least(service_counter, 1));
    EXPECT_EQ(observed_timer_count.load(std::memory_order_acquire), 1);
    ASSERT_TRUE(wait_for_counter(timer_counter, 4));

    runtime.stop();
}

TEST(RuntimeConfigTests, RuntimeTaskRunSliceLetsServiceTasksRunBeforeLargeTaskBudget) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.scheduler.task_drain_budget = 1000000;
    config.scheduler.service_task_budget = 1;
    config.scheduler.max_task_run_slice = std::chrono::nanoseconds(1);
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> task_counter{0};
    std::atomic<int> service_counter{0};
    std::atomic<int> observed_task_count{-1};
    std::atomic<int> control_counter{0};
    std::atomic<bool> register_ok{false};
    std::atomic<bool> release_first_run{false};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    TaskBudgetProbeService service(service_counter, task_counter, observed_task_count);
    ServiceTaskControlWork register_work(ServiceTaskControlWork::action::register_service,
                                         cpu_thread, service, control_counter, register_ok);
    ASSERT_TRUE(runtime.post(cpu_thread, &register_work));
    ASSERT_TRUE(wait_for_counter(control_counter, 1));
    ASSERT_TRUE(register_ok.load(std::memory_order_acquire));

    constexpr int task_limit = 1000000;
    RepostingRuntimeWork work(cpu_thread, task_limit, task_counter, &release_first_run);
    ASSERT_TRUE(runtime.post(cpu_thread, &work));
    ASSERT_TRUE(wait_for_counter_at_least(task_counter, 1));

    service.mark_pending();
    ASSERT_TRUE(runtime.wake_service_tasks(cpu_thread));
    release_first_run.store(true, std::memory_order_release);
    ASSERT_TRUE(wait_for_counter(service_counter, 1));

    EXPECT_GT(observed_task_count.load(std::memory_order_acquire), 0);
    EXPECT_LT(observed_task_count.load(std::memory_order_acquire), task_limit);

    runtime.stop();
}

TEST(RuntimeConfigTests, RuntimeTaskPoolConfigAllowsSmallInitialSlabsToExpand) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.task_pool.slab_object_count = 1;
    config.task_pool.oom = af::oom_policy::throw_exception;

    af::runtime runtime(config);
    std::array<af::runtime_task_handle<PoolOnlyRuntimeTask>, 8> tasks;
    for (auto &task : tasks) {
        task = af::make_task<PoolOnlyRuntimeTask>(runtime);
        EXPECT_TRUE(task);
        EXPECT_NE(task->task_id(), af::runtime_invalid_task_id);
    }
}

TEST(RuntimeConfigTests, RuntimeTryMakeTaskReturnsEmptyHandleWhenConstructorThrows) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.task_pool.slab_object_count = 1;
    config.task_pool.oom = af::oom_policy::throw_exception;

    af::runtime runtime(config);

    auto failed = af::try_make_task<ThrowingRuntimeTask>(runtime, true);
    EXPECT_FALSE(failed);
    EXPECT_THROW(static_cast<void>(af::make_task<ThrowingRuntimeTask>(runtime, true)),
                 std::runtime_error);

    auto task = af::make_task<ThrowingRuntimeTask>(runtime, false);
    EXPECT_TRUE(task);
    EXPECT_NE(task->task_id(), af::runtime_invalid_task_id);
}

TEST(RuntimeConfigTests, RuntimeMakeTaskSchedulesAndTracksTaskId) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<af::runtime_task_id> observed_task_id{af::runtime_invalid_task_id};
    std::atomic<af::runtime_task_id> observed_current_task_id{af::runtime_invalid_task_id};
    std::atomic<std::uint16_t> observed_thread{af::runtime_invalid_thread_index};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 2));

    auto task = af::make_task<InstanceRuntimeTask>(runtime, counter, observed_task_id,
                                                   observed_current_task_id, observed_thread);
    const auto task_id = task->task_id();
    ASSERT_NE(task_id, af::runtime_invalid_task_id);

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    ASSERT_TRUE(task->do_it(cpu_thread));
    task.reset();

    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_EQ(observed_task_id.load(std::memory_order_acquire), task_id);
    EXPECT_EQ(observed_current_task_id.load(std::memory_order_acquire), task_id);
    EXPECT_EQ(observed_thread.load(std::memory_order_acquire), cpu_thread);

    runtime.stop();
}

TEST(RuntimeConfigTests, RuntimeTaskDefersRunningScheduleUntilRunReturns) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 2),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<std::uint16_t> first_thread{af::runtime_invalid_thread_index};
    std::atomic<std::uint16_t> second_thread{af::runtime_invalid_thread_index};
    std::atomic<bool> next_schedule_ok{false};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 3));

    const auto cpu0 = runtime.select_thread(af::thread_selector::cpu(0));
    const auto cpu1 = runtime.select_thread(af::thread_selector::cpu(1));
    auto task = af::make_task<TwoHopRuntimeTask>(runtime, cpu1, counter, first_thread,
                                                 second_thread, next_schedule_ok);
    ASSERT_TRUE(task->do_it(cpu0));
    task.reset();

    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_TRUE(next_schedule_ok.load(std::memory_order_acquire));
    EXPECT_EQ(first_thread.load(std::memory_order_acquire), cpu0);
    EXPECT_EQ(second_thread.load(std::memory_order_acquire), cpu1);

    runtime.stop();
}

TEST(RuntimeConfigTests, RuntimeTaskScheduleAfterRunsOnTargetThread) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<std::uint16_t> observed_thread{af::runtime_invalid_thread_index};
    std::atomic<long long> elapsed_ns{0};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 2));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    auto task = af::make_task<DelayedRuntimeTask>(runtime, counter, observed_thread, elapsed_ns);
    ASSERT_TRUE(task->do_it(cpu_thread, std::chrono::milliseconds(20)));
    task.reset();

    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_EQ(observed_thread.load(std::memory_order_acquire), cpu_thread);
    EXPECT_GE(elapsed_ns.load(std::memory_order_acquire), 5'000'000LL);

    runtime.stop();
}

TEST(RuntimeConfigTests, RuntimeIdleWaitSpinAndYieldRunDelayedTasks) {
    expect_idle_wait_strategy_runs_delayed_task(af::idle_wait_strategy::spin);
    expect_idle_wait_strategy_runs_delayed_task(af::idle_wait_strategy::yield);
}

TEST(RuntimeConfigTests, RuntimeTaskCanRequestDelayedNextHopWhileRunning) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 2),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<std::uint16_t> first_thread{af::runtime_invalid_thread_index};
    std::atomic<std::uint16_t> second_thread{af::runtime_invalid_thread_index};
    std::atomic<bool> next_schedule_ok{false};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 3));

    const auto cpu0 = runtime.select_thread(af::thread_selector::cpu(0));
    const auto cpu1 = runtime.select_thread(af::thread_selector::cpu(1));
    auto task = af::make_task<DelayedTwoHopRuntimeTask>(runtime, cpu1, counter, first_thread,
                                                        second_thread, next_schedule_ok);
    ASSERT_TRUE(task->do_it(cpu0));
    task.reset();

    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_TRUE(next_schedule_ok.load(std::memory_order_acquire));
    EXPECT_EQ(first_thread.load(std::memory_order_acquire), cpu0);
    EXPECT_EQ(second_thread.load(std::memory_order_acquire), cpu1);

    runtime.stop();
}

TEST(RuntimeConfigTests, RuntimeStopCancelsPendingDelayedTask) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> destroyed{0};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 2));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    auto task = af::make_task<CancelledDelayedRuntimeTask>(runtime, destroyed);
    ASSERT_TRUE(task->do_it(cpu_thread));
    task.reset();

    runtime.stop();
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

void run_reactor_readiness_test(af::reactor_backend backend) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    config.reactor.backend = backend;
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    int pipe_fds[2]{-1, -1};
    ASSERT_EQ(::pipe(pipe_fds), 0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<std::uint16_t> observed_thread{af::runtime_invalid_thread_index};
    std::atomic<std::uint32_t> observed_events{0};
    ReactorReadinessState state{pipe_fds[0], pipe_fds[1],     {},
                                counter,     observed_thread, observed_events};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 2));

    const auto io_thread = runtime.select_thread(af::thread_selector::io(0));
    auto task = af::make_task<ReactorReadinessTask>(runtime, state);
    ASSERT_TRUE(task->do_it(io_thread));
    task.reset();

    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_EQ(observed_thread.load(std::memory_order_acquire), io_thread);
    EXPECT_NE(observed_events.load(std::memory_order_acquire) & af::reactor_readable, 0U);

    runtime.stop();
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
}

TEST(RuntimeConfigTests, RuntimeSelectReactorDispatchesReadinessOnIoThread) {
    run_reactor_readiness_test(af::reactor_backend::select);
}

TEST(RuntimeConfigTests, RuntimeAutoReactorDispatchesReadinessOnIoThread) {
    run_reactor_readiness_test(af::reactor_backend::auto_select);
}

TEST(RuntimeConfigTests, RuntimeEpollReactorDispatchesReadinessOnIoThread) {
    if (!af::supports_epoll) {
        GTEST_SKIP() << "epoll is not supported on this platform";
    }
    run_reactor_readiness_test(af::reactor_backend::epoll);
}

TEST(RuntimeConfigTests, RuntimeKqueueReactorDispatchesReadinessOnIoThread) {
    if (!af::supports_kqueue) {
        GTEST_SKIP() << "kqueue is not supported on this platform";
    }
    run_reactor_readiness_test(af::reactor_backend::kqueue);
}
