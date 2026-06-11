#include <array>
#include <chrono>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <variant>

#include <pthread.h>
#include <unistd.h>

#if defined(__linux__)
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#endif

#include <gtest/gtest.h>

#include "af/platform.hpp"
#include "af/runtime.hpp"
#include "af/runtime/detail/pooled_object.hpp"
#include "af/runtime/detail/task_pool.hpp"
#include "af/runtime_config.hpp"
#include "af/thread_layout.hpp"

namespace {

template <typename EnumT, typename = void> struct has_done_value : std::false_type {};
template <typename EnumT>
struct has_done_value<EnumT, std::void_t<decltype(EnumT::Done)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_pending_value : std::false_type {};
template <typename EnumT>
struct has_pending_value<EnumT, std::void_t<decltype(EnumT::Pending)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_again_value : std::false_type {};
template <typename EnumT>
struct has_again_value<EnumT, std::void_t<decltype(EnumT::Again)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_failed_value : std::false_type {};
template <typename EnumT>
struct has_failed_value<EnumT, std::void_t<decltype(EnumT::Failed)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_cancelled_value : std::false_type {};
template <typename EnumT>
struct has_cancelled_value<EnumT, std::void_t<decltype(EnumT::Cancelled)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_wait_for_tasks_value : std::false_type {};
template <typename EnumT>
struct has_wait_for_tasks_value<EnumT, std::void_t<decltype(EnumT::WaitForTasks)>>
    : std::true_type {};

template <typename EnumT, typename = void> struct has_stop_immediately_value : std::false_type {};
template <typename EnumT>
struct has_stop_immediately_value<EnumT, std::void_t<decltype(EnumT::StopImmediately)>>
    : std::true_type {};

template <typename EnumT, typename = void> struct has_created_value : std::false_type {};
template <typename EnumT>
struct has_created_value<EnumT, std::void_t<decltype(EnumT::Created)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_queued_value : std::false_type {};
template <typename EnumT>
struct has_queued_value<EnumT, std::void_t<decltype(EnumT::Queued)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_timer_arming_value : std::false_type {};
template <typename EnumT>
struct has_timer_arming_value<EnumT, std::void_t<decltype(EnumT::TimerArming)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_timer_pending_value : std::false_type {};
template <typename EnumT>
struct has_timer_pending_value<EnumT, std::void_t<decltype(EnumT::TimerPending)>> : std::true_type {
};

template <typename EnumT, typename = void> struct has_starting_value : std::false_type {};
template <typename EnumT>
struct has_starting_value<EnumT, std::void_t<decltype(EnumT::Starting)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_running_value : std::false_type {};
template <typename EnumT>
struct has_running_value<EnumT, std::void_t<decltype(EnumT::Running)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_drop_newest_value : std::false_type {};
template <typename EnumT>
struct has_drop_newest_value<EnumT, std::void_t<decltype(EnumT::DropNewest)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_block_value : std::false_type {};
template <typename EnumT>
struct has_block_value<EnumT, std::void_t<decltype(EnumT::Block)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_ordered_value : std::false_type {};
template <typename EnumT>
struct has_ordered_value<EnumT, std::void_t<decltype(EnumT::Ordered)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_relaxed_value : std::false_type {};
template <typename EnumT>
struct has_relaxed_value<EnumT, std::void_t<decltype(EnumT::Relaxed)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_non_empty_only_value : std::false_type {};
template <typename EnumT>
struct has_non_empty_only_value<EnumT, std::void_t<decltype(EnumT::NonEmptyOnly)>>
    : std::true_type {};

template <typename EnumT, typename = void> struct has_all_shards_value : std::false_type {};
template <typename EnumT>
struct has_all_shards_value<EnumT, std::void_t<decltype(EnumT::AllShards)>> : std::true_type {};

template <typename EnumT, typename = void> struct has_strict_value : std::false_type {};
template <typename EnumT>
struct has_strict_value<EnumT, std::void_t<decltype(EnumT::Strict)>> : std::true_type {};

template <typename EnumT, typename = void>
struct has_skip_already_applied_value : std::false_type {};
template <typename EnumT>
struct has_skip_already_applied_value<EnumT, std::void_t<decltype(EnumT::SkipAlreadyApplied)>>
    : std::true_type {};

static_assert(af::supports_native_io_wait == (af::supports_epoll || af::supports_kqueue));
static_assert(af::supports_eventfd == af::platform_linux);
static_assert(af::supports_timerfd == af::platform_linux);
static_assert(af::supports_openat2 == af::platform_linux);
static_assert(af::supports_sendfile == af::platform_linux);
static_assert(af::supports_splice == af::platform_linux);
static_assert(af::supports_zero_copy_send == af::platform_linux);
static_assert(af::supports_thread_affinity == af::platform_linux);
static_assert(af::supports_thread_priority == af::platform_linux);
static_assert(af::platform_posix != af::platform_windows);
static_assert(std::is_enum_v<af::task_result>);
static_assert(std::is_same_v<af::detail::runtime_service_task, af::detail::RuntimeServiceTask>);
static_assert(std::is_same_v<af::detail::runtime_timer_entry, af::detail::RuntimeTimerEntry>);
static_assert(std::is_same_v<af::detail::runtime_timer_heap, af::detail::RuntimeTimerHeap>);
static_assert(std::is_same_v<af::detail::runtime_hierarchical_timer_wheel,
                             af::detail::RuntimeHierarchicalTimerWheel>);
static_assert(std::is_enum_v<af::shutdown_policy>);
static_assert(std::is_enum_v<af::task_state>);
static_assert(std::is_enum_v<af::parallel_mode>);
static_assert(std::is_enum_v<af::ordered_batch_replay_policy>);
static_assert(std::is_class_v<af::ordered_batch_options>);
static_assert(std::is_class_v<af::sharded_ops<int>>);
static_assert(af::task_result::done == af::task_result::done);
static_assert(af::task_result::pending == af::task_result::pending);
static_assert(af::task_result::again == af::task_result::again);
static_assert(af::task_result::failed == af::task_result::failed);
static_assert(af::task_result::cancelled == af::task_result::cancelled);
static_assert(!has_done_value<af::task_result>::value);
static_assert(!has_pending_value<af::task_result>::value);
static_assert(!has_again_value<af::task_result>::value);
static_assert(!has_failed_value<af::task_result>::value);
static_assert(!has_cancelled_value<af::task_result>::value);
static_assert(af::shutdown_policy::wait_for_tasks == af::shutdown_policy::wait_for_tasks);
static_assert(af::shutdown_policy::stop_immediately == af::shutdown_policy::stop_immediately);
static_assert(!has_wait_for_tasks_value<af::shutdown_policy>::value);
static_assert(!has_stop_immediately_value<af::shutdown_policy>::value);
static_assert(af::task_state::created == af::task_state::created);
static_assert(af::task_state::queued == af::task_state::queued);
static_assert(af::task_state::timer_arming == af::task_state::timer_arming);
static_assert(af::task_state::timer_pending == af::task_state::timer_pending);
static_assert(af::task_state::starting == af::task_state::starting);
static_assert(af::task_state::running == af::task_state::running);
static_assert(af::task_state::pending == af::task_state::pending);
static_assert(af::task_state::done == af::task_state::done);
static_assert(!has_created_value<af::task_state>::value);
static_assert(!has_queued_value<af::task_state>::value);
static_assert(!has_timer_arming_value<af::task_state>::value);
static_assert(!has_timer_pending_value<af::task_state>::value);
static_assert(!has_starting_value<af::task_state>::value);
static_assert(!has_running_value<af::task_state>::value);
static_assert(!has_pending_value<af::task_state>::value);
static_assert(!has_done_value<af::task_state>::value);
static_assert(af::parallel_mode::non_empty_only == af::parallel_mode::non_empty_only);
static_assert(af::parallel_mode::all_shards == af::parallel_mode::all_shards);
static_assert(!has_non_empty_only_value<af::parallel_mode>::value);
static_assert(!has_all_shards_value<af::parallel_mode>::value);
static_assert(af::ordered_batch_replay_policy::strict == af::ordered_batch_replay_policy::strict);
static_assert(af::ordered_batch_replay_policy::skip_already_applied ==
              af::ordered_batch_replay_policy::skip_already_applied);
static_assert(!has_strict_value<af::ordered_batch_replay_policy>::value);
static_assert(!has_skip_already_applied_value<af::ordered_batch_replay_policy>::value);
static_assert(std::is_enum_v<af::log_ordering>);
static_assert(std::is_enum_v<af::log_overflow_policy>);
static_assert(std::is_same_v<af::detail::runtime_pooled_object_pool_type<int, 8>,
                             af::detail::RuntimePooledObjectPool<int, 8>>);
static_assert(std::is_same_v<af::detail::runtime_pooled_object_pool_holder_type<int, 8>,
                             af::detail::RuntimePooledObjectPoolHolder<int, 8>>);
static_assert(std::is_same_v<af::detail::runtime_task_pool_type<int, 8>,
                             af::detail::RuntimeTaskPool<int, 8>>);
static_assert(std::is_same_v<af::detail::runtime_task_pool_holder_type<int, 8>,
                             af::detail::RuntimeTaskPoolHolder<int, 8>>);
static_assert(std::is_same_v<af::detail::ordered_batch_state, af::detail::OrderedBatchState>);
static_assert(std::is_class_v<af::detail::runtime_instance_parallel_group>);
static_assert(std::is_class_v<af::detail::runtime_instance_parallel_group_pool_type>);
static_assert(af::log_ordering::ordered == af::log_ordering::ordered);
static_assert(af::log_ordering::relaxed == af::log_ordering::relaxed);
static_assert(!has_ordered_value<af::log_ordering>::value);
static_assert(!has_relaxed_value<af::log_ordering>::value);
static_assert(af::log_overflow_policy::drop_newest == af::log_overflow_policy::drop_newest);
static_assert(af::log_overflow_policy::block == af::log_overflow_policy::block);
static_assert(!has_drop_newest_value<af::log_overflow_policy>::value);
static_assert(!has_block_value<af::log_overflow_policy>::value);

struct layout_logic_tag;
struct layout_io_tag;

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

[[nodiscard]] std::string current_os_thread_name() {
    std::array<char, 64> name{};
#if defined(__APPLE__) || defined(__linux__)
    if (::pthread_getname_np(::pthread_self(), name.data(), name.size()) != 0) {
        return {};
    }
#endif
    return name.data();
}

class ThreadNameProbeWork final : public af::runtime_work {
public:
    ThreadNameProbeWork(std::string &observed_name, std::atomic<int> &counter)
        : observed_name_(observed_name), counter_(counter) {}

    void run(af::runtime &owner) noexcept override {
        static_cast<void>(owner);
        observed_name_ = current_os_thread_name();
        counter_.fetch_add(1, std::memory_order_release);
    }

private:
    std::string &observed_name_;
    std::atomic<int> &counter_;
};

#if defined(__linux__)
[[nodiscard]] std::uint32_t first_allowed_cpu_id() noexcept {
    cpu_set_t set;
    CPU_ZERO(&set);
    if (::pthread_getaffinity_np(::pthread_self(), sizeof(set), &set) != 0) {
        return std::numeric_limits<std::uint32_t>::max();
    }
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &set)) {
            return static_cast<std::uint32_t>(cpu);
        }
    }
    return std::numeric_limits<std::uint32_t>::max();
}

class ThreadAffinityProbeWork final : public af::runtime_work {
public:
    ThreadAffinityProbeWork(std::uint32_t expected_cpu, std::atomic<int> &counter,
                            std::atomic<bool> &matches)
        : expected_cpu_(expected_cpu), counter_(counter), matches_(matches) {}

    void run(af::runtime &owner) noexcept override {
        static_cast<void>(owner);
        cpu_set_t set;
        CPU_ZERO(&set);
        bool matches = false;
        if (::pthread_getaffinity_np(::pthread_self(), sizeof(set), &set) == 0) {
            int selected_count = 0;
            for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
                if (CPU_ISSET(cpu, &set)) {
                    ++selected_count;
                }
            }
            matches = expected_cpu_ < CPU_SETSIZE &&
                      CPU_ISSET(static_cast<int>(expected_cpu_), &set) && selected_count == 1;
        }
        matches_.store(matches, std::memory_order_release);
        counter_.fetch_add(1, std::memory_order_release);
    }

private:
    std::uint32_t expected_cpu_;
    std::atomic<int> &counter_;
    std::atomic<bool> &matches_;
};

[[nodiscard]] int current_linux_thread_nice() noexcept {
    const auto tid = static_cast<id_t>(::syscall(SYS_gettid));
    errno = 0;
    const int value = ::getpriority(PRIO_PROCESS, tid);
    if (value == -1 && errno != 0) {
        return std::numeric_limits<int>::max();
    }
    return value;
}

class ThreadPriorityProbeWork final : public af::runtime_work {
public:
    ThreadPriorityProbeWork(std::atomic<int> &observed_priority, std::atomic<int> &counter)
        : observed_priority_(observed_priority), counter_(counter) {}

    void run(af::runtime &owner) noexcept override {
        static_cast<void>(owner);
        observed_priority_.store(current_linux_thread_nice(), std::memory_order_release);
        counter_.fetch_add(1, std::memory_order_release);
    }

private:
    std::atomic<int> &observed_priority_;
    std::atomic<int> &counter_;
};
#endif

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

class CountingRuntimeService final : public af::detail::runtime_service_task {
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

    ServiceTaskControlWork(action op, std::uint16_t thread,
                           af::detail::runtime_service_task &service, std::atomic<int> &counter,
                           std::atomic<bool> &ok)
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
    af::detail::runtime_service_task &service_;
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

class TaskBudgetProbeService final : public af::detail::runtime_service_task {
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

class TimerBudgetProbeService final : public af::detail::runtime_service_task {
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

    [[nodiscard]] bool do_it(af::thread_ref thread) noexcept {
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

class RescheduleRuntimeTask final : public af::runtime_task {
public:
    RescheduleRuntimeTask(af::runtime_task::factory_token token, af::runtime &owner,
                          std::atomic<int> &run_count, std::atomic<std::uint16_t> &first_thread,
                          std::atomic<std::uint16_t> &second_thread)
        : af::runtime_task(token, owner), run_count_(run_count), first_thread_(first_thread),
          second_thread_(second_thread) {}

    [[nodiscard]] bool do_it(std::uint16_t thread) noexcept {
        return schedule_to(thread);
    }

private:
    af::task_result run_task() noexcept override {
        const int phase = run_count_.fetch_add(1, std::memory_order_acq_rel);
        if (phase == 0) {
            first_thread_.store(af::runtime::current_thread_index(), std::memory_order_release);
            return reschedule();
        }

        second_thread_.store(af::runtime::current_thread_index(), std::memory_order_release);
        return done();
    }

    std::atomic<int> &run_count_;
    std::atomic<std::uint16_t> &first_thread_;
    std::atomic<std::uint16_t> &second_thread_;
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
    af::runtime *runtime{nullptr};
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
        if (state.runtime != nullptr) {
            static_cast<void>(state.runtime->unregister_reactor_source(
                af::runtime::current_thread_index(), &source));
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
        if (!owner().register_reactor_source(af::runtime::current_thread_index(), &state_.source)) {
            return failed();
        }

        const char value = 'x';
        if (::write(state_.write_fd, &value, sizeof(value)) != sizeof(value)) {
            static_cast<void>(owner().unregister_reactor_source(af::runtime::current_thread_index(),
                                                                &state_.source));
            return failed();
        }
        return done();
    }

    ReactorReadinessState &state_;
};

struct ReactorBudgetPipe {
    ReactorBudgetPipe() = default;

    ~ReactorBudgetPipe() {
        close_all();
    }

    ReactorBudgetPipe(const ReactorBudgetPipe &) = delete;
    ReactorBudgetPipe &operator=(const ReactorBudgetPipe &) = delete;

    [[nodiscard]] bool open_pipe() noexcept {
        int pipe_fds[2]{-1, -1};
        if (::pipe(pipe_fds) != 0) {
            return false;
        }
        read_fd = pipe_fds[0];
        write_fd = pipe_fds[1];
        return true;
    }

    [[nodiscard]] bool write_ready_byte() const noexcept {
        const char value = 'x';
        return ::write(write_fd, &value, sizeof(value)) == sizeof(value);
    }

    void close_all() noexcept {
        if (read_fd >= 0) {
            ::close(read_fd);
            read_fd = -1;
        }
        if (write_fd >= 0) {
            ::close(write_fd);
            write_fd = -1;
        }
    }

    int read_fd{-1};
    int write_fd{-1};
    af::fd_event_source source{};
    int callbacks{0};
    std::uint32_t events{0};
};

void on_reactor_budget_event(void *owner, af::fd_event_source &, std::uint32_t events) noexcept {
    auto &pipe = *static_cast<ReactorBudgetPipe *>(owner);
    char value = 0;
    static_cast<void>(::read(pipe.read_fd, &value, sizeof(value)));
    pipe.events |= events;
    ++pipe.callbacks;
}

template <std::size_t Size>
[[nodiscard]] int
total_reactor_budget_callbacks(const std::array<ReactorBudgetPipe, Size> &pipes) noexcept {
    int total = 0;
    for (const ReactorBudgetPipe &pipe : pipes) {
        total += pipe.callbacks;
    }
    return total;
}

} // namespace

TEST(RuntimeConfigTests, PreservesThreadCountsAboveSixtyFour) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 257)};

    const auto resolution = af::resolve_runtime_config(config);
    ASSERT_TRUE(resolution);
    EXPECT_EQ(resolution.resolved.thread_count(), 257U);
    EXPECT_EQ(resolution.resolved.invalid_thread_index(), 257U);
    EXPECT_TRUE(resolution.resolved.valid_thread(256U));
    EXPECT_FALSE(resolution.resolved.valid_thread(257U));
}

TEST(RuntimeConfigTests, ThreadLayoutGroupsCarryKindNameAndIndexMetadata) {
    af::runtime_config config;
    config.threads = {
        af::cpu_threads("logic", 3),
        af::io_threads("io", 2),
        af::cpu_threads("log", 1),
    };

    const auto resolution = af::resolve_runtime_config(config);
    ASSERT_TRUE(resolution);
    const auto &resolved = resolution.resolved;
    EXPECT_EQ(resolved.thread_count(), 6U);
    EXPECT_EQ(sizeof(af::thread_ref), sizeof(std::uint16_t));

    const af::thread_group_ref logic = resolved.thread_group("logic");
    const af::thread_group_ref io = resolved.thread_group("io");
    const af::thread_group_ref log = resolved.thread_group("log");
    ASSERT_EQ(logic.size(), 3U);
    ASSERT_EQ(io.size(), 2U);
    ASSERT_EQ(log.size(), 1U);

    EXPECT_EQ(logic.front(), af::thread_ref(0));
    EXPECT_EQ(logic.at(2), af::thread_ref(2));
    EXPECT_EQ(io.front(), af::thread_ref(3));
    EXPECT_EQ(io.shard(5U), af::thread_ref(4));
    EXPECT_EQ(log.front(), af::thread_ref(5));
    EXPECT_TRUE(logic.contains(logic.at(1)));
    EXPECT_FALSE(logic.contains(io.front()));
    EXPECT_FALSE(log.contains(io.front()));

    EXPECT_EQ(resolved.thread_kind_of(logic.front()), af::thread_kind::cpu);
    EXPECT_EQ(resolved.thread_kind_of(io.front()), af::thread_kind::io);
    EXPECT_EQ(resolved.thread_kind_of(log.front()), af::thread_kind::cpu);
    EXPECT_EQ(resolved.thread_name(logic.front()), "logic");
    EXPECT_EQ(resolved.thread_name(io.at(1)), "io");
    EXPECT_EQ(resolved.thread_name(log.front()), "log");
    EXPECT_EQ(resolved.thread_group_offset(io.at(1)), 1U);
    EXPECT_EQ(resolved.thread_group_offset(log.front()), 0U);
}

TEST(RuntimeConfigTests, CompileTimeThreadLayoutUsesLowerCaseTypeNames) {
    using logic_spec = af::thread_group_spec<layout_logic_tag, 3, af::thread_kind::cpu>;
    using io_spec = af::thread_group_spec<layout_io_tag, 2, af::thread_kind::io>;
    using lower_layout = af::static_thread_layout<logic_spec, io_spec>;

    static_assert(std::is_class_v<logic_spec>);
    static_assert(std::is_class_v<lower_layout>);
    static_assert(std::is_class_v<typename lower_layout::thread>);

    const auto layout = af::thread_layout(logic_spec{"logic"}, io_spec{"io"});
    using layout_type = decltype(layout);
    using thread = typename layout_type::thread;
    static_assert(std::is_same_v<af::thread_id<typename layout_type::thread_shape>, thread>);
    static_assert(std::is_class_v<af::static_thread_group<thread, 3, 2>>);

    const auto logic = layout_type::template group<layout_logic_tag>();
    const auto io = layout_type::template group<layout_io_tag>();
    EXPECT_EQ(layout_type::thread_count, 5U);
    EXPECT_EQ(layout_type::group_begin_index<layout_logic_tag>(), 0U);
    EXPECT_EQ(layout_type::group_count<layout_io_tag>(), 2U);
    EXPECT_EQ(logic.at(2).index(), 2U);
    EXPECT_EQ(io.at(0).index(), 3U);
    EXPECT_EQ(io.shard(3U).index(), 4U);
    EXPECT_TRUE(io.contains(io.at<1>()));
    EXPECT_FALSE(io.contains(logic.front()));
    EXPECT_EQ(io.offset_of(io.at<1>()), 1U);
    EXPECT_EQ(layout.thread_kind(logic.front()), af::thread_kind::cpu);
    EXPECT_EQ(layout.thread_kind(io.front()), af::thread_kind::io);
    EXPECT_EQ(layout.thread_name(logic.front()), "logic");
    EXPECT_EQ(layout.thread_name(io.at<1>()), "io");
    EXPECT_EQ(layout.thread_group_offset(io.at<1>()), 1U);
}

TEST(RuntimeConfigTests, DefaultsAndOverridesPoolTimerAndServiceBudgets) {
    af::runtime_config defaults;
    defaults.threads = {af::cpu_threads("logic", 1)};
    auto resolution = af::resolve_runtime_config(defaults);
    ASSERT_TRUE(resolution);
    EXPECT_EQ(resolution.resolved.config.task_pool.local_cache_size, 256U);
    EXPECT_EQ(resolution.resolved.config.task_pool.slab_object_count, 4096U);
    EXPECT_EQ(resolution.resolved.config.timer.drain_budget, 256U);
    EXPECT_EQ(resolution.resolved.config.timer.initial_reserve, 1024U);
    EXPECT_EQ(resolution.resolved.config.scheduler.service_task_budget, 32U);

    af::runtime_config overrides;
    overrides.threads = {af::cpu_threads("logic", 1)};
    overrides.task_pool.local_cache_size = 128;
    overrides.task_pool.slab_object_count = 512;
    overrides.timer.drain_budget = 64;
    overrides.timer.initial_reserve = 2048;
    overrides.scheduler.service_task_budget = 8;

    resolution = af::resolve_runtime_config(overrides);
    ASSERT_TRUE(resolution);
    EXPECT_EQ(resolution.resolved.config.task_pool.local_cache_size, 128U);
    EXPECT_EQ(resolution.resolved.config.task_pool.slab_object_count, 512U);
    EXPECT_EQ(resolution.resolved.config.timer.drain_budget, 64U);
    EXPECT_EQ(resolution.resolved.config.timer.initial_reserve, 2048U);
    EXPECT_EQ(resolution.resolved.config.scheduler.service_task_budget, 8U);
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
    EXPECT_EQ(config.timer.kind, af::timer_kind::hierarchical_wheel);
    EXPECT_EQ(config.timer.tick, std::chrono::milliseconds(1));
    EXPECT_EQ(config.timer.drain_budget, 256U);
    EXPECT_EQ(config.reactor.backend, af::reactor_backend::auto_select);
    EXPECT_EQ(config.reactor.event_capacity, 1024U);
    EXPECT_EQ(config.logger.ordering, af::log_ordering::ordered);
    EXPECT_EQ(config.logger.consumer_thread.kind, af::thread_selector_kind::cpu);
    EXPECT_EQ(config.logger.consumer_thread.index, 0U);
    EXPECT_EQ(config.logger.overflow, af::log_overflow_policy::drop_newest);
    EXPECT_EQ(config.logger.record_pool.local_cache_size, 256U);
    EXPECT_EQ(config.logger.record_pool.slab_object_count, 4096U);
    EXPECT_EQ(config.logger.record_pool.oom, af::oom_policy::fatal);
    EXPECT_TRUE(config.logger.record_pool.enable_stats);
    EXPECT_EQ(config.shutdown.drain_timeout, std::chrono::seconds(5));
    EXPECT_EQ(config.shutdown.connection_close_timeout, std::chrono::seconds(5));
    EXPECT_EQ(config.shutdown.log_flush_timeout, std::chrono::seconds(5));
    EXPECT_TRUE(config.shutdown.stop_accept_first);
    EXPECT_TRUE(config.diagnostics.enable_task_id);
    EXPECT_TRUE(config.diagnostics.enable_stats);
    EXPECT_TRUE(config.diagnostics.enable_thread_name);
    EXPECT_TRUE(config.diagnostics.enable_queue_metrics);

    config.threads = {
        af::io_threads("io", 2),
        af::cpu_threads("logic", 4),
    };
    ASSERT_EQ(config.threads.size(), 2U);
    EXPECT_EQ(config.threads[0].name, "io");
    EXPECT_EQ(config.threads[0].kind, af::thread_kind::io);
    EXPECT_EQ(config.threads[0].count, 2U);
    EXPECT_TRUE(config.threads[0].set_os_thread_name);
    EXPECT_FALSE(config.threads[0].priority.enabled);
    EXPECT_EQ(config.threads[0].priority.value, 0);
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

TEST(RuntimeConfigTests, RuntimeThreadNameDiagnosticsControlsOsThreadName) {
    auto observe_name = [](bool enable_thread_name) {
        af::runtime_config config;
        config.threads = {af::cpu_threads("diag", 1)};
        config.diagnostics.enable_thread_name = enable_thread_name;
        config.logger.consumer_thread = af::thread_selector::cpu(0);

        af::runtime runtime(config);
        std::string observed_name;
        std::atomic<int> counter{0};
        ThreadNameProbeWork work(observed_name, counter);

        EXPECT_TRUE(runtime.start());
        EXPECT_TRUE(wait_for_active_threads(runtime, 1));
        const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
        EXPECT_TRUE(runtime.post(cpu_thread, &work));
        EXPECT_TRUE(wait_for_counter(counter, 1));
        runtime.stop();
        return observed_name;
    };

    constexpr std::string_view expected_name = "af-diag-0";
    EXPECT_EQ(observe_name(true), expected_name);
    EXPECT_NE(observe_name(false), expected_name);
}

TEST(RuntimeConfigTests, RuntimeAppliesThreadAffinityOnSupportedPlatforms) {
    if (!af::supports_thread_affinity) {
        GTEST_SKIP() << "thread affinity is not supported on this platform";
    }
#if defined(__linux__)
    const std::uint32_t cpu_id = first_allowed_cpu_id();
    if (cpu_id == std::numeric_limits<std::uint32_t>::max()) {
        GTEST_SKIP() << "no allowed CPU is visible for affinity test";
    }

    af::runtime_config config;
    config.threads = {af::cpu_threads("affinity", 1)};
    config.threads[0].affinity.cpu_ids = {cpu_id};
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<bool> matches{false};
    ThreadAffinityProbeWork work(cpu_id, counter, matches);

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));
    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    ASSERT_TRUE(runtime.post(cpu_thread, &work));
    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_TRUE(matches.load(std::memory_order_acquire));

    runtime.stop();
#endif
}

TEST(RuntimeConfigTests, RuntimeAppliesThreadPriorityOnSupportedPlatforms) {
    if (!af::supports_thread_priority) {
        GTEST_SKIP() << "thread priority is not supported on this platform";
    }
#if defined(__linux__)
    const int current_priority = current_linux_thread_nice();
    if (current_priority == std::numeric_limits<int>::max() ||
        current_priority >= af::thread_priority_max) {
        GTEST_SKIP() << "no lower Linux nice value is available for priority test";
    }
    const int target_priority = current_priority + 1;

    af::runtime_config config;
    config.threads = {af::cpu_threads("priority", 1)};
    config.threads[0].priority.enabled = true;
    config.threads[0].priority.value = target_priority;
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<int> observed_priority{std::numeric_limits<int>::max()};
    ThreadPriorityProbeWork work(observed_priority, counter);

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));
    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    ASSERT_TRUE(runtime.post(cpu_thread, &work));
    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_EQ(observed_priority.load(std::memory_order_acquire), target_priority);

    runtime.stop();
#endif
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
    EXPECT_EQ(resolved.select_thread_ref(af::thread_selector::cpu(2)), af::thread_ref(4));
    EXPECT_FALSE(resolved.select_thread_ref(af::thread_selector::io(2)));
    EXPECT_EQ(resolved.thread_name(0), "io");
    EXPECT_EQ(resolved.thread_name(3), "logic");
    EXPECT_EQ(resolved.thread_name(resolved.invalid_thread_index()), "invalid");
    EXPECT_EQ(resolved.thread_kind_of(0), af::thread_kind::io);
    EXPECT_EQ(resolved.thread_kind_of(4), af::thread_kind::cpu);
    EXPECT_EQ(resolved.thread_group_offset(0), 0U);
    EXPECT_EQ(resolved.thread_group_offset(1), 1U);
    EXPECT_EQ(resolved.thread_group_offset(4), 2U);
    ASSERT_EQ(resolved.thread_groups.size(), 2U);
    EXPECT_EQ(resolved.thread_groups[0].name, "io");
    EXPECT_EQ(resolved.thread_groups[0].kind, af::thread_kind::io);
    EXPECT_EQ(resolved.thread_groups[0].begin, 0U);
    EXPECT_EQ(resolved.thread_groups[0].count, 2U);
    EXPECT_EQ(resolved.thread_groups[1].name, "logic");
    EXPECT_EQ(resolved.thread_groups[1].kind, af::thread_kind::cpu);
    EXPECT_EQ(resolved.thread_groups[1].begin, 2U);
    EXPECT_EQ(resolved.thread_groups[1].count, 3U);
    EXPECT_EQ(resolved.config.task_pool.local_cache_size, 256U);

    const af::thread_group_ref io_threads = resolved.io_thread_group();
    const af::thread_group_ref cpu_threads = resolved.cpu_thread_group();
    const af::thread_group_ref io_group = resolved.thread_group(0);
    const af::thread_group_ref logic_group = resolved.thread_group("logic");
    ASSERT_EQ(io_threads.size(), 2U);
    ASSERT_EQ(cpu_threads.size(), 3U);
    ASSERT_EQ(io_group.size(), 2U);
    ASSERT_EQ(logic_group.size(), 3U);
    EXPECT_EQ(io_threads.front(), af::thread_ref(0));
    EXPECT_EQ(io_threads[1], af::thread_ref(1));
    EXPECT_EQ(io_threads.shard(3), af::thread_ref(1));
    EXPECT_TRUE(io_threads.contains(af::thread_ref(0)));
    EXPECT_FALSE(io_threads.contains(af::thread_ref(2)));
    EXPECT_EQ(cpu_threads.front(), af::thread_ref(2));
    EXPECT_EQ(cpu_threads.shard(5), af::thread_ref(4));
    EXPECT_EQ(resolved.thread_name(cpu_threads.shard(2)), "logic");
    EXPECT_EQ(resolved.thread_group_offset(cpu_threads.shard(2)), 2U);
    EXPECT_EQ(io_group.front(), af::thread_ref(0));
    EXPECT_EQ(logic_group.front(), af::thread_ref(2));
    EXPECT_EQ(logic_group.shard(5), af::thread_ref(4));
    EXPECT_FALSE(resolved.thread_group("missing"));
}

TEST(RuntimeConfigTests, RuntimeTaskPoolLocalCacheSizeUsesSizeClasses) {
    EXPECT_EQ(af::normalize_runtime_task_pool_local_cache_size(1), 2U);
    EXPECT_EQ(af::normalize_runtime_task_pool_local_cache_size(3), 4U);
    EXPECT_EQ(af::normalize_runtime_task_pool_local_cache_size(129), 256U);
    EXPECT_EQ(af::normalize_runtime_task_pool_local_cache_size(257), 512U);
    EXPECT_EQ(af::normalize_runtime_task_pool_local_cache_size(4096), 4096U);

    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.task_pool.local_cache_size = 257;

    auto resolution = af::resolve_runtime_config(config);
    ASSERT_TRUE(resolution);
    EXPECT_EQ(resolution.resolved.config.task_pool.local_cache_size, 512U);
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

    config.threads = {af::cpu_threads("priority", 1)};
    config.threads[0].priority.enabled = true;
    config.threads[0].priority.value = af::thread_priority_min - 1;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::thread_priority_value_out_of_range);
    EXPECT_EQ(af::runtime_config_status_name(validation.status),
              "thread_priority_value_out_of_range");
    EXPECT_EQ(validation.index, 0U);

    config.threads[0].priority.value = af::thread_priority_max + 1;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::thread_priority_value_out_of_range);
    EXPECT_EQ(validation.index, 0U);

    config.threads = {af::cpu_threads("logic", 1)};
    config.scheduler.task_drain_budget = 0;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::scheduler_task_drain_budget_zero);

    config.scheduler.task_drain_budget = 1;
    config.scheduler.max_task_run_slice = std::chrono::nanoseconds(-1);
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::scheduler_max_task_run_slice_negative);

    config.scheduler.max_task_run_slice = std::chrono::nanoseconds(0);
    config.task_pool.local_cache_size = 0;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::task_pool_local_cache_size_zero);
    EXPECT_EQ(af::runtime_config_status_name(validation.status), "task_pool_local_cache_size_zero");

    config.task_pool.local_cache_size = af::runtime_task_pool_max_local_cache_size + 1U;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::task_pool_local_cache_size_too_large);
    EXPECT_EQ(af::runtime_config_status_name(validation.status),
              "task_pool_local_cache_size_too_large");

    config.task_pool.local_cache_size = 1;
    config.timer.drain_budget = 0;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::timer_drain_budget_zero);

    config.timer.drain_budget = 1;
    config.timer.kind = af::timer_kind::hierarchical_wheel;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::ok);

    config.timer.kind = static_cast<af::timer_kind>(255);
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::timer_kind_unsupported);
    EXPECT_EQ(af::runtime_config_status_name(validation.status), "timer_kind_unsupported");

    config.timer.kind = af::timer_kind::min_heap;
    config.logger.record_pool.local_cache_size = 0;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::log_record_pool_local_cache_size_zero);
    EXPECT_EQ(af::runtime_config_status_name(validation.status),
              "log_record_pool_local_cache_size_zero");

    config.logger.record_pool.local_cache_size =
        af::async_log_record_pool_max_local_cache_size + 1U;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status,
              af::runtime_config_status::log_record_pool_local_cache_size_too_large);
    EXPECT_EQ(af::runtime_config_status_name(validation.status),
              "log_record_pool_local_cache_size_too_large");

    config.logger.record_pool.local_cache_size = 1;
    config.logger.record_pool.slab_object_count = 0;
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::log_record_pool_slab_object_count_zero);
    EXPECT_EQ(af::runtime_config_status_name(validation.status),
              "log_record_pool_slab_object_count_zero");

    config.logger.record_pool.slab_object_count = 1;
    config.logger.consumer_thread = af::thread_selector::io(0);
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::log_consumer_thread_not_found);

    config.logger.consumer_thread = af::thread_selector::cpu(0);
    config.logger.backends = {af::udp_log_backend_config{"127.0.0.1", 9000}};
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::log_udp_backend_thread_not_found);
    EXPECT_EQ(validation.index, 0U);

    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);
    config.logger.backends = {
        af::udp_log_backend_config{"127.0.0.1", 9000, 1400, af::thread_selector::cpu(0)}};
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::log_udp_backend_thread_not_io);
    EXPECT_EQ(af::runtime_config_status_name(validation.status), "log_udp_backend_thread_not_io");
    EXPECT_EQ(validation.index, 0U);

    config.logger.backends = {af::tcp_log_backend_config{
        "127.0.0.1", 9001, std::chrono::milliseconds(500), af::thread_selector::cpu(0)}};
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::log_tcp_backend_thread_not_io);
    EXPECT_EQ(af::runtime_config_status_name(validation.status), "log_tcp_backend_thread_not_io");
    EXPECT_EQ(validation.index, 0U);

    config.threads = {af::io_threads("io", 1)};
    config.logger.consumer_thread = af::thread_selector::io(0);
    config.logger.backends = {af::file_log_backend_config{}};
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::log_file_backend_path_empty);
    EXPECT_EQ(validation.index, 0U);

    config.threads = {af::cpu_threads("logic", 1)};
    config.logger.consumer_thread = af::thread_selector::cpu(0);
    config.logger.backends.clear();
    config.shutdown.drain_timeout = std::chrono::seconds(-1);
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::shutdown_drain_timeout_negative);
    EXPECT_EQ(af::runtime_config_status_name(validation.status), "shutdown_drain_timeout_negative");

    config.shutdown.drain_timeout = std::chrono::seconds(0);
    config.shutdown.connection_close_timeout = std::chrono::seconds(-1);
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status,
              af::runtime_config_status::shutdown_connection_close_timeout_negative);
    EXPECT_EQ(af::runtime_config_status_name(validation.status),
              "shutdown_connection_close_timeout_negative");

    config.shutdown.connection_close_timeout = std::chrono::seconds(0);
    config.shutdown.log_flush_timeout = std::chrono::seconds(-1);
    validation = af::validate_runtime_config(config);
    EXPECT_EQ(validation.status, af::runtime_config_status::shutdown_log_flush_timeout_negative);
    EXPECT_EQ(af::runtime_config_status_name(validation.status),
              "shutdown_log_flush_timeout_negative");
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
    EXPECT_EQ(runtime.select_thread_ref(af::thread_selector::io(1)), af::thread_ref(1));
    EXPECT_EQ(runtime.select_thread_ref(af::thread_selector::cpu(0)), af::thread_ref(2));
    EXPECT_EQ(runtime.thread_kind_of(0), af::thread_kind::io);
    EXPECT_EQ(runtime.thread_kind_of(2), af::thread_kind::cpu);
    EXPECT_EQ(runtime.thread_name(0), "io");
    EXPECT_EQ(runtime.thread_name(2), "logic");
    EXPECT_EQ(runtime.thread_group_offset(1), 1U);
    EXPECT_EQ(runtime.thread_group_offset(2), 0U);
    EXPECT_EQ(runtime.resolved_config().cpu_threads.size(), 1U);
    EXPECT_EQ(runtime.resolved_config().io_threads.size(), 2U);

    const af::thread_group_ref io_threads = runtime.io_threads();
    const af::thread_group_ref cpu_threads = runtime.cpu_threads();
    const af::thread_group_ref io_group = runtime.thread_group("io");
    const af::thread_group_ref logic_group = runtime.thread_group(1);
    ASSERT_EQ(io_threads.size(), 2U);
    ASSERT_EQ(cpu_threads.size(), 1U);
    ASSERT_EQ(io_group.size(), 2U);
    ASSERT_EQ(logic_group.size(), 1U);
    EXPECT_TRUE(runtime.valid_thread(io_threads.front()));
    EXPECT_EQ(runtime.thread_kind_of(io_threads.front()), af::thread_kind::io);
    EXPECT_EQ(runtime.thread_name(cpu_threads.front()), "logic");
    EXPECT_EQ(runtime.thread_group_offset(io_threads.shard(3)), 1U);
    EXPECT_TRUE(io_threads.contains(af::thread_ref(1)));
    EXPECT_FALSE(cpu_threads.contains(af::thread_ref(1)));
    EXPECT_EQ(io_group.shard(3), af::thread_ref(1));
    EXPECT_EQ(logic_group.front(), af::thread_ref(2));
    EXPECT_FALSE(runtime.thread_group("missing"));
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

    const af::thread_ref io_thread = runtime.io_threads().front();
    const af::thread_ref cpu_thread = runtime.cpu_threads().shard(1);
    ASSERT_TRUE(runtime.post(io_thread, &first));
    ASSERT_TRUE(runtime.post(cpu_thread, &second));
    ASSERT_TRUE(wait_for_counter(counter, 2));
    EXPECT_EQ(first_thread.load(std::memory_order_acquire), io_thread.index);
    EXPECT_EQ(second_thread.load(std::memory_order_acquire), cpu_thread.index);
    EXPECT_TRUE(first_owner_matches.load(std::memory_order_acquire));
    EXPECT_TRUE(second_owner_matches.load(std::memory_order_acquire));

    ASSERT_TRUE(runtime.post(cpu_thread, &drain));
    runtime.stop();
    EXPECT_EQ(counter.load(std::memory_order_acquire), 3);
    EXPECT_EQ(drain_thread.load(std::memory_order_acquire), cpu_thread.index);
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
    config.task_pool.local_cache_size = 3;
    config.task_pool.slab_object_count = 1;
    config.task_pool.oom = af::oom_policy::throw_exception;

    af::runtime runtime(config);
    EXPECT_EQ(runtime.config().task_pool.local_cache_size, 4U);
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

TEST(RuntimeConfigTests, RuntimeCanDisableTaskIdTracking) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.diagnostics.enable_task_id = false;
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::atomic<af::runtime_task_id> observed_task_id{1};
    std::atomic<af::runtime_task_id> observed_current_task_id{1};
    std::atomic<std::uint16_t> observed_thread{af::runtime_invalid_thread_index};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    auto task = af::make_task<InstanceRuntimeTask>(runtime, counter, observed_task_id,
                                                   observed_current_task_id, observed_thread);
    EXPECT_EQ(task->task_id(), af::runtime_invalid_task_id);
    ASSERT_TRUE(task->do_it(cpu_thread));
    task.reset();

    ASSERT_TRUE(wait_for_counter(counter, 1));
    EXPECT_EQ(observed_task_id.load(std::memory_order_acquire), af::runtime_invalid_task_id);
    EXPECT_EQ(observed_current_task_id.load(std::memory_order_acquire),
              af::runtime_invalid_task_id);
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

TEST(RuntimeConfigTests, RuntimeTaskRescheduleRunsAgainOnCurrentThread) {
    af::runtime_config config;
    config.threads = {
        af::io_threads("io", 1),
        af::cpu_threads("logic", 1),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    std::atomic<int> run_count{0};
    std::atomic<std::uint16_t> first_thread{af::runtime_invalid_thread_index};
    std::atomic<std::uint16_t> second_thread{af::runtime_invalid_thread_index};

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 2));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    auto task =
        af::make_task<RescheduleRuntimeTask>(runtime, run_count, first_thread, second_thread);
    ASSERT_TRUE(task->do_it(cpu_thread));
    task.reset();

    ASSERT_TRUE(wait_for_counter(run_count, 2));
    EXPECT_EQ(first_thread.load(std::memory_order_acquire), cpu_thread);
    EXPECT_EQ(second_thread.load(std::memory_order_acquire), cpu_thread);

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

TEST(RuntimeConfigTests, RuntimeHierarchicalWheelRunsDelayedTasks) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.logger.consumer_thread = af::thread_selector::cpu(0);
    config.timer.kind = af::timer_kind::hierarchical_wheel;
    config.timer.tick = std::chrono::milliseconds(1);
    config.timer.wheel_slots = 4;
    config.timer.drain_budget = 8;
    config.timer.initial_reserve = 4;

    af::runtime runtime(config);
    std::atomic<int> counter{0};
    std::array<std::atomic<std::uint16_t>, 3> observed_threads{};
    std::array<std::atomic<long long>, 3> elapsed_ns{};
    for (auto &thread : observed_threads) {
        thread.store(af::runtime_invalid_thread_index, std::memory_order_release);
    }
    for (auto &elapsed : elapsed_ns) {
        elapsed.store(0, std::memory_order_release);
    }

    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));

    const auto cpu_thread = runtime.select_thread(af::thread_selector::cpu(0));
    const std::array<std::chrono::milliseconds, 3> delays{
        std::chrono::milliseconds(2),
        std::chrono::milliseconds(7),
        std::chrono::milliseconds(20),
    };

    for (std::size_t i = 0; i < delays.size(); ++i) {
        auto task =
            af::make_task<DelayedRuntimeTask>(runtime, counter, observed_threads[i], elapsed_ns[i]);
        ASSERT_TRUE(task->do_it(cpu_thread, delays[i]));
        task.reset();
    }

    ASSERT_TRUE(wait_for_counter(counter, static_cast<int>(delays.size())));
    for (std::size_t i = 0; i < delays.size(); ++i) {
        EXPECT_EQ(observed_threads[i].load(std::memory_order_acquire), cpu_thread);
        EXPECT_GE(elapsed_ns[i].load(std::memory_order_acquire), 500'000LL);
    }

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
    ReactorReadinessState state{pipe_fds[0],     pipe_fds[1],    {}, &runtime, counter,
                                observed_thread, observed_events};

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

void run_reactor_event_budget_test(af::reactor_backend backend) {
    std::array<ReactorBudgetPipe, 3> pipes{};

    af::reactor_config config;
    config.backend = backend;
    config.event_capacity = 8;
    config.event_budget = 1;

    auto reactor = af::make_reactor(config);
    if (reactor == nullptr) {
        GTEST_SKIP() << "reactor backend is not supported on this platform";
    }

    for (ReactorBudgetPipe &pipe : pipes) {
        ASSERT_TRUE(pipe.open_pipe());
        pipe.source.fd = pipe.read_fd;
        pipe.source.interests = af::reactor_readable;
        pipe.source.owner = &pipe;
        pipe.source.on_event = &on_reactor_budget_event;
        ASSERT_TRUE(reactor->add(&pipe.source));
    }
    for (const ReactorBudgetPipe &pipe : pipes) {
        ASSERT_TRUE(pipe.write_ready_byte());
    }

    for (int expected = 1; expected <= static_cast<int>(pipes.size()); ++expected) {
        EXPECT_TRUE(reactor->poll(std::chrono::nanoseconds(0)));
        EXPECT_EQ(total_reactor_budget_callbacks(pipes), expected);
    }

    for (const ReactorBudgetPipe &pipe : pipes) {
        EXPECT_EQ(pipe.callbacks, 1);
        EXPECT_NE(pipe.events & af::reactor_readable, 0U);
    }
    EXPECT_FALSE(reactor->poll(std::chrono::nanoseconds(0)));

    for (ReactorBudgetPipe &pipe : pipes) {
        EXPECT_TRUE(reactor->del(&pipe.source));
    }
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

TEST(RuntimeConfigTests, ReactorEventBudgetLimitsSelectDispatch) {
    run_reactor_event_budget_test(af::reactor_backend::select);
}

TEST(RuntimeConfigTests, ReactorEventBudgetLimitsAutoBackendDispatch) {
    run_reactor_event_budget_test(af::reactor_backend::auto_select);
}

TEST(RuntimeConfigTests, ReactorEventBudgetLimitsEpollDispatch) {
    if (!af::supports_epoll) {
        GTEST_SKIP() << "epoll is not supported on this platform";
    }
    run_reactor_event_budget_test(af::reactor_backend::epoll);
}

TEST(RuntimeConfigTests, ReactorEventBudgetLimitsKqueueDispatch) {
    if (!af::supports_kqueue) {
        GTEST_SKIP() << "kqueue is not supported on this platform";
    }
    run_reactor_event_budget_test(af::reactor_backend::kqueue);
}
