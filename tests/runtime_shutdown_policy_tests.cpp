#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

#include "af/detail/runtime/atomic_wait.hpp"
#include "af/runtime.hpp"

namespace {

[[nodiscard]] af::runtime_config make_shutdown_runtime_config() {
    af::runtime_config config;
    config.threads = {
        af::cpu_threads("shutdown-logic", 1),
        af::cpu_threads("shutdown-db", 1),
    };
    config.logger.consumer_thread = af::thread_selector::cpu(0);
    config.diagnostics.enable_thread_name = false;
    return config;
}

template <typename T>
[[nodiscard]] bool wait_until_at_least(const std::atomic<T> &value, T expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

[[nodiscard]] bool wait_until_state(const af::runtime &runtime, af::runtime_state expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (runtime.state() != expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

template <typename TaskT, typename... Args>
[[nodiscard]] bool start_task(af::runtime &runtime, Args &&...args) {
    auto task = af::make_task<TaskT>(runtime);
    if (!task->do_it(std::forward<Args>(args)...)) {
        return false;
    }
    return true;
}

class failed_schedule_task final : public af::runtime_task {
public:
    failed_schedule_task(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref thread, std::atomic<int> &destroyed) noexcept {
        destroyed_ = &destroyed;
        return schedule_to(thread);
    }

    ~failed_schedule_task() override {
        if (destroyed_ != nullptr) {
            destroyed_->fetch_add(1, std::memory_order_release);
        }
    }

private:
    af::task_result run_task() noexcept override {
        return failed();
    }

    std::atomic<int> *destroyed_{nullptr};
};

class blocking_task final : public af::runtime_task {
public:
    blocking_task(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref thread, std::atomic<int> &started,
                             std::atomic<bool> &release, std::atomic<int> &completed) noexcept {
        started_ = &started;
        release_ = &release;
        completed_ = &completed;
        return schedule_to(thread);
    }

private:
    af::task_result run_task() noexcept override {
        started_->fetch_add(1, std::memory_order_release);
        af::detail::atomic_notify_one(*started_);
        while (!release_->load(std::memory_order_acquire)) {
            af::detail::atomic_wait_value(*release_, false, std::memory_order_acquire);
        }
        completed_->fetch_add(1, std::memory_order_release);
        af::detail::atomic_notify_one(*completed_);
        return done();
    }

    std::atomic<int> *started_{nullptr};
    std::atomic<bool> *release_{nullptr};
    std::atomic<int> *completed_{nullptr};
};

class hop_during_stop_task final : public af::runtime_task {
public:
    hop_during_stop_task(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref logic_thread, af::thread_ref db_thread,
                             std::atomic<int> &started, std::atomic<bool> &release,
                             std::atomic<int> &completed,
                             std::array<std::atomic<std::uint16_t>, 2> &seen) noexcept {
        logic_thread_ = logic_thread;
        db_thread_ = db_thread;
        started_ = &started;
        release_ = &release;
        completed_ = &completed;
        seen_ = &seen;
        return schedule_to(logic_thread_);
    }

private:
    enum class state : std::uint8_t {
        start,
        db,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::start:
            (*seen_)[0].store(af::runtime::current_thread_index(), std::memory_order_release);
            started_->fetch_add(1, std::memory_order_release);
            af::detail::atomic_notify_one(*started_);
            while (!release_->load(std::memory_order_acquire)) {
                af::detail::atomic_wait_value(*release_, false, std::memory_order_acquire);
            }
            state_ = state::db;
            return pending_to(db_thread_);

        case state::db:
            (*seen_)[1].store(af::runtime::current_thread_index(), std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            af::detail::atomic_notify_one(*completed_);
            return done();
        }
        return failed();
    }

    af::thread_ref logic_thread_;
    af::thread_ref db_thread_;
    state state_{state::start};
    std::atomic<int> *started_{nullptr};
    std::atomic<bool> *release_{nullptr};
    std::atomic<int> *completed_{nullptr};
    std::array<std::atomic<std::uint16_t>, 2> *seen_{nullptr};
};

class pending_task final : public af::runtime_task {
public:
    pending_task(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref thread, std::atomic<int> &entered,
                             std::atomic<int> &destroyed) noexcept {
        entered_ = &entered;
        destroyed_ = &destroyed;
        return schedule_to(thread);
    }

    ~pending_task() override {
        if (destroyed_ != nullptr) {
            destroyed_->fetch_add(1, std::memory_order_release);
        }
    }

private:
    af::task_result run_task() noexcept override {
        entered_->fetch_add(1, std::memory_order_release);
        af::detail::atomic_notify_one(*entered_);
        return pending();
    }

    std::atomic<int> *entered_{nullptr};
    std::atomic<int> *destroyed_{nullptr};
};

class delayed_task final : public af::runtime_task {
public:
    delayed_task(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(af::thread_ref thread, std::atomic<int> &destroyed) noexcept {
        destroyed_ = &destroyed;
        return schedule_after(thread, std::chrono::hours(1));
    }

    ~delayed_task() override {
        if (destroyed_ != nullptr) {
            destroyed_->fetch_add(1, std::memory_order_release);
        }
    }

private:
    af::task_result run_task() noexcept override {
        return failed();
    }

    std::atomic<int> *destroyed_{nullptr};
};

static_assert(!std::is_default_constructible_v<failed_schedule_task>);

} // namespace

TEST(RuntimeShutdownTests, StartTaskFailsAndDestroysTaskWhenRuntimeIsNotStarted) {
    af::runtime runtime(make_shutdown_runtime_config());
    const af::thread_ref logic_thread = runtime.thread_group("shutdown-logic").front();
    ASSERT_TRUE(logic_thread);

    std::atomic<int> destroyed{0};
    EXPECT_FALSE(start_task<failed_schedule_task>(runtime, logic_thread, destroyed));
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, MakeTaskDestroysRawTaskWhenScheduleFailsBeforeStart) {
    af::runtime runtime(make_shutdown_runtime_config());
    const af::thread_ref logic_thread = runtime.thread_group("shutdown-logic").front();
    ASSERT_TRUE(logic_thread);

    std::atomic<int> destroyed{0};
    {
        auto task = af::make_task<failed_schedule_task>(runtime);
        EXPECT_FALSE(task->do_it(logic_thread, destroyed));
    }
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, StopBlocksUntilRunningTaskCompletes) {
    af::runtime runtime(make_shutdown_runtime_config());
    const af::thread_ref logic_thread = runtime.thread_group("shutdown-logic").front();
    ASSERT_TRUE(logic_thread);
    ASSERT_TRUE(runtime.start());

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<bool> shutdown_done{false};

    ASSERT_TRUE(start_task<blocking_task>(runtime, logic_thread, started, release, completed));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    std::thread shutdown_thread([&] {
        runtime.stop();
        shutdown_done.store(true, std::memory_order_release);
    });

    ASSERT_TRUE(wait_until_state(runtime, af::runtime_state::stopping));
    EXPECT_FALSE(shutdown_done.load(std::memory_order_acquire));

    release.store(true, std::memory_order_release);
    af::detail::atomic_notify_one(release);
    shutdown_thread.join();

    EXPECT_TRUE(shutdown_done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
    EXPECT_EQ(runtime.state(), af::runtime_state::stopped);
}

TEST(RuntimeShutdownTests, StopAllowsRuntimeThreadRescheduleWhileStopping) {
    af::runtime runtime(make_shutdown_runtime_config());
    const af::thread_ref logic_thread = runtime.thread_group("shutdown-logic").front();
    const af::thread_ref db_thread = runtime.thread_group("shutdown-db").front();
    ASSERT_TRUE(logic_thread);
    ASSERT_TRUE(db_thread);
    ASSERT_TRUE(runtime.start());

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<bool> shutdown_done{false};
    std::array<std::atomic<std::uint16_t>, 2> seen{};
    for (auto &value : seen) {
        value.store(af::runtime_invalid_thread_index, std::memory_order_relaxed);
    }

    ASSERT_TRUE(start_task<hop_during_stop_task>(runtime, logic_thread, db_thread, started, release,
                                                 completed, seen));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    std::thread shutdown_thread([&] {
        runtime.stop();
        shutdown_done.store(true, std::memory_order_release);
    });

    ASSERT_TRUE(wait_until_state(runtime, af::runtime_state::stopping));
    EXPECT_FALSE(shutdown_done.load(std::memory_order_acquire));

    release.store(true, std::memory_order_release);
    af::detail::atomic_notify_one(release);
    shutdown_thread.join();

    EXPECT_TRUE(shutdown_done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
    EXPECT_EQ(seen[0].load(std::memory_order_acquire), logic_thread.index);
    EXPECT_EQ(seen[1].load(std::memory_order_acquire), db_thread.index);
    EXPECT_EQ(runtime.state(), af::runtime_state::stopped);
}

TEST(RuntimeShutdownTests, StopRejectsExternalStartsWhileStopping) {
    af::runtime runtime(make_shutdown_runtime_config());
    const af::thread_ref logic_thread = runtime.thread_group("shutdown-logic").front();
    ASSERT_TRUE(logic_thread);
    ASSERT_TRUE(runtime.start());

    std::atomic<int> started{0};
    std::atomic<bool> release{false};
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};
    std::atomic<bool> shutdown_done{false};

    ASSERT_TRUE(start_task<blocking_task>(runtime, logic_thread, started, release, completed));
    ASSERT_TRUE(wait_until_at_least(started, 1));

    std::thread shutdown_thread([&] {
        runtime.stop();
        shutdown_done.store(true, std::memory_order_release);
    });

    ASSERT_TRUE(wait_until_state(runtime, af::runtime_state::stopping));
    EXPECT_FALSE(start_task<failed_schedule_task>(runtime, logic_thread, destroyed));
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
    EXPECT_FALSE(shutdown_done.load(std::memory_order_acquire));

    release.store(true, std::memory_order_release);
    af::detail::atomic_notify_one(release);
    shutdown_thread.join();

    EXPECT_TRUE(shutdown_done.load(std::memory_order_acquire));
    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
}

TEST(RuntimeShutdownTests, PendingTaskWithoutHandleIsReleased) {
    af::runtime runtime(make_shutdown_runtime_config());
    const af::thread_ref logic_thread = runtime.thread_group("shutdown-logic").front();
    ASSERT_TRUE(logic_thread);
    ASSERT_TRUE(runtime.start());

    std::atomic<int> entered{0};
    std::atomic<int> destroyed{0};
    ASSERT_TRUE(start_task<pending_task>(runtime, logic_thread, entered, destroyed));
    ASSERT_TRUE(wait_until_at_least(entered, 1));
    ASSERT_TRUE(wait_until_at_least(destroyed, 1));

    runtime.stop();
    EXPECT_EQ(runtime.state(), af::runtime_state::stopped);
}

TEST(RuntimeShutdownTests, StopCancelsDelayedTaskAndDestroysIt) {
    af::runtime runtime(make_shutdown_runtime_config());
    const af::thread_ref logic_thread = runtime.thread_group("shutdown-logic").front();
    ASSERT_TRUE(logic_thread);
    ASSERT_TRUE(runtime.start());

    std::atomic<int> destroyed{0};
    ASSERT_TRUE(start_task<delayed_task>(runtime, logic_thread, destroyed));

    runtime.stop();
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
    EXPECT_EQ(runtime.state(), af::runtime_state::stopped);
}
