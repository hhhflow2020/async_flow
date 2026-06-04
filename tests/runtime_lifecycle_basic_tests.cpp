#include "support/runtime_lifecycle_test_support.hpp"

#include <filesystem>
#include <string>
#include <utility>

#include <fcntl.h>

namespace af::test::runtime_lifecycle {

namespace {

[[nodiscard]] std::filesystem::path unique_temp_path(const char *name) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           (std::string{"asyncflow-"} + name + "-" + std::to_string(now));
}

[[nodiscard]] bool wait_until_service_consumed(const CountingServiceTask &service, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (service.consumed() < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

class RuntimeFileLifecycleTask final : public Task {
public:
    explicit RuntimeFileLifecycleTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(TestThread thread, std::string path, std::atomic<int> *completed,
               std::atomic<int> *status) {
        thread_ = thread;
        path_ = std::move(path);
        completed_ = completed;
        status_ = status;
        return schedule(thread);
    }

private:
    af::TaskResult run() override {
        IoOpState state{};
        int raw_fd = -1;
        IoStatus status = af::io_openat(*this, thread_, AT_FDCWD, path_.c_str(),
                                        O_CREAT | O_TRUNC | O_RDWR, 0600, &raw_fd, state);
        if (!expect_ready(status)) {
            return done();
        }

        UniqueFd fd(raw_fd);
        const char payload[] = "abc";
        status = af::io_write_some(*this, thread_, fd.get(), payload, 3, state);
        if (!expect_ready(status, 3)) {
            return done();
        }
        status = af::io_ftruncate(*this, thread_, fd.get(), 2, state);
        if (!expect_ready(status)) {
            return done();
        }
        status = af::io_fsync(*this, thread_, fd.get(), 0, state);
        if (!expect_ready(status)) {
            return done();
        }
        status = af::io_close(*this, thread_, fd, state);
        if (!expect_ready(status)) {
            return done();
        }

        status_->store(0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    [[nodiscard]] bool expect_ready(IoStatus status, std::size_t bytes = 0) noexcept {
        if (status.ready() && status.bytes == bytes) {
            return true;
        }
        status_->store(status.failed() ? status.error : EIO, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return false;
    }

    TestThread thread_{TestThreads::Logic_0};
    std::string path_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *status_{nullptr};
};

#if defined(__linux__)
class RuntimeOpenat2Task final : public Task {
public:
    explicit RuntimeOpenat2Task(Task::FactoryToken token) : Task(token) {}

    bool do_it(TestThread thread, std::string path, std::atomic<int> *completed,
               std::atomic<int> *status) {
        thread_ = thread;
        path_ = std::move(path);
        completed_ = completed;
        status_ = status;
        return schedule(thread);
    }

private:
    af::TaskResult run() override {
        IoOpState state{};
        int raw_fd = -1;
        open_how how{};
        how.flags = O_CREAT | O_TRUNC | O_RDWR;
        how.mode = 0600;

        const IoStatus status =
            af::io_openat2(*this, thread_, AT_FDCWD, path_.c_str(), &how, &raw_fd, state);
        if (!status.ready()) {
            status_->store(status.failed() ? status.error : EIO, std::memory_order_release);
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        UniqueFd fd(raw_fd);
        status_->store(0, std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    TestThread thread_{TestThreads::Logic_0};
    std::string path_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<int> *status_{nullptr};
};
#endif

} // namespace

TEST_F(RuntimeFixture, OneShotTaskRunsOnRequestedThread) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    ASSERT_TRUE(Runtime::start_task<OneShotTask>(TestThreads::Logic_2, &completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_2));
}

TEST_F(RuntimeFixture, ScheduleToAliasRunsOnRequestedThread) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    ASSERT_TRUE(
        Runtime::start_task<ScheduleToAliasTask>(TestThreads::Logic_2, &completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_2));
}

TEST_F(RuntimeFixture, PendingToAliasResumesOnRequestedThread) {
    std::atomic<int> completed{0};
    std::array<std::atomic<std::uint16_t>, 2> seen{};
    for (auto &value : seen) {
        value.store(Runtime::invalid_thread_index, std::memory_order_relaxed);
    }

    ASSERT_TRUE(Runtime::start_task<PendingToAliasTask>(&completed, &seen));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(seen[0].load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_0));
    EXPECT_EQ(seen[1].load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_1));
}

TEST_F(RuntimeFixture, FileLifecycleSyscallsRunOnTargetRuntimeThread) {
    const std::filesystem::path path = unique_temp_path("file-lifecycle");
    std::atomic<int> completed{0};
    std::atomic<int> status{EIO};

    ASSERT_TRUE(Runtime::start_task<RuntimeFileLifecycleTask>(TestThreads::Logic_0, path.string(),
                                                              &completed, &status));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(status.load(std::memory_order_acquire), 0);
    std::error_code ignored;
    EXPECT_EQ(std::filesystem::file_size(path, ignored), 2U);
    EXPECT_FALSE(ignored);
    std::filesystem::remove(path, ignored);
}

#if defined(__linux__)
TEST_F(RuntimeFixture, Openat2SyscallRunsOnTargetRuntimeThread) {
    const std::filesystem::path path = unique_temp_path("openat2");
    std::atomic<int> completed{0};
    std::atomic<int> status{EIO};

    ASSERT_TRUE(Runtime::start_task<RuntimeOpenat2Task>(TestThreads::Logic_0, path.string(),
                                                        &completed, &status));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    const int observed = status.load(std::memory_order_acquire);
    if (observed == ENOSYS || observed == EPERM) {
        GTEST_SKIP() << "openat2 is not available in this kernel/container";
    }
    EXPECT_EQ(observed, 0);
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
}
#endif

TEST_F(RuntimeFixture, MakeTaskSupportsCustomStartFunction) {
    std::atomic<int> completed{0};

    auto task = Runtime::make_task<ManualStartTask>();
    ASSERT_TRUE(task);
    EXPECT_FALSE(task.scheduled());
    ASSERT_TRUE(task->begin_on(TestThreads::Logic_3, &completed));
    EXPECT_TRUE(task.scheduled());
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, TryMakeTaskReturnsEmptyHandleWhenConstructorThrows) {
    auto failed = Runtime::try_make_task<TryMakeTask>(true);
    EXPECT_FALSE(failed);
    EXPECT_FALSE(failed.scheduled());
    Runtime::wait_for_idle();
    EXPECT_EQ(Runtime::unfinished_task_count(), 0U);
}

TEST_F(RuntimeFixture, TryMakeTaskSupportsCustomStartFunction) {
    std::atomic<int> completed{0};

    auto task = Runtime::try_make_task<TryMakeTask>(false);
    ASSERT_TRUE(task);
    EXPECT_FALSE(task.scheduled());
    ASSERT_TRUE(task->begin_on(TestThreads::Logic_3, &completed));
    EXPECT_TRUE(task.scheduled());
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, FastScheduleModeRequiresRuntimeProducer) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    auto task = Runtime::make_task<ScheduleModeStartTask>();
    ASSERT_TRUE(task);
    EXPECT_FALSE(task->begin_fast_on(TestThreads::Logic_0, &completed, &ran_on));
    EXPECT_FALSE(task.scheduled());
    Runtime::wait_for_idle();

    EXPECT_EQ(completed.load(std::memory_order_acquire), 0);
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::invalid_thread_index);
}

TEST_F(RuntimeFixture, FastScheduleModeAcceptsRuntimeProducer) {
    std::atomic<int> completed{0};
    std::atomic<int> result{0};
    std::array<std::atomic<std::uint16_t>, 2> seen{};
    for (auto &value : seen) {
        value.store(Runtime::invalid_thread_index, std::memory_order_relaxed);
    }

    ASSERT_TRUE(Runtime::start_task<RuntimeFastScheduleChildTask>(&completed, &result, &seen));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(result.load(std::memory_order_acquire), 1);
    EXPECT_EQ(seen[0].load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_0));
    EXPECT_EQ(seen[1].load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_1));
}

TEST_F(RuntimeFixture, OrderedScheduleModeAcceptsExternalProducer) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    auto task = Runtime::make_task<ScheduleModeStartTask>();
    ASSERT_TRUE(task);
    ASSERT_TRUE(task->begin_ordered_on(TestThreads::Logic_0, &completed, &ran_on));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_0));
}

TEST_F(RuntimeFixture, DelayedStartRunsOnRequestedThreadAfterDelay) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};
    std::atomic<std::int64_t> elapsed_ms{0};

    ASSERT_TRUE(Runtime::start_task<DelayedStartTask>(
        TestThreads::Logic_2, std::chrono::milliseconds(20), &completed, &ran_on, &elapsed_ms));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_2));
    EXPECT_GE(elapsed_ms.load(std::memory_order_acquire), 15);
}

TEST_F(RuntimeFixture, OrderedScheduleModeSurvivesPendingWakeRequest) {
    std::atomic<int> completed{0};
    std::array<std::atomic<std::uint16_t>, 2> seen{};
    for (auto &value : seen) {
        value.store(Runtime::invalid_thread_index, std::memory_order_relaxed);
    }

    ASSERT_TRUE(Runtime::start_task<OrderedPendingModeTask>(&completed, &seen));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(seen[0].load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_0));
    EXPECT_EQ(seen[1].load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_1));
}

TEST_F(RuntimeFixture, PendingAfterResumesOnRequestedThreadAfterDelay) {
    std::atomic<int> completed{0};
    std::array<std::atomic<std::uint16_t>, 2> seen{};
    std::atomic<std::int64_t> elapsed_ms{0};
    for (auto &value : seen) {
        value.store(Runtime::invalid_thread_index, std::memory_order_relaxed);
    }

    ASSERT_TRUE(Runtime::start_task<DelayedPendingTask>(std::chrono::milliseconds(20), &completed,
                                                        &seen, &elapsed_ms));
    ASSERT_TRUE(wait_until_at_least(completed, 1));

    EXPECT_EQ(seen[0].load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_0));
    EXPECT_EQ(seen[1].load(std::memory_order_acquire), Runtime::thread_index(TestThreads::DB_0));
    EXPECT_GE(elapsed_ms.load(std::memory_order_acquire), 15);
}

TEST_F(RuntimeFixture, ServiceTaskRunsWhenExecutorIsWoken) {
    CountingServiceTask service;
    std::atomic<int> completed{0};
    std::atomic<bool> ok{false};

    ASSERT_TRUE(Runtime::start_task<ServiceControlTask>(
        TestThreads::Logic_0, &service, ServiceControlTask::Operation::Register, &completed, &ok));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    ASSERT_TRUE(ok.load(std::memory_order_acquire));

    service.add_work(3);
    ASSERT_TRUE(Runtime::wake_service_tasks(TestThreads::Logic_0));
    ASSERT_TRUE(wait_until_service_consumed(service, 3));
    EXPECT_GE(service.runs(), 1);

    completed.store(0, std::memory_order_release);
    ok.store(false, std::memory_order_release);
    ASSERT_TRUE(Runtime::start_task<ServiceControlTask>(TestThreads::Logic_0, &service,
                                                        ServiceControlTask::Operation::Unregister,
                                                        &completed, &ok));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    ASSERT_TRUE(ok.load(std::memory_order_acquire));

    const int consumed_after_unregister = service.consumed();
    service.add_work(2);
    ASSERT_TRUE(Runtime::wake_service_tasks(TestThreads::Logic_0));

    completed.store(0, std::memory_order_release);
    ASSERT_TRUE(Runtime::start_task<ServiceControlTask>(TestThreads::Logic_0, &service,
                                                        ServiceControlTask::Operation::Barrier,
                                                        &completed, nullptr));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(service.consumed(), consumed_after_unregister);
}

TEST_F(RuntimeFixture, UnscheduledCreatedTaskIsDestroyedByHandle) {
    std::atomic<int> destroyed{0};

    {
        auto task = Runtime::make_task<UnscheduledTask>(&destroyed);
        ASSERT_TRUE(task);
        task->configure_without_schedule();
        EXPECT_FALSE(task.scheduled());
    }

    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST_F(RuntimeFixture, CreatedHandleKeepsCompletedTaskAliveUntilReset) {
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    {
        auto task = Runtime::make_task<TrackedDoneTask>(&destroyed);
        ASSERT_TRUE(task->do_it(&completed));
        ASSERT_TRUE(wait_until_at_least(completed, 1));
        Runtime::wait_for_idle();
        EXPECT_EQ(destroyed.load(std::memory_order_acquire), 0);
    }

    EXPECT_EQ(destroyed.load(std::memory_order_acquire), 1);
}

TEST_F(RuntimeFixture, FailedTaskIsReleased) {
    std::atomic<int> completed{0};

    ASSERT_TRUE(Runtime::start_task<FailTask>(&completed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
}

TEST_F(RuntimeFixture, CancelledTaskIsReleased) {
    std::atomic<int> completed{0};
    std::atomic<int> destroyed{0};

    ASSERT_TRUE(Runtime::start_task<CancelResultTask>(&completed, &destroyed));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    ASSERT_TRUE(wait_until_at_least(destroyed, 1));
}

TEST_F(RuntimeFixture, StateMachineCanHopThreadsAndAgainOnCurrentThread) {
    std::atomic<int> completed{0};
    std::array<std::atomic<std::uint16_t>, 4> seen{};
    for (auto &value : seen) {
        value.store(Runtime::invalid_thread_index, std::memory_order_relaxed);
    }

    ASSERT_TRUE(Runtime::start_task<HopTask>(&completed, &seen));
    ASSERT_TRUE(wait_until_at_least(completed, 1));
    EXPECT_EQ(seen[0].load(), Runtime::thread_index(TestThreads::Logic_0));
    EXPECT_EQ(seen[1].load(), Runtime::thread_index(TestThreads::DB_0));
    EXPECT_EQ(seen[2].load(), Runtime::thread_index(TestThreads::Logic_1));
    EXPECT_EQ(seen[3].load(), Runtime::thread_index(TestThreads::Logic_1));
}

TEST_F(RuntimeFixture, WaitForIdleReturnsAfterAcceptedTasksComplete) {
    std::atomic<int> completed{0};
    std::atomic<std::uint16_t> ran_on{Runtime::invalid_thread_index};

    ASSERT_TRUE(Runtime::start_task<OneShotTask>(TestThreads::Logic_1, &completed, &ran_on));
    Runtime::wait_for_idle();

    EXPECT_EQ(completed.load(std::memory_order_acquire), 1);
    EXPECT_EQ(ran_on.load(std::memory_order_acquire), Runtime::thread_index(TestThreads::Logic_1));
    EXPECT_EQ(Runtime::unfinished_task_count(), 0U);
}
} // namespace af::test::runtime_lifecycle
