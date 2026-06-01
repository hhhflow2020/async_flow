#pragma once

template <typename T> bool wait_until_at_least(std::atomic<T> &value, T expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

struct TestLogicThreadTag;
struct TestDbThreadTag;

struct TestRuntimeTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<TestLogicThreadTag, 4>(),
                                                      af::thread_group<TestDbThreadTag, 1>());
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
};

using Runtime = af::AsyncRuntime<TestRuntimeTraits>;
using Task = Runtime::Task;
using TestThread = Runtime::Thread;

struct TestThreads {
    static constexpr TestThread Logic_0 =
        Runtime::thread_group<TestLogicThreadTag>().template at<0>();
    static constexpr TestThread Logic_1 =
        Runtime::thread_group<TestLogicThreadTag>().template at<1>();
    static constexpr TestThread Logic_2 =
        Runtime::thread_group<TestLogicThreadTag>().template at<2>();
    static constexpr TestThread Logic_3 =
        Runtime::thread_group<TestLogicThreadTag>().template at<3>();
    static constexpr TestThread DB_0 = Runtime::thread_group<TestDbThreadTag>().template at<0>();
};

class ParallelRuntimeFixture : public testing::Test {
protected:
    void SetUp() override {
        Runtime::init();
    }

    void TearDown() override {
        Runtime::shutdown();
    }
};
