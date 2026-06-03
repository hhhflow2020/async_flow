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

struct IoTestLogicThreadTag;
struct IoTestIoThreadTag;

inline constexpr auto io_test_threads =
    af::thread_layout(af::thread_group<IoTestLogicThreadTag, 1>(),
                      af::thread_group<IoTestIoThreadTag, 1, af::ThreadKind::Io>());

inline constexpr auto uring_io_test_threads =
    af::thread_layout(af::thread_group<IoTestLogicThreadTag, 1>(),
                      af::thread_group<IoTestIoThreadTag, 1, af::ThreadKind::IoUring>());

struct IoTestRuntimeTraits {
    static constexpr auto threads = io_test_threads;
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
};

using IoRuntime = af::AsyncRuntime<IoTestRuntimeTraits>;
using IoTaskBase = IoRuntime::Task;
using IoTestThread = IoRuntime::Thread;

struct IoTestThreads {
    static constexpr IoTestThread Logic_0 =
        IoRuntime::thread_group<IoTestLogicThreadTag>().template at<0>();
    static constexpr IoTestThread IO_0 =
        IoRuntime::thread_group<IoTestIoThreadTag>().template at<0>();
};

struct FastIoRuntimeTraits {
    static constexpr auto threads = io_test_threads;
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;
};

using FastIoRuntime = af::AsyncRuntime<FastIoRuntimeTraits>;
using FastIoTaskBase = FastIoRuntime::Task;

struct UringIoRuntimeTraits {
    static constexpr auto threads = uring_io_test_threads;
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy runtime_queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::QueueFullPolicy external_queue_full_policy = af::QueueFullPolicy::Yield;
};

using UringIoRuntime = af::AsyncRuntime<UringIoRuntimeTraits>;
using UringIoTaskBase = UringIoRuntime::Task;

struct TunedIoRuntimeTraits {
    static constexpr auto threads = uring_io_test_threads;
    static constexpr std::size_t spsc_queue_capacity = 2048;
    static constexpr std::size_t external_queue_capacity = 4096;
    static constexpr unsigned io_uring_entries = 512;
    static constexpr unsigned io_uring_submit_batch_threshold = 128;
    static constexpr unsigned io_uring_cq_entries = 1024;
    static constexpr bool io_uring_setup_sqpoll = true;
    static constexpr unsigned io_uring_sqpoll_idle_ms = 2500;
    static constexpr int io_uring_sqpoll_cpu = 0;
    static constexpr bool io_uring_setup_submit_all = true;
    static constexpr bool io_uring_setup_coop_taskrun = true;
    static constexpr bool io_uring_setup_single_issuer = true;
    static constexpr bool io_uring_setup_defer_taskrun = true;
    static constexpr std::size_t io_wait_reserve = 256;
    static constexpr std::size_t io_uring_provided_buffer_group_reserve = 8;
};

using TunedIoRuntime = af::AsyncRuntime<TunedIoRuntimeTraits>;
static_assert(TunedIoRuntime::spsc_queue_capacity == 2048U);
static_assert(TunedIoRuntime::external_queue_capacity == 4096U);
static_assert(TunedIoRuntime::io_uring_entries == 512U);
static_assert(TunedIoRuntime::io_uring_submit_batch_threshold == 128U);
static_assert(TunedIoRuntime::io_uring_cq_entries == 1024U);
static_assert(TunedIoRuntime::io_uring_setup_sqpoll);
static_assert(TunedIoRuntime::io_uring_sqpoll_idle_ms == 2500U);
static_assert(TunedIoRuntime::io_uring_sqpoll_cpu == 0);
static_assert(TunedIoRuntime::io_uring_setup_submit_all);
static_assert(TunedIoRuntime::io_uring_setup_coop_taskrun);
static_assert(TunedIoRuntime::io_uring_setup_single_issuer);
static_assert(TunedIoRuntime::io_uring_setup_defer_taskrun);
static_assert(TunedIoRuntime::io_wait_reserve == 256U);
static_assert(TunedIoRuntime::io_uring_provided_buffer_group_reserve == 8U);

class IoRuntimeFixture : public testing::Test {
protected:
    void SetUp() override {
        IoRuntime::init();
    }

    void TearDown() override {
        IoRuntime::shutdown();
    }
};

class UringIoRuntimeFixture : public testing::Test {
protected:
    void SetUp() override {
        UringIoRuntime::init();
    }

    void TearDown() override {
        UringIoRuntime::shutdown();
    }
};
