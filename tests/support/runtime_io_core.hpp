#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_io_core.hpp is a runtime_io_test_support implementation detail"
#endif

template <typename T>
bool wait_until_at_least(std::atomic<T>& value, T expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (value.load(std::memory_order_acquire) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

enum class IoTestThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    IO_0,
    enum_thread_index_end,
};

struct IoTestRuntimeTraits {
    using Thread = IoTestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(IoTestThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(IoTestThread thread) noexcept {
        return thread == IoTestThread::IO_0 ? af::ThreadKind::Io : af::ThreadKind::Worker;
    }
};

using IoRuntime = af::AsyncRuntime<IoTestRuntimeTraits>;
using IoTaskBase = IoRuntime::Task;

struct FastIoRuntimeTraits {
    using Thread = IoTestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(IoTestThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;
    static constexpr af::ShutdownPolicy shutdown_policy = af::ShutdownPolicy::StopImmediately;
    static constexpr bool enable_task_registry = true;

    static constexpr af::ThreadKind thread_kind(IoTestThread thread) noexcept {
        return thread == IoTestThread::IO_0 ? af::ThreadKind::Io : af::ThreadKind::Worker;
    }
};

using FastIoRuntime = af::AsyncRuntime<FastIoRuntimeTraits>;
using FastIoTaskBase = FastIoRuntime::Task;

struct UringIoRuntimeTraits {
    using Thread = IoTestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(IoTestThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Yield;

    static constexpr af::ThreadKind thread_kind(IoTestThread thread) noexcept {
        return thread == IoTestThread::IO_0 ? af::ThreadKind::IoUring : af::ThreadKind::Worker;
    }
};

using UringIoRuntime = af::AsyncRuntime<UringIoRuntimeTraits>;
using UringIoTaskBase = UringIoRuntime::Task;

struct TunedIoRuntimeTraits {
    using Thread = IoTestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(IoTestThread::enum_thread_index_end);
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
