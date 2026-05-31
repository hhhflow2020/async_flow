#if !defined(AF_RUNTIME_PARALLEL_TEST_SUPPORT_FRAGMENT_INCLUDE)
#error "runtime_parallel_core_support_fragment.hpp is a runtime parallel test support fragment"
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

enum class TestThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    DB_0,
    enum_thread_index_end,
};

struct TestRuntimeTraits {
    using Thread = TestThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(TestThread::enum_thread_index_end);
    static constexpr std::size_t spsc_queue_capacity = 1024;
    static constexpr std::size_t external_queue_capacity = 1024;
    static constexpr af::QueueFullPolicy queue_full_policy = af::QueueFullPolicy::Reject;
};

using Runtime = af::AsyncRuntime<TestRuntimeTraits>;
using Task = Runtime::Task;

class ParallelRuntimeFixture : public testing::Test {
protected:
    void SetUp() override {
        Runtime::init();
    }

    void TearDown() override {
        Runtime::shutdown();
    }
};
