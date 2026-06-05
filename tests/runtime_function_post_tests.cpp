#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include "af/runtime.hpp"
#include "af/runtime_config.hpp"

namespace {

[[nodiscard]] bool wait_for_counter(const std::atomic<int> &counter, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (counter.load(std::memory_order_acquire) == expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return counter.load(std::memory_order_acquire) == expected;
}

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

class MoveOnlyCallback {
public:
    MoveOnlyCallback(std::atomic<int> &runs, std::atomic<int> &destroys,
                     std::atomic<std::uint16_t> &thread)
        : runs_(&runs), destroys_(&destroys), thread_(&thread) {}

    MoveOnlyCallback(const MoveOnlyCallback &) = delete;
    MoveOnlyCallback &operator=(const MoveOnlyCallback &) = delete;

    MoveOnlyCallback(MoveOnlyCallback &&other) noexcept
        : runs_(other.runs_), destroys_(other.destroys_), thread_(other.thread_) {
        other.runs_ = nullptr;
        other.destroys_ = nullptr;
        other.thread_ = nullptr;
    }

    MoveOnlyCallback &operator=(MoveOnlyCallback &&) = delete;

    ~MoveOnlyCallback() {
        if (destroys_ != nullptr) {
            destroys_->fetch_add(1, std::memory_order_release);
        }
    }

    void operator()(af::runtime &) noexcept {
        thread_->store(af::runtime::current_thread_index(), std::memory_order_release);
        runs_->fetch_add(1, std::memory_order_release);
    }

private:
    std::atomic<int> *runs_{nullptr};
    std::atomic<int> *destroys_{nullptr};
    std::atomic<std::uint16_t> *thread_{nullptr};
};

} // namespace

TEST(RuntimeFunctionPostTests, ExternalCallableRunsOnTargetThread) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 2)};
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 2));

    std::atomic<int> runs{0};
    std::atomic<std::uint16_t> observed{af::runtime_invalid_thread_index};
    const af::thread_ref target = runtime.cpu_threads().at(1);

    ASSERT_TRUE(runtime.post(target, [&runs, &observed](af::runtime &owner) noexcept {
        observed.store(af::runtime::current_thread_index(), std::memory_order_release);
        EXPECT_EQ(af::runtime::current(), &owner);
        runs.fetch_add(1, std::memory_order_release);
    }));

    ASSERT_TRUE(wait_for_counter(runs, 1));
    EXPECT_EQ(observed.load(std::memory_order_acquire), target.index);
    runtime.stop();
}

TEST(RuntimeFunctionPostTests, MoveOnlyCallableIsDestroyedAfterRun) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    ASSERT_TRUE(runtime.start());
    ASSERT_TRUE(wait_for_active_threads(runtime, 1));

    std::atomic<int> runs{0};
    std::atomic<int> destroys{0};
    std::atomic<std::uint16_t> observed{af::runtime_invalid_thread_index};
    const af::thread_ref target = runtime.cpu_threads().front();

    ASSERT_TRUE(runtime.post(target, MoveOnlyCallback(runs, destroys, observed)));
    ASSERT_TRUE(wait_for_counter(runs, 1));
    ASSERT_TRUE(wait_for_counter(destroys, 1));
    EXPECT_EQ(observed.load(std::memory_order_acquire), target.index);
    runtime.stop();
}

TEST(RuntimeFunctionPostTests, FailedPostDestroysCallable) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("logic", 1)};
    config.logger.consumer_thread = af::thread_selector::cpu(0);

    af::runtime runtime(config);
    const af::thread_ref target = runtime.cpu_threads().front();

    std::atomic<int> runs{0};
    std::atomic<int> destroys{0};
    std::atomic<std::uint16_t> observed{af::runtime_invalid_thread_index};

    EXPECT_FALSE(runtime.post(target, MoveOnlyCallback(runs, destroys, observed)));
    EXPECT_EQ(runs.load(std::memory_order_acquire), 0);
    EXPECT_EQ(destroys.load(std::memory_order_acquire), 1);
}
