#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "af/runtime.hpp"

namespace {

[[nodiscard]] af::runtime_config make_ordered_runtime_config() {
    af::runtime_config config;
    config.threads = {
        af::cpu_threads("logic", 4),
        af::io_threads("io", 1),
    };
    return config;
}

[[nodiscard]] bool wait_for_counter(std::atomic<int> &counter, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (counter.load(std::memory_order_acquire) >= expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return counter.load(std::memory_order_acquire) >= expected;
}

class InstanceOrderedBatchTask final : public af::runtime_task {
public:
    static constexpr std::uint16_t no_fail_shard = UINT16_MAX;

    InstanceOrderedBatchTask(factory_token token, af::runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] bool do_group(af::thread_group_ref logic_threads, std::uint64_t batch_id,
                                std::atomic<int> &completed,
                                std::array<std::atomic<int>, 4> *shard_hits,
                                std::array<std::atomic<std::uint64_t>, 4> *batch_seen,
                                std::atomic<std::uint32_t> *failures = nullptr,
                                std::uint16_t fail_shard = no_fail_shard,
                                af::ordered_batch_options options = {}) {
        logic_threads_ = logic_threads;
        use_begin_thread_ = false;
        return configure(batch_id, completed, shard_hits, batch_seen, failures, fail_shard,
                         options);
    }

    [[nodiscard]] bool do_begin(af::thread_ref first_thread, std::uint64_t batch_id,
                                std::atomic<int> &completed,
                                std::array<std::atomic<int>, 4> *shard_hits,
                                std::array<std::atomic<std::uint64_t>, 4> *batch_seen,
                                std::atomic<std::uint32_t> *failures = nullptr,
                                std::uint16_t fail_shard = no_fail_shard,
                                af::ordered_batch_options options = {}) {
        first_thread_ = first_thread;
        use_begin_thread_ = true;
        return configure(batch_id, completed, shard_hits, batch_seen, failures, fail_shard,
                         options);
    }

private:
    enum class state : std::uint8_t {
        apply,
        finish,
    };

    [[nodiscard]] bool configure(std::uint64_t batch_id, std::atomic<int> &completed,
                                 std::array<std::atomic<int>, 4> *shard_hits,
                                 std::array<std::atomic<std::uint64_t>, 4> *batch_seen,
                                 std::atomic<std::uint32_t> *failures, std::uint16_t fail_shard,
                                 af::ordered_batch_options options) {
        batch_id_ = batch_id;
        completed_ = &completed;
        shard_hits_ = shard_hits;
        batch_seen_ = batch_seen;
        failures_ = failures;
        fail_shard_ = fail_shard;
        options_ = options;

        ops_ = af::sharded_ops<int>(4);
        ops_.shards[0] = {7};
        return schedule_to(use_begin_thread_ ? first_thread_ : logic_threads_.front());
    }

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::apply:
            state_ = state::finish;
            if (use_begin_thread_) {
                return owner().parallel_shards_ordered(
                           first_thread_, ops_, batch_id_, options_, this,
                           [this](std::uint16_t shard, std::vector<int> &, std::uint64_t batch_id) {
                               return handle_shard(shard, batch_id);
                           })
                           ? pending()
                           : failed();
            }
            return owner().parallel_shards_ordered(
                       logic_threads_, ops_, batch_id_, options_, this,
                       [this](std::uint16_t shard, std::vector<int> &, std::uint64_t batch_id) {
                           return handle_shard(shard, batch_id);
                       })
                       ? pending()
                       : failed();

        case state::finish:
            if (failures_ != nullptr) {
                failures_->store(last_parallel_failures(), std::memory_order_release);
            }
            completed_->fetch_add(1, std::memory_order_release);
            return done();
        }

        return failed();
    }

    [[nodiscard]] bool handle_shard(std::uint16_t shard, std::uint64_t batch_id) noexcept {
        if (shard_hits_ != nullptr) {
            (*shard_hits_)[shard].fetch_add(1, std::memory_order_release);
        }
        if (batch_seen_ != nullptr) {
            (*batch_seen_)[shard].store(batch_id, std::memory_order_release);
        }
        return fail_shard_ == no_fail_shard || shard != fail_shard_;
    }

    state state_{state::apply};
    af::thread_group_ref logic_threads_;
    af::thread_ref first_thread_;
    bool use_begin_thread_{false};
    std::uint64_t batch_id_{0};
    af::ordered_batch_options options_;
    af::sharded_ops<int> ops_{4};
    std::atomic<int> *completed_{nullptr};
    std::array<std::atomic<int>, 4> *shard_hits_{nullptr};
    std::array<std::atomic<std::uint64_t>, 4> *batch_seen_{nullptr};
    std::atomic<std::uint32_t> *failures_{nullptr};
    std::uint16_t fail_shard_{no_fail_shard};
};

} // namespace

TEST(RuntimeInstanceOrderedBatchTests, RunsEveryShardAndAcceptsContiguousBatches) {
    af::runtime runtime(make_ordered_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    ASSERT_EQ(logic_threads.size(), 4U);

    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};
    std::array<std::atomic<std::uint64_t>, 4> batch_seen{};

    auto first = af::make_task<InstanceOrderedBatchTask>(runtime);
    ASSERT_TRUE(first->do_group(logic_threads, 1U, completed, &shard_hits, &batch_seen));
    ASSERT_TRUE(wait_for_counter(completed, 1));
    for (std::uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(shard_hits[i].load(), 1);
        EXPECT_EQ(batch_seen[i].load(), 1U);
    }

    auto second = af::make_task<InstanceOrderedBatchTask>(runtime);
    ASSERT_TRUE(second->do_group(logic_threads, 2U, completed, &shard_hits, &batch_seen));
    ASSERT_TRUE(wait_for_counter(completed, 2));

    for (std::uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(shard_hits[i].load(), 2);
        EXPECT_EQ(batch_seen[i].load(), 2U);
        EXPECT_EQ(runtime.ordered_last_applied_batch_id(logic_threads.at(i)), 2U);
    }
    runtime.stop();
}

TEST(RuntimeInstanceOrderedBatchTests, ThreadBeginOverloadRunsAllShards) {
    af::runtime runtime(make_ordered_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    ASSERT_EQ(logic_threads.size(), 4U);

    std::atomic<int> completed{0};
    std::array<std::atomic<int>, 4> shard_hits{};

    auto task = af::make_task<InstanceOrderedBatchTask>(runtime);
    ASSERT_TRUE(task->do_begin(logic_threads.front(), 1U, completed, &shard_hits, nullptr));

    ASSERT_TRUE(wait_for_counter(completed, 1));

    for (std::uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(shard_hits[i].load(std::memory_order_acquire), 1);
        EXPECT_EQ(runtime.ordered_last_applied_batch_id(logic_threads.at(i)), 1U);
    }
    runtime.stop();
}

TEST(RuntimeInstanceOrderedBatchTests, FailureDoesNotAdvanceFailedShard) {
    af::runtime runtime(make_ordered_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    ASSERT_EQ(logic_threads.size(), 4U);

    std::atomic<int> completed{0};
    std::atomic<std::uint32_t> failures{0};

    auto task = af::make_task<InstanceOrderedBatchTask>(runtime);
    ASSERT_TRUE(task->do_group(logic_threads, 1U, completed, nullptr, nullptr, &failures, 1U));

    ASSERT_TRUE(wait_for_counter(completed, 1));

    EXPECT_EQ(failures.load(std::memory_order_acquire), 1U);
    EXPECT_EQ(runtime.ordered_last_applied_batch_id(logic_threads.at(0)), 1U);
    EXPECT_EQ(runtime.ordered_last_applied_batch_id(logic_threads.at(1)), 0U);
    EXPECT_EQ(runtime.ordered_last_applied_batch_id(logic_threads.at(2)), 1U);
    EXPECT_EQ(runtime.ordered_last_applied_batch_id(logic_threads.at(3)), 1U);
    runtime.stop();
}

TEST(RuntimeInstanceOrderedBatchTests, RetryableBatchSkipsAlreadyAppliedShards) {
    af::runtime runtime(make_ordered_runtime_config());
    ASSERT_TRUE(runtime.start());
    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    ASSERT_EQ(logic_threads.size(), 4U);

    std::atomic<int> failed_completed{0};
    std::atomic<std::uint32_t> failed_failures{0};
    auto failed = af::make_task<InstanceOrderedBatchTask>(runtime);
    ASSERT_TRUE(failed->do_group(logic_threads, 1U, failed_completed, nullptr, nullptr,
                                 &failed_failures, 1U));
    ASSERT_TRUE(wait_for_counter(failed_completed, 1));
    ASSERT_EQ(failed_failures.load(std::memory_order_acquire), 1U);

    std::atomic<int> retry_completed{0};
    std::atomic<std::uint32_t> retry_failures{0};
    std::array<std::atomic<int>, 4> shard_hits{};
    auto retry = af::make_task<InstanceOrderedBatchTask>(runtime);
    ASSERT_TRUE(retry->do_group(logic_threads, 1U, retry_completed, &shard_hits, nullptr,
                                &retry_failures, InstanceOrderedBatchTask::no_fail_shard,
                                af::retryable_ordered_batch_options));

    ASSERT_TRUE(wait_for_counter(retry_completed, 1));

    EXPECT_EQ(retry_failures.load(std::memory_order_acquire), 0U);
    EXPECT_EQ(shard_hits[0].load(std::memory_order_acquire), 0);
    EXPECT_EQ(shard_hits[1].load(std::memory_order_acquire), 1);
    EXPECT_EQ(shard_hits[2].load(std::memory_order_acquire), 0);
    EXPECT_EQ(shard_hits[3].load(std::memory_order_acquire), 0);
    for (std::uint16_t i = 0; i < 4; ++i) {
        EXPECT_EQ(runtime.ordered_last_applied_batch_id(logic_threads.at(i)), 1U);
    }
    runtime.stop();
}
