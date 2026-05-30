#include <array>
#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "af/async_flow.hpp"
#include "af/detail/bounded_queues.hpp"
#include "af/detail/object_pool.hpp"

namespace {

enum class UtilityThread : std::int16_t {
    enum_thread_index_start = -1,
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    enum_thread_index_end,
};

struct UtilityRuntimeTraits {
    using Thread = UtilityThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(UtilityThread::enum_thread_index_end);
};

using Runtime = af::AsyncRuntime<UtilityRuntimeTraits>;

} // namespace

TEST(QueueTests, BoundedSpscPreservesFifoAndRejectsWhenFull) {
    af::detail::BoundedSpscQueue<int> queue(2);
    int a = 1;
    int b = 2;
    int c = 3;

    EXPECT_TRUE(queue.try_push(&a));
    EXPECT_TRUE(queue.try_push(&b));
    EXPECT_FALSE(queue.try_push(&c));
    EXPECT_EQ(queue.try_pop(), &a);
    EXPECT_EQ(queue.try_pop(), &b);
    EXPECT_EQ(queue.try_pop(), nullptr);
    EXPECT_TRUE(queue.try_push(&c));
    EXPECT_EQ(queue.try_pop(), &c);
}

TEST(QueueTests, BoundedMpscRejectsWhenFull) {
    af::detail::BoundedMpscQueue<int> queue(2);
    int a = 1;
    int b = 2;
    int c = 3;

    EXPECT_TRUE(queue.try_push(&a));
    EXPECT_TRUE(queue.try_push(&b));
    EXPECT_FALSE(queue.try_push(&c));
    EXPECT_EQ(queue.try_pop(), &a);
    EXPECT_EQ(queue.try_pop(), &b);
    EXPECT_EQ(queue.try_pop(), nullptr);
}

TEST(QueueTests, BoundedMpscSupportsConcurrentProducers) {
    constexpr int producer_count = 4;
    constexpr int values_per_producer = 64;
    constexpr int total_values = producer_count * values_per_producer;

    af::detail::BoundedMpscQueue<int> queue(128);
    std::array<std::array<int, values_per_producer>, producer_count> values{};
    std::array<std::thread, producer_count> producers;
    std::atomic<int> pushed{0};

    for (int producer = 0; producer < producer_count; ++producer) {
        for (int i = 0; i < values_per_producer; ++i) {
            values[producer][i] = producer * values_per_producer + i;
        }

        producers[producer] = std::thread([producer, &queue, &values, &pushed] {
            for (int i = 0; i < values_per_producer; ++i) {
                while (!queue.try_push(&values[producer][i])) {
                    std::this_thread::yield();
                }
                pushed.fetch_add(1, std::memory_order_release);
            }
        });
    }

    int popped = 0;
    while (popped < total_values) {
        if (queue.try_pop() != nullptr) {
            ++popped;
        } else {
            std::this_thread::yield();
        }
    }

    for (auto& producer : producers) {
        producer.join();
    }

    EXPECT_EQ(pushed.load(std::memory_order_acquire), total_values);
    EXPECT_EQ(queue.try_pop(), nullptr);
}

TEST(QueueTests, BoundedMpmcRejectsWhenFull) {
    af::detail::BoundedMpmcQueue<int> queue(2);
    int a = 1;
    int b = 2;
    int c = 3;

    EXPECT_TRUE(queue.try_push(&a));
    EXPECT_TRUE(queue.try_push(&b));
    EXPECT_FALSE(queue.try_push(&c));
    EXPECT_EQ(queue.try_pop(), &a);
    EXPECT_EQ(queue.try_pop(), &b);
    EXPECT_EQ(queue.try_pop(), nullptr);
}

TEST(PoolTests, ObjectPoolReusesReleasedStorage) {
    struct Payload {
        int value{0};
    };

    af::detail::ObjectPool<Payload, 1> pool;
    Payload* first = pool.create();
    first->value = 42;
    pool.destroy(first);

    Payload* second = pool.create();
    EXPECT_EQ(second, first);
    pool.destroy(second);
}

TEST(UtilityTests, IoOpStateResetClearsCompletionToken) {
    int token = 0;
    af::IoOpState state{};
    state.wait = af::IoResult{3, af::io_readable, 0, 16};
    state.wait.completion_token = &token;
    state.waiting = true;
    state.wait_kind = af::IoWaitKind::Completion;

    state.reset();

    EXPECT_EQ(state.wait.completion_token, nullptr);
    EXPECT_FALSE(state.waiting);
    EXPECT_EQ(state.wait_kind, af::IoWaitKind::None);
}

TEST(UtilityTests, SplitByShardGroupsByKey) {
    struct Op {
        std::uint64_t key;
        int value;
    };

    std::vector<Op> ops{{0, 10}, {1, 11}, {4, 14}, {6, 16}};
    auto sharded = Runtime::split_by_shard(std::move(ops), 4, [](const Op& op) {
        return op.key;
    });

    ASSERT_EQ(sharded.shard_count(), 4);
    ASSERT_EQ(sharded.shards[0].size(), 2);
    ASSERT_EQ(sharded.shards[1].size(), 1);
    ASSERT_EQ(sharded.shards[2].size(), 1);
    ASSERT_TRUE(sharded.shards[3].empty());
}

TEST(UtilityTests, SplitCrudOpsGroupsByKeyAndKeepsOperationData) {
    std::vector<af::CrudOp<std::uint64_t, int>> ops{
        {af::OpType::Add, 0, 10},
        {af::OpType::Update, 5, 15},
        {af::OpType::Delete, 2, 20},
    };

    auto sharded = af::split_crud_ops(std::move(ops), 4);

    ASSERT_EQ(sharded.shard_count(), 4);
    ASSERT_EQ(sharded.shards[0].size(), 1);
    ASSERT_EQ(sharded.shards[1].size(), 1);
    ASSERT_EQ(sharded.shards[2].size(), 1);
    EXPECT_EQ(sharded.shards[0][0].type, af::OpType::Add);
    EXPECT_EQ(sharded.shards[0][0].value, 10);
    EXPECT_EQ(sharded.shards[1][0].type, af::OpType::Update);
    EXPECT_EQ(sharded.shards[1][0].value, 15);
    EXPECT_EQ(sharded.shards[2][0].type, af::OpType::Delete);
    EXPECT_EQ(sharded.shards[2][0].value, 20);
}

TEST(UtilityTests, SplitChangeBatchSupportsCustomShardFunction) {
    af::ChangeBatch<std::uint64_t, int> batch{
        7,
        {
            {af::OpType::Add, 10, 1},
            {af::OpType::Update, 11, 2},
            {af::OpType::Delete, 12, 3},
        },
    };

    auto sharded = af::split_change_batch(
        batch,
        2,
        [](std::uint64_t key) {
            return key / 10U;
        });

    EXPECT_EQ(batch.batch_id, 7U);
    EXPECT_TRUE(batch.ops.empty());
    ASSERT_EQ(sharded.shard_count(), 2);
    EXPECT_TRUE(sharded.shards[0].empty());
    ASSERT_EQ(sharded.shards[1].size(), 3);
    EXPECT_EQ(sharded.shards[1][0].key, 10U);
    EXPECT_EQ(sharded.shards[1][1].key, 11U);
    EXPECT_EQ(sharded.shards[1][2].key, 12U);
}

TEST(UtilityTests, BatchSequencerBuffersOutOfOrderBatches) {
    af::BatchSequencer<int> sequencer(1);
    std::vector<int> submitted;

    auto submit = [&](int value) {
        submitted.push_back(value);
    };

    EXPECT_EQ(sequencer.submit(2, 20, submit), af::BatchSubmitStatus::Buffered);
    EXPECT_EQ(sequencer.submit(2, 200, submit), af::BatchSubmitStatus::Duplicate);
    EXPECT_TRUE(submitted.empty());
    EXPECT_EQ(sequencer.submit(1, 10, submit), af::BatchSubmitStatus::Submitted);
    ASSERT_EQ(submitted.size(), 2);
    EXPECT_EQ(submitted[0], 10);
    EXPECT_EQ(submitted[1], 20);
    EXPECT_EQ(sequencer.submit(1, 10, submit), af::BatchSubmitStatus::Duplicate);
}

TEST(UtilityTests, OrderedBatchRetrySkipPolicyTracksRetryAndSkipDecisions) {
    af::OrderedBatchRetrySkipPolicy<std::uint64_t> policy({
        .max_retries = 2,
        .skip_after_retries = true,
    });

    auto first = policy.record_failure(7U);
    EXPECT_TRUE(first.should_retry());
    EXPECT_EQ(first.failure_count, 1U);

    auto second = policy.record_failure(7U);
    EXPECT_TRUE(second.should_retry());
    EXPECT_EQ(second.failure_count, 2U);

    auto third = policy.record_failure(7U);
    EXPECT_TRUE(third.should_skip());
    EXPECT_EQ(third.failure_count, 3U);
    EXPECT_EQ(policy.failure_count(7U), 3U);

    policy.record_success(7U);
    EXPECT_EQ(policy.failure_count(7U), 0U);
    EXPECT_TRUE(policy.record_failure(7U).should_retry());
}

TEST(UtilityTests, OrderedBatchRetrySkipPolicyCanStopInsteadOfSkipping) {
    af::OrderedBatchRetrySkipPolicy<std::uint64_t> policy({
        .max_retries = 0,
        .skip_after_retries = false,
    });

    auto decision = policy.record_failure(9U);
    EXPECT_TRUE(decision.should_stop());
    EXPECT_EQ(decision.failure_count, 1U);
}
