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

enum class UtilityThread : std::uint16_t {
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,
    enum_num_end,
};

struct UtilityRuntimeTraits {
    using Thread = UtilityThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(UtilityThread::enum_num_end);
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
