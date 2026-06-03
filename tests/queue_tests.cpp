#include <array>
#include <atomic>
#include <limits>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

#include "af/detail/queue/bounded_queues.hpp"

TEST(QueueTests, BoundedQueueSequenceBeforeHandlesUnsignedWrapWindow) {
    constexpr std::size_t max = std::numeric_limits<std::size_t>::max();

    EXPECT_FALSE(af::detail::bounded_queue_sequence_before(10, 10));
    EXPECT_FALSE(af::detail::bounded_queue_sequence_before(11, 10));
    EXPECT_TRUE(af::detail::bounded_queue_sequence_before(9, 10));
    EXPECT_FALSE(af::detail::bounded_queue_sequence_before(0, max));
    EXPECT_TRUE(af::detail::bounded_queue_sequence_before(max, 0));
}

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

TEST(QueueTests, BoundedSpscPopsManyInFifoOrder) {
    af::detail::BoundedSpscQueue<int> queue(4);
    int a = 1;
    int b = 2;
    int c = 3;
    int d = 4;
    std::array<int *, 4> popped{};

    EXPECT_TRUE(queue.try_push(&a));
    EXPECT_TRUE(queue.try_push(&b));
    EXPECT_TRUE(queue.try_push(&c));

    EXPECT_EQ(queue.try_pop_many(popped.data(), 0), 0U);
    EXPECT_EQ(queue.try_pop_many(popped.data(), 2), 2U);
    EXPECT_EQ(popped[0], &a);
    EXPECT_EQ(popped[1], &b);

    EXPECT_TRUE(queue.try_push(&d));
    EXPECT_EQ(queue.try_pop_many(popped.data(), popped.size()), 2U);
    EXPECT_EQ(popped[0], &c);
    EXPECT_EQ(popped[1], &d);
    EXPECT_EQ(queue.try_pop_many(popped.data(), popped.size()), 0U);
}

TEST(QueueTests, BoundedSpscPushesManyInFifoOrder) {
    af::detail::BoundedSpscQueue<int> queue(4);
    int a = 1;
    int b = 2;
    int c = 3;
    int d = 4;
    std::array<int *, 3> first_push{&a, &b, &c};
    std::array<int *, 2> second_push{&d, &a};

    EXPECT_EQ(queue.try_push_many(first_push.data(), first_push.size()), first_push.size());
    EXPECT_EQ(queue.try_pop(), &a);
    EXPECT_EQ(queue.try_pop(), &b);

    EXPECT_EQ(queue.try_push_many(second_push.data(), second_push.size()), second_push.size());
    EXPECT_EQ(queue.try_pop(), &c);
    EXPECT_EQ(queue.try_pop(), &d);
    EXPECT_EQ(queue.try_pop(), &a);
    EXPECT_EQ(queue.try_pop(), nullptr);
}

TEST(QueueTests, BoundedSpscPushManyStopsWhenFull) {
    af::detail::BoundedSpscQueue<int> queue(2);
    int a = 1;
    int b = 2;
    int c = 3;
    std::array<int *, 3> values{&a, &b, &c};

    EXPECT_EQ(queue.try_push_many(values.data(), values.size()), 2U);
    EXPECT_FALSE(queue.try_push(&c));
    EXPECT_EQ(queue.try_pop(), &a);
    EXPECT_EQ(queue.try_pop(), &b);
    EXPECT_EQ(queue.try_pop(), nullptr);
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

TEST(QueueTests, BoundedMpscPopsManyInFifoOrder) {
    af::detail::BoundedMpscQueue<int> queue(4);
    int a = 1;
    int b = 2;
    int c = 3;
    int d = 4;
    std::array<int *, 4> popped{};

    EXPECT_TRUE(queue.try_push(&a));
    EXPECT_TRUE(queue.try_push(&b));
    EXPECT_TRUE(queue.try_push(&c));

    EXPECT_EQ(queue.try_pop_many(popped.data(), 0), 0U);
    EXPECT_EQ(queue.try_pop_many(popped.data(), 2), 2U);
    EXPECT_EQ(popped[0], &a);
    EXPECT_EQ(popped[1], &b);

    EXPECT_TRUE(queue.try_push(&d));
    EXPECT_EQ(queue.try_pop_many(popped.data(), popped.size()), 2U);
    EXPECT_EQ(popped[0], &c);
    EXPECT_EQ(popped[1], &d);
    EXPECT_EQ(queue.try_pop_many(popped.data(), popped.size()), 0U);
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

    for (auto &producer : producers) {
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

TEST(QueueTests, BoundedQueuesRejectOverflowingCapacities) {
    constexpr std::size_t too_large = std::numeric_limits<std::size_t>::max();

    EXPECT_THROW({ af::detail::BoundedSpscQueue<int> queue(too_large); }, std::length_error);
    EXPECT_THROW({ af::detail::BoundedMpscQueue<int> queue(too_large); }, std::length_error);
    EXPECT_THROW({ af::detail::BoundedMpmcQueue<int> queue(too_large); }, std::length_error);
}
