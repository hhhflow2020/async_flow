#include <array>
#include <atomic>
#include <thread>

#include <gtest/gtest.h>

#include "af/detail/queue/bounded_queues.hpp"

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
