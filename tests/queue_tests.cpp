#include <algorithm>
#include <array>
#include <atomic>
#include <limits>
#include <stdexcept>
#include <thread>
#include <type_traits>

#include <gtest/gtest.h>

#include "af/detail/queue/bounded_queues.hpp"
#include "af/detail/queue/intrusive_mpsc_queue.hpp"

namespace {

static_assert(
    std::is_same_v<af::detail::bounded_mpsc_queue<int>, af::detail::BoundedMpscQueue<int>>);
static_assert(
    std::is_same_v<af::detail::bounded_mpmc_queue<int>, af::detail::BoundedMpmcQueue<int>>);
static_assert(
    std::is_same_v<af::detail::intrusive_mpsc_node<int>, af::detail::IntrusiveMpscNode<int>>);
static_assert(
    std::is_same_v<af::detail::intrusive_mpsc_queue<int>, af::detail::IntrusiveMpscQueue<int>>);
static_assert(std::is_same_v<af::detail::queue_full_backoff, af::detail::QueueFullBackoff>);

struct IntrusiveQueueValue {
    int producer{0};
    int sequence{0};
    af::detail::intrusive_mpsc_node<IntrusiveQueueValue> intrusive_mpsc_node_{this};
};

} // namespace

TEST(QueueTests, BoundedQueueSequenceBeforeHandlesUnsignedWrapWindow) {
    constexpr std::size_t max = std::numeric_limits<std::size_t>::max();

    EXPECT_FALSE(af::detail::bounded_queue_sequence_before(10, 10));
    EXPECT_FALSE(af::detail::bounded_queue_sequence_before(11, 10));
    EXPECT_TRUE(af::detail::bounded_queue_sequence_before(9, 10));
    EXPECT_FALSE(af::detail::bounded_queue_sequence_before(0, max));
    EXPECT_TRUE(af::detail::bounded_queue_sequence_before(max, 0));
}

TEST(QueueTests, BoundedMpscRejectsWhenFull) {
    af::detail::bounded_mpsc_queue<int> queue(2);
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
    af::detail::bounded_mpsc_queue<int> queue(4);
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

TEST(QueueTests, BoundedMpscPushesManyInFifoOrder) {
    af::detail::bounded_mpsc_queue<int> queue(4);
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

TEST(QueueTests, BoundedMpscPushManyStopsWhenFull) {
    af::detail::bounded_mpsc_queue<int> queue(2);
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

TEST(QueueTests, BoundedMpscSupportsConcurrentProducers) {
    constexpr int producer_count = 4;
    constexpr int values_per_producer = 64;
    constexpr int total_values = producer_count * values_per_producer;

    af::detail::bounded_mpsc_queue<int> queue(128);
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

TEST(QueueTests, BoundedMpscPushManySupportsConcurrentProducers) {
    constexpr int producer_count = 4;
    constexpr int values_per_producer = 64;
    constexpr int batch_size = 8;
    constexpr int total_values = producer_count * values_per_producer;

    af::detail::bounded_mpsc_queue<int> queue(128);
    std::array<std::array<int, values_per_producer>, producer_count> values{};
    std::array<std::thread, producer_count> producers;
    std::atomic<int> pushed{0};

    for (int producer = 0; producer < producer_count; ++producer) {
        for (int i = 0; i < values_per_producer; ++i) {
            values[producer][i] = producer * values_per_producer + i;
        }

        producers[producer] = std::thread([producer, &queue, &values, &pushed] {
            constexpr int local_values_per_producer = 64;
            constexpr int local_batch_size = 8;
            std::array<int *, local_batch_size> batch{};
            int index = 0;
            while (index < local_values_per_producer) {
                const int count = std::min(local_batch_size, local_values_per_producer - index);
                for (int i = 0; i < count; ++i) {
                    batch[i] = &values[producer][index + i];
                }

                std::size_t batch_pushed = 0;
                while (batch_pushed < static_cast<std::size_t>(count)) {
                    const std::size_t pushed_now =
                        queue.try_push_many(batch.data() + batch_pushed,
                                            static_cast<std::size_t>(count) - batch_pushed);
                    if (pushed_now == 0U) {
                        std::this_thread::yield();
                        continue;
                    }
                    batch_pushed += pushed_now;
                }

                pushed.fetch_add(count, std::memory_order_release);
                index += count;
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

TEST(QueueTests, IntrusiveMpscReturnsSingleNodeAndAllowsImmediateReuse) {
    af::detail::intrusive_mpsc_queue<IntrusiveQueueValue> queue;
    IntrusiveQueueValue value;

    EXPECT_TRUE(queue.empty());
    queue.push(&value);
    EXPECT_FALSE(queue.empty());
    EXPECT_EQ(queue.try_pop(), &value);
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.try_pop(), nullptr);

    queue.push(&value);
    EXPECT_EQ(queue.try_pop(), &value);
    EXPECT_TRUE(queue.empty());
}

TEST(QueueTests, IntrusiveMpscEmptyReportsBufferedTailNode) {
    af::detail::intrusive_mpsc_queue<IntrusiveQueueValue> queue;
    IntrusiveQueueValue first;
    IntrusiveQueueValue second;

    queue.push(&first);
    queue.push(&second);

    EXPECT_EQ(queue.try_pop(), &first);
    EXPECT_FALSE(queue.empty());
    EXPECT_EQ(queue.try_pop(), &second);
    EXPECT_TRUE(queue.empty());
}

TEST(QueueTests, IntrusiveMpscSupportsConcurrentProducersInPerProducerOrder) {
    constexpr int producer_count = 4;
    constexpr int values_per_producer = 256;
    constexpr int total_values = producer_count * values_per_producer;

    af::detail::intrusive_mpsc_queue<IntrusiveQueueValue> queue;
    std::array<std::array<IntrusiveQueueValue, values_per_producer>, producer_count> values{};
    std::array<std::thread, producer_count> producers;
    std::array<int, producer_count> next_sequence{};
    std::atomic<int> pushed{0};
    std::atomic<bool> start{false};

    for (int producer = 0; producer < producer_count; ++producer) {
        for (int sequence = 0; sequence < values_per_producer; ++sequence) {
            values[producer][sequence].producer = producer;
            values[producer][sequence].sequence = sequence;
        }

        producers[producer] = std::thread([producer, &queue, &values, &pushed, &start] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int sequence = 0; sequence < values_per_producer; ++sequence) {
                queue.push(&values[producer][sequence]);
                pushed.fetch_add(1, std::memory_order_release);
            }
        });
    }

    start.store(true, std::memory_order_release);
    int popped = 0;
    while (popped < total_values) {
        IntrusiveQueueValue *value = queue.try_pop();
        if (value == nullptr) {
            std::this_thread::yield();
            continue;
        }
        EXPECT_EQ(value->sequence, next_sequence[value->producer]);
        ++next_sequence[value->producer];
        ++popped;
    }

    for (auto &producer : producers) {
        producer.join();
    }

    EXPECT_EQ(pushed.load(std::memory_order_acquire), total_values);
    for (int producer = 0; producer < producer_count; ++producer) {
        EXPECT_EQ(next_sequence[producer], values_per_producer);
    }
    EXPECT_TRUE(queue.empty());
}

TEST(QueueTests, BoundedMpmcRejectsWhenFull) {
    af::detail::bounded_mpmc_queue<int> queue(2);
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

    EXPECT_THROW({ af::detail::bounded_mpsc_queue<int> queue(too_large); }, std::length_error);
    EXPECT_THROW({ af::detail::bounded_mpmc_queue<int> queue(too_large); }, std::length_error);
}
