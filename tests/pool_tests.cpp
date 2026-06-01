#include <array>
#include <atomic>
#include <cstdint>
#include <thread>

#include <gtest/gtest.h>

#include "af/detail/object_pool.hpp"

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

TEST(PoolTests, ObjectPoolSupportsConcurrentCreateDestroy) {
    struct Payload {
        std::uint64_t producer{0};
        std::uint64_t sequence{0};
        std::uint64_t checksum{0};

        Payload(std::uint64_t owner, std::uint64_t value)
            : producer(owner),
              sequence(value),
              checksum(owner ^ (value << 1U)) {}
    };

    constexpr int thread_count = 8;
    constexpr int iterations = 4096;

    af::detail::ObjectPool<Payload, 8> pool;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> failures{0};
    std::array<std::thread, thread_count> threads;

    for (int thread = 0; thread < thread_count; ++thread) {
        threads[static_cast<std::size_t>(thread)] = std::thread([&, thread] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (int i = 0; i < iterations; ++i) {
                auto* object = pool.create(
                    static_cast<std::uint64_t>(thread),
                    static_cast<std::uint64_t>(i));
                if (object->producer != static_cast<std::uint64_t>(thread) ||
                    object->sequence != static_cast<std::uint64_t>(i) ||
                    object->checksum !=
                        (static_cast<std::uint64_t>(thread) ^
                         (static_cast<std::uint64_t>(i) << 1U))) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
                pool.destroy(object);
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != thread_count) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(failures.load(std::memory_order_acquire), 0);
}
