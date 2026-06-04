#include <array>
#include <atomic>
#include <cstdint>
#include <new>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

#include "af/detail/memory/object_pool.hpp"

TEST(PoolTests, ObjectPoolReusesReleasedStorage) {
    struct Payload {
        int value{0};
    };

    af::detail::ObjectPool<Payload, 1> pool;
    Payload *first = pool.create();
    first->value = 42;
    pool.destroy(first);

    Payload *second = pool.create();
    EXPECT_EQ(second, first);
    pool.destroy(second);
}

TEST(PoolTests, ObjectPoolReturnsSlotAfterConstructorThrows) {
    struct Payload {
        Payload(bool fail, void **attempted_address) {
            if (attempted_address != nullptr) {
                *attempted_address = this;
            }
            if (fail) {
                throw std::runtime_error("constructor failed");
            }
        }
    };

    af::detail::ObjectPool<Payload, 1> pool;
    void *attempted = nullptr;

    EXPECT_THROW(static_cast<void>(pool.create(true, &attempted)), std::runtime_error);
    ASSERT_NE(attempted, nullptr);

    Payload *object = pool.create(false, nullptr);
    EXPECT_EQ(static_cast<void *>(object), attempted);
    pool.destroy(object);
}

TEST(PoolTests, ObjectPoolOomHandlerDoesNotHandleConstructorBadAlloc) {
    struct Payload {
        explicit Payload(bool fail) {
            if (fail) {
                throw std::bad_alloc();
            }
        }
    };

    af::detail::ObjectPool<Payload, 1> pool;
    bool oom_handler_called = false;

    EXPECT_THROW(
        static_cast<void>(pool.create_with_oom_handler([&] { oom_handler_called = true; }, true)),
        std::bad_alloc);
    EXPECT_FALSE(oom_handler_called);

    Payload *object = pool.create_with_oom_handler([&] { oom_handler_called = true; }, false);
    EXPECT_FALSE(oom_handler_called);
    pool.destroy(object);
}

TEST(PoolTests, ObjectPoolUncachedReturnsSlotAfterConstructorThrows) {
    struct Payload {
        Payload(bool fail, void **attempted_address) {
            if (attempted_address != nullptr) {
                *attempted_address = this;
            }
            if (fail) {
                throw std::runtime_error("constructor failed");
            }
        }
    };

    af::detail::ObjectPool<Payload, 1> pool;
    void *attempted = nullptr;

    EXPECT_THROW(static_cast<void>(pool.create_uncached(true, &attempted)), std::runtime_error);
    ASSERT_NE(attempted, nullptr);

    Payload *object = pool.create_uncached(false, nullptr);
    EXPECT_EQ(static_cast<void *>(object), attempted);
    pool.destroy_uncached(object);
}

TEST(PoolTests, ObjectPoolTryCreateReturnsNullAndReusesSlotAfterConstructorThrows) {
    struct Payload {
        Payload(bool fail, void **attempted_address) {
            if (attempted_address != nullptr) {
                *attempted_address = this;
            }
            if (fail) {
                throw std::runtime_error("constructor failed");
            }
        }
    };

    af::detail::ObjectPool<Payload, 1> pool;
    void *attempted = nullptr;

    EXPECT_EQ(pool.try_create(true, &attempted), nullptr);
    ASSERT_NE(attempted, nullptr);

    Payload *object = pool.try_create(false, nullptr);
    ASSERT_NE(object, nullptr);
    EXPECT_EQ(static_cast<void *>(object), attempted);
    pool.destroy(object);
}

TEST(PoolTests, ObjectPoolTryCreateUncachedReturnsNullAndReusesSlotAfterConstructorThrows) {
    struct Payload {
        Payload(bool fail, void **attempted_address) {
            if (attempted_address != nullptr) {
                *attempted_address = this;
            }
            if (fail) {
                throw std::runtime_error("constructor failed");
            }
        }
    };

    af::detail::ObjectPool<Payload, 1> pool;
    void *attempted = nullptr;

    EXPECT_EQ(pool.try_create_uncached(true, &attempted), nullptr);
    ASSERT_NE(attempted, nullptr);

    Payload *object = pool.try_create_uncached(false, nullptr);
    ASSERT_NE(object, nullptr);
    EXPECT_EQ(static_cast<void *>(object), attempted);
    pool.destroy_uncached(object);
}

TEST(PoolTests, ObjectPoolSupportsCrossThreadDestroy) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload, 1> pool;
    Payload *object = pool.create(7);
    ASSERT_EQ(object->value, 7U);

    std::thread destroyer([&] { pool.destroy(object); });
    destroyer.join();

    Payload *reused = pool.create(9);
    EXPECT_EQ(reused, object);
    EXPECT_EQ(reused->value, 9U);
    pool.destroy(reused);
}

TEST(PoolTests, ObjectPoolDefaultRemoteDestroyReturnsBeforeThreadExit) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload, 1> pool;
    Payload *object = pool.create(7);
    std::atomic<bool> destroyed{false};
    std::atomic<bool> finish{false};

    std::thread destroyer([&] {
        pool.destroy(object);
        destroyed.store(true, std::memory_order_release);
        while (!finish.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    });

    while (!destroyed.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    Payload *reused = pool.create(9);
    EXPECT_EQ(reused, object);
    EXPECT_EQ(reused->value, 9U);
    pool.destroy(reused);

    finish.store(true, std::memory_order_release);
    destroyer.join();
}

TEST(PoolTests, ObjectPoolDefaultRemoteDestroyHandlesAlternatingPoolsBeforeThreadExit) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload, 4> first_pool;
    af::detail::ObjectPool<Payload, 4> second_pool;
    std::array<Payload *, 4> first_objects{};
    std::array<Payload *, 4> second_objects{};

    for (std::size_t i = 0; i < first_objects.size(); ++i) {
        first_objects[i] = first_pool.create(static_cast<std::uint64_t>(i));
        second_objects[i] = second_pool.create(static_cast<std::uint64_t>(10 + i));
    }

    std::atomic<bool> destroyed{false};
    std::atomic<bool> finish{false};
    std::thread destroyer([&] {
        for (std::size_t i = 0; i < first_objects.size(); ++i) {
            first_pool.destroy(first_objects[i]);
            second_pool.destroy(second_objects[i]);
        }
        destroyed.store(true, std::memory_order_release);
        while (!finish.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    });

    while (!destroyed.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    for (std::size_t i = 0; i < first_objects.size(); ++i) {
        Payload *first = first_pool.create(static_cast<std::uint64_t>(20 + i));
        Payload *second = second_pool.create(static_cast<std::uint64_t>(30 + i));
        bool found_first = false;
        bool found_second = false;
        for (std::size_t original = 0; original < first_objects.size(); ++original) {
            found_first = found_first || first == first_objects[original];
            found_second = found_second || second == second_objects[original];
        }
        EXPECT_TRUE(found_first);
        EXPECT_TRUE(found_second);
        first_pool.destroy(first);
        second_pool.destroy(second);
    }

    finish.store(true, std::memory_order_release);
    destroyer.join();
}

TEST(PoolTests, ObjectPoolSupportsCustomLocalCacheSetSize) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    using Pool = af::detail::ObjectPool<Payload, 2, 1, false, 4>;
    std::array<Pool, 4> pools;
    std::array<Payload *, 4> objects{};

    for (std::size_t i = 0; i < pools.size(); ++i) {
        objects[i] = pools[i].create(static_cast<std::uint64_t>(i));
        EXPECT_EQ(objects[i]->value, i);
    }
    for (std::size_t i = 0; i < pools.size(); ++i) {
        pools[i].destroy(objects[i]);
    }
}

TEST(PoolTests, ObjectPoolSupportsSingleLocalCacheEntry) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    using Pool = af::detail::ObjectPool<Payload, 2, 1, false, 1>;
    Pool first_pool;
    Pool second_pool;

    Payload *first = first_pool.create(1);
    Payload *second = second_pool.create(2);
    first_pool.destroy(first);
    second_pool.destroy(second);

    Payload *first_reused = first_pool.create(3);
    Payload *second_reused = second_pool.create(4);
    EXPECT_EQ(first_reused->value, 3U);
    EXPECT_EQ(second_reused->value, 4U);
    first_pool.destroy(first_reused);
    second_pool.destroy(second_reused);
}

TEST(PoolTests, ObjectPoolSupportsCustomDirectReleaseSetSize) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    using Pool = af::detail::ObjectPool<Payload, 2, 1, false, 4, 4>;
    std::array<Pool, 4> pools;
    std::array<std::array<Payload *, 2>, 4> objects{};

    for (std::size_t i = 0; i < pools.size(); ++i) {
        objects[i][0] = pools[i].create(static_cast<std::uint64_t>(i * 2U));
        objects[i][1] = pools[i].create(static_cast<std::uint64_t>(i * 2U + 1U));
    }

    std::thread destroyer([&] {
        for (std::size_t i = 0; i < pools.size(); ++i) {
            pools[i].destroy(objects[i][0]);
            pools[i].destroy(objects[i][1]);
        }
    });
    destroyer.join();

    for (std::size_t i = 0; i < pools.size(); ++i) {
        Payload *first = pools[i].create(static_cast<std::uint64_t>(10 + i));
        Payload *second = pools[i].create(static_cast<std::uint64_t>(20 + i));
        const bool first_reused = first == objects[i][0] || first == objects[i][1];
        const bool second_reused = second == objects[i][0] || second == objects[i][1];
        EXPECT_TRUE(first_reused);
        EXPECT_TRUE(second_reused);
        EXPECT_NE(first, second);
        pools[i].destroy(first);
        pools[i].destroy(second);
    }
}

TEST(PoolTests, ObjectPoolSupportsCustomLocalCacheCapacity) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    using Pool = af::detail::ObjectPool<Payload, 2, 1, false, 4, 4, 4>;
    Pool pool;
    std::array<Payload *, 4> objects{};

    for (std::size_t i = 0; i < objects.size(); ++i) {
        objects[i] = pool.create(static_cast<std::uint64_t>(i));
    }
    for (Payload *object : objects) {
        pool.destroy(object);
    }
    for (std::size_t i = 0; i < objects.size(); ++i) {
        Payload *object = pool.create(static_cast<std::uint64_t>(10 + i));
        EXPECT_EQ(object->value, 10 + i);
        pool.destroy(object);
    }
}

TEST(PoolTests, ObjectPoolRemoteReleaseBatchFlushesAtThreshold) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload, 4, 4> pool;
    std::array<Payload *, 4> objects{};
    for (std::size_t i = 0; i < objects.size(); ++i) {
        objects[i] = pool.create(static_cast<std::uint64_t>(i));
    }

    std::atomic<bool> destroyed{false};
    std::atomic<bool> finish{false};
    std::thread destroyer([&] {
        for (Payload *object : objects) {
            pool.destroy(object);
        }
        destroyed.store(true, std::memory_order_release);
        while (!finish.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    });

    while (!destroyed.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    std::array<Payload *, 4> reused{};
    for (std::size_t i = 0; i < reused.size(); ++i) {
        reused[i] = pool.create(static_cast<std::uint64_t>(10 + i));
        bool found = false;
        for (Payload *original : objects) {
            found = found || reused[i] == original;
        }
        EXPECT_TRUE(found);
    }
    for (Payload *object : reused) {
        pool.destroy(object);
    }

    finish.store(true, std::memory_order_release);
    destroyer.join();
}

TEST(PoolTests, ObjectPoolSingleLocalCacheEntryBatchesRemoteRelease) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    using Pool = af::detail::ObjectPool<Payload, 4, 4, false, 1>;
    Pool pool;
    std::array<Payload *, 4> objects{};
    for (std::size_t i = 0; i < objects.size(); ++i) {
        objects[i] = pool.create(static_cast<std::uint64_t>(i));
    }

    std::atomic<bool> destroyed{false};
    std::atomic<bool> finish{false};
    std::thread destroyer([&] {
        for (Payload *object : objects) {
            pool.destroy(object);
        }
        destroyed.store(true, std::memory_order_release);
        while (!finish.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    });

    while (!destroyed.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    std::array<Payload *, 4> reused{};
    for (std::size_t i = 0; i < reused.size(); ++i) {
        reused[i] = pool.create(static_cast<std::uint64_t>(10 + i));
        bool found = false;
        for (Payload *original : objects) {
            found = found || reused[i] == original;
        }
        EXPECT_TRUE(found);
    }
    for (Payload *object : reused) {
        pool.destroy(object);
    }

    finish.store(true, std::memory_order_release);
    destroyer.join();
}

TEST(PoolTests, ObjectPoolRemoteReleaseBatchFlushesOnThreadExit) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload, 4, 8> pool;
    std::array<Payload *, 4> objects{};
    for (std::size_t i = 0; i < objects.size(); ++i) {
        objects[i] = pool.create(static_cast<std::uint64_t>(i));
    }

    std::thread destroyer([&] {
        for (Payload *object : objects) {
            pool.destroy(object);
        }
    });
    destroyer.join();

    std::array<Payload *, 4> reused{};
    for (std::size_t i = 0; i < reused.size(); ++i) {
        reused[i] = pool.create(static_cast<std::uint64_t>(10 + i));
        bool found = false;
        for (Payload *original : objects) {
            found = found || reused[i] == original;
        }
        EXPECT_TRUE(found);
    }
    for (Payload *object : reused) {
        pool.destroy(object);
    }
}

TEST(PoolTests, ObjectPoolCachedSlotIndexRemoteReleaseBatchFlushesAtThreshold) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload, 4, 4, true> pool;
    std::array<Payload *, 4> objects{};
    for (std::size_t i = 0; i < objects.size(); ++i) {
        objects[i] = pool.create(static_cast<std::uint64_t>(i));
    }

    std::thread destroyer([&] {
        for (Payload *object : objects) {
            pool.destroy(object);
        }
    });
    destroyer.join();

    std::array<Payload *, 4> reused{};
    for (std::size_t i = 0; i < reused.size(); ++i) {
        reused[i] = pool.create(static_cast<std::uint64_t>(10 + i));
        bool found = false;
        for (Payload *original : objects) {
            found = found || reused[i] == original;
        }
        EXPECT_TRUE(found);
    }
    for (Payload *object : reused) {
        pool.destroy(object);
    }
}

TEST(PoolTests, ObjectPoolKeepsSameTypePoolCachesSeparate) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload, 1> first_pool;
    af::detail::ObjectPool<Payload, 1> second_pool;

    Payload *first = first_pool.create(1);
    first_pool.destroy(first);

    Payload *second = second_pool.create(2);
    second_pool.destroy(second);

    Payload *first_again = first_pool.create(3);
    EXPECT_EQ(first_again, first);
    EXPECT_EQ(first_again->value, 3U);
    first_pool.destroy(first_again);
}

TEST(PoolTests, ObjectPoolSupportsReserveSlots) {
    struct Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload, 2> pool;
    pool.reserve_slots(0);
    pool.reserve_slots(5);

    std::array<Payload *, 5> objects{};
    for (std::size_t i = 0; i < objects.size(); ++i) {
        objects[i] = pool.create(static_cast<std::uint64_t>(i));
        EXPECT_EQ(objects[i]->value, i);
    }
    for (Payload *object : objects) {
        pool.destroy(object);
    }
}

TEST(PoolTests, ObjectPoolSupportsOverAlignedPayload) {
    struct alignas(128) Payload {
        explicit Payload(std::uint64_t value) : value(value) {}
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload, 2> pool;
    std::array<Payload *, 3> objects{};

    for (std::size_t i = 0; i < objects.size(); ++i) {
        objects[i] = pool.create(static_cast<std::uint64_t>(i));
        const auto address = reinterpret_cast<std::uintptr_t>(objects[i]);
        EXPECT_EQ(address % alignof(Payload), 0U);
        EXPECT_EQ(objects[i]->value, i);
    }
    for (Payload *object : objects) {
        pool.destroy(object);
    }
}

TEST(PoolTests, ObjectPoolSupportsReserveBlocks) {
    struct Payload {
        std::uint64_t value{0};
    };

    af::detail::ObjectPool<Payload, 2> pool;
    pool.reserve_blocks(3);

    std::array<Payload *, 6> objects{};
    for (Payload *&object : objects) {
        object = pool.create();
    }
    for (Payload *object : objects) {
        pool.destroy(object);
    }
}

TEST(PoolTests, ObjectPoolSupportsRepeatedCrossThreadBatchDestroy) {
    struct Payload {
        Payload(std::uint64_t round, std::uint64_t index)
            : round(round), index(index), checksum(round ^ (index << 1U)) {}

        std::uint64_t round{0};
        std::uint64_t index{0};
        std::uint64_t checksum{0};
    };

    constexpr std::size_t batch_size = 512;
    constexpr std::uint64_t rounds = 256;

    af::detail::ObjectPool<Payload, 16> pool;
    std::array<Payload *, batch_size> objects{};
    std::atomic<std::uint64_t> published_round{0};
    std::atomic<std::uint64_t> consumed_round{0};
    std::atomic<int> failures{0};

    std::thread destroyer([&] {
        for (std::uint64_t round = 1; round <= rounds; ++round) {
            while (published_round.load(std::memory_order_acquire) != round) {
                std::this_thread::yield();
            }

            for (std::size_t i = 0; i < objects.size(); ++i) {
                Payload *object = objects[i];
                if (object->round != round || object->index != i ||
                    object->checksum != (round ^ (static_cast<std::uint64_t>(i) << 1U))) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
                pool.destroy(object);
            }

            consumed_round.store(round, std::memory_order_release);
        }
    });

    for (std::uint64_t round = 1; round <= rounds; ++round) {
        for (std::size_t i = 0; i < objects.size(); ++i) {
            objects[i] = pool.create(round, static_cast<std::uint64_t>(i));
        }

        published_round.store(round, std::memory_order_release);
        while (consumed_round.load(std::memory_order_acquire) != round) {
            std::this_thread::yield();
        }
    }

    destroyer.join();
    EXPECT_EQ(failures.load(std::memory_order_acquire), 0);
}

TEST(PoolTests, ObjectPoolSupportsConcurrentCreateDestroy) {
    struct Payload {
        std::uint64_t producer{0};
        std::uint64_t sequence{0};
        std::uint64_t checksum{0};

        Payload(std::uint64_t owner, std::uint64_t value)
            : producer(owner), sequence(value), checksum(owner ^ (value << 1U)) {}
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
                auto *object =
                    pool.create(static_cast<std::uint64_t>(thread), static_cast<std::uint64_t>(i));
                if (object->producer != static_cast<std::uint64_t>(thread) ||
                    object->sequence != static_cast<std::uint64_t>(i) ||
                    object->checksum != (static_cast<std::uint64_t>(thread) ^
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

    for (auto &thread : threads) {
        thread.join();
    }

    EXPECT_EQ(failures.load(std::memory_order_acquire), 0);
}

TEST(PoolTests, ObjectPoolUncachedSupportsConcurrentCreateDestroy) {
    struct Payload {
        std::uint64_t producer{0};
        std::uint64_t sequence{0};
        std::uint64_t checksum{0};
        std::atomic<int> *destroyed{nullptr};

        Payload(std::uint64_t owner, std::uint64_t value, std::atomic<int> *destroyed_counter)
            : producer(owner), sequence(value), checksum(owner ^ (value << 1U)),
              destroyed(destroyed_counter) {}

        ~Payload() {
            destroyed->fetch_add(1, std::memory_order_relaxed);
        }
    };

    constexpr int thread_count = 8;
    constexpr int iterations = 4096;

    af::detail::ObjectPool<Payload, 8> pool;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> failures{0};
    std::atomic<int> destroyed{0};
    std::array<std::thread, thread_count> threads;

    for (int thread = 0; thread < thread_count; ++thread) {
        threads[static_cast<std::size_t>(thread)] = std::thread([&, thread] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (int i = 0; i < iterations; ++i) {
                auto *object = pool.create_uncached(static_cast<std::uint64_t>(thread),
                                                    static_cast<std::uint64_t>(i), &destroyed);
                if (object->producer != static_cast<std::uint64_t>(thread) ||
                    object->sequence != static_cast<std::uint64_t>(i) ||
                    object->checksum != (static_cast<std::uint64_t>(thread) ^
                                         (static_cast<std::uint64_t>(i) << 1U))) {
                    failures.fetch_add(1, std::memory_order_relaxed);
                }
                pool.destroy_uncached(object);
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != thread_count) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);

    for (auto &thread : threads) {
        thread.join();
    }

    EXPECT_EQ(failures.load(std::memory_order_acquire), 0);
    EXPECT_EQ(destroyed.load(std::memory_order_acquire), thread_count * iterations);
}
