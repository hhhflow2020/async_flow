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
