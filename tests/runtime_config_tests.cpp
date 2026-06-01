#include <cstddef>
#include <cstdint>

#include <gtest/gtest.h>

#include "af/async_runtime.hpp"

namespace {

enum class ConfigThread : std::uint16_t {
    Logic0,
};

struct AboveSixtyFourThreadTraits {
    using Thread = ConfigThread;

    static constexpr std::size_t thread_count = 257;
};

using AboveSixtyFourRuntime = af::AsyncRuntime<AboveSixtyFourThreadTraits>;

static_assert(AboveSixtyFourRuntime::thread_count == 257U);
static_assert(AboveSixtyFourRuntime::invalid_thread_index == 257U);
static_assert(
    AboveSixtyFourRuntime::Config::task_pool_remote_release_batch_size == 64U);
static_assert(AboveSixtyFourRuntime::Config::task_pool_chunk_size == 256U);
static_assert(!AboveSixtyFourRuntime::Config::task_pool_cache_slot_index);
static_assert(AboveSixtyFourRuntime::Config::task_pool_local_cache_set_size ==
              1U);
static_assert(
    AboveSixtyFourRuntime::Config::task_pool_direct_release_set_size == 4U);
static_assert(AboveSixtyFourRuntime::Config::task_pool_local_cache_capacity ==
              64U);

struct RemoteBatchOverrideTraits {
    using Thread = ConfigThread;

    static constexpr std::size_t thread_count = 1;
    static constexpr std::size_t task_pool_remote_release_batch_size = 32;
    static constexpr std::size_t task_pool_chunk_size = 512;
    static constexpr bool task_pool_cache_slot_index = true;
    static constexpr std::size_t task_pool_local_cache_set_size = 16;
    static constexpr std::size_t task_pool_direct_release_set_size = 8;
    static constexpr std::size_t task_pool_local_cache_capacity = 128;
};

using RemoteBatchOverrideRuntime = af::AsyncRuntime<RemoteBatchOverrideTraits>;

static_assert(
    RemoteBatchOverrideRuntime::Config::task_pool_remote_release_batch_size ==
    32U);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_chunk_size == 512U);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_cache_slot_index);
static_assert(
    RemoteBatchOverrideRuntime::Config::task_pool_local_cache_set_size == 16U);
static_assert(
    RemoteBatchOverrideRuntime::Config::task_pool_direct_release_set_size == 8U);
static_assert(
    RemoteBatchOverrideRuntime::Config::task_pool_local_cache_capacity == 128U);

} // namespace

TEST(RuntimeConfigTests, PreservesThreadCountsAboveSixtyFour) {
    EXPECT_EQ(AboveSixtyFourRuntime::thread_count, 257U);
    EXPECT_EQ(AboveSixtyFourRuntime::invalid_thread_index, 257U);
}

TEST(RuntimeConfigTests, DefaultsAndOverridesTaskPoolRemoteReleaseBatchSize) {
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_remote_release_batch_size,
              64U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_chunk_size, 256U);
    EXPECT_FALSE(AboveSixtyFourRuntime::Config::task_pool_cache_slot_index);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_local_cache_set_size,
              1U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_direct_release_set_size,
              4U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_local_cache_capacity,
              64U);
    EXPECT_EQ(
        RemoteBatchOverrideRuntime::Config::task_pool_remote_release_batch_size,
        32U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_chunk_size, 512U);
    EXPECT_TRUE(RemoteBatchOverrideRuntime::Config::task_pool_cache_slot_index);
    EXPECT_EQ(
        RemoteBatchOverrideRuntime::Config::task_pool_local_cache_set_size,
        16U);
    EXPECT_EQ(
        RemoteBatchOverrideRuntime::Config::task_pool_direct_release_set_size,
        8U);
    EXPECT_EQ(
        RemoteBatchOverrideRuntime::Config::task_pool_local_cache_capacity,
        128U);
}
