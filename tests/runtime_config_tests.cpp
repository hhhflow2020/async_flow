#include <cstddef>
#include <cstdint>
#include <type_traits>

#include <gtest/gtest.h>

#include "af/async_runtime.hpp"

namespace {

struct ConfigThreadTag;
struct ConfigLogicThreadTag;
struct ConfigIoThreadTag;

struct AboveSixtyFourThreadTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<ConfigThreadTag, 257>());
};

using AboveSixtyFourRuntime = af::AsyncRuntime<AboveSixtyFourThreadTraits>;

static_assert(AboveSixtyFourRuntime::thread_count == 257U);
static_assert(AboveSixtyFourRuntime::invalid_thread_index == 257U);
static_assert(AboveSixtyFourRuntime::Config::task_pool_remote_release_batch_size == 64U);
static_assert(AboveSixtyFourRuntime::Config::task_pool_chunk_size == 256U);
static_assert(!AboveSixtyFourRuntime::Config::task_pool_cache_slot_index);
static_assert(AboveSixtyFourRuntime::Config::task_pool_local_cache_set_size == 1U);
static_assert(AboveSixtyFourRuntime::Config::task_pool_direct_release_set_size == 4U);
static_assert(AboveSixtyFourRuntime::Config::task_pool_local_cache_capacity == 64U);

struct ThreadLayoutMetadataTraits {
    static constexpr auto threads = af::thread_layout(
        af::thread_group<ConfigLogicThreadTag, 3, af::ThreadKind::Worker, "logic">(),
        af::thread_group<ConfigIoThreadTag, 2, af::ThreadKind::IoUring, "io">());
};

using ThreadLayoutMetadataRuntime = af::AsyncRuntime<ThreadLayoutMetadataTraits>;
using ConfigLogicGroup =
    decltype(ThreadLayoutMetadataRuntime::thread_group<ConfigLogicThreadTag>());

static_assert(ThreadLayoutMetadataRuntime::thread_count == 5U);
static_assert(sizeof(ThreadLayoutMetadataRuntime::Thread) == sizeof(std::uint16_t));
static_assert(std::is_empty_v<ConfigLogicGroup>);

struct RemoteBatchOverrideTraits {
    static constexpr auto threads = af::thread_layout(af::thread_group<ConfigThreadTag, 1>());
    static constexpr std::size_t task_pool_remote_release_batch_size = 32;
    static constexpr std::size_t task_pool_chunk_size = 512;
    static constexpr bool task_pool_cache_slot_index = true;
    static constexpr std::size_t task_pool_local_cache_set_size = 16;
    static constexpr std::size_t task_pool_direct_release_set_size = 8;
    static constexpr std::size_t task_pool_local_cache_capacity = 128;
};

using RemoteBatchOverrideRuntime = af::AsyncRuntime<RemoteBatchOverrideTraits>;

static_assert(RemoteBatchOverrideRuntime::Config::task_pool_remote_release_batch_size == 32U);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_chunk_size == 512U);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_cache_slot_index);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_local_cache_set_size == 16U);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_direct_release_set_size == 8U);
static_assert(RemoteBatchOverrideRuntime::Config::task_pool_local_cache_capacity == 128U);

} // namespace

TEST(RuntimeConfigTests, PreservesThreadCountsAboveSixtyFour) {
    EXPECT_EQ(AboveSixtyFourRuntime::thread_count, 257U);
    EXPECT_EQ(AboveSixtyFourRuntime::invalid_thread_index, 257U);
}

TEST(RuntimeConfigTests, ThreadLayoutGroupsCarryKindNameAndIndexMetadata) {
    constexpr auto logic = ThreadLayoutMetadataRuntime::thread_group<ConfigLogicThreadTag>();
    constexpr auto io = ThreadLayoutMetadataRuntime::thread_group<ConfigIoThreadTag>();

    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_index(logic.begin()), 0U);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_index(logic.template at<2>()), 2U);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_index(io.begin()), 3U);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_index(io.shard(5U)), 4U);
    EXPECT_TRUE(logic.contains(logic.template at<1>()));
    EXPECT_FALSE(logic.contains(io.template at<0>()));
    EXPECT_EQ(logic.offset_of(logic.template at<2>()), 2U);

    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_kind(logic.template at<0>()),
              af::ThreadKind::Worker);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_kind(io.template at<0>()),
              af::ThreadKind::IoUring);
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_name(logic.template at<0>()), "logic");
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_name(io.template at<1>()), "io");
    EXPECT_EQ(ThreadLayoutMetadataRuntime::thread_group_offset(io.template at<1>()), 1U);
}

TEST(RuntimeConfigTests, DefaultsAndOverridesTaskPoolRemoteReleaseBatchSize) {
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_remote_release_batch_size, 64U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_chunk_size, 256U);
    EXPECT_FALSE(AboveSixtyFourRuntime::Config::task_pool_cache_slot_index);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_local_cache_set_size, 1U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_direct_release_set_size, 4U);
    EXPECT_EQ(AboveSixtyFourRuntime::Config::task_pool_local_cache_capacity, 64U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_remote_release_batch_size, 32U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_chunk_size, 512U);
    EXPECT_TRUE(RemoteBatchOverrideRuntime::Config::task_pool_cache_slot_index);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_local_cache_set_size, 16U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_direct_release_set_size, 8U);
    EXPECT_EQ(RemoteBatchOverrideRuntime::Config::task_pool_local_cache_capacity, 128U);
}
