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

} // namespace

TEST(RuntimeConfigTests, PreservesThreadCountsAboveSixtyFour) {
    EXPECT_EQ(AboveSixtyFourRuntime::thread_count, 257U);
    EXPECT_EQ(AboveSixtyFourRuntime::invalid_thread_index, 257U);
}
