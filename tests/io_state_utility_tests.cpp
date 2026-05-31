#include <gtest/gtest.h>

#include "af/async_flow.hpp"

TEST(UtilityTests, IoOpStateResetClearsCompletionToken) {
    int token = 0;
    af::IoOpState state{};
    state.wait = af::IoResult{3, af::io_readable, 0, 16};
    state.wait.completion_token = &token;
    state.waiting = true;
    state.wait_kind = af::IoWaitKind::Completion;

    state.reset();

    EXPECT_EQ(state.wait.completion_token, nullptr);
    EXPECT_FALSE(state.waiting);
    EXPECT_EQ(state.wait_kind, af::IoWaitKind::None);
}
