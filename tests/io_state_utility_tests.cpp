#include <cerrno>

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

TEST(UtilityTests, SetIoResultErrorClearsStaleCompletionState) {
    int token = 0;
    af::IoResult result{7, af::io_readable, 0, 64, &token};

    af::detail::set_io_result_error(result, 9, EINVAL);

    EXPECT_EQ(result.fd, 9);
    EXPECT_EQ(result.events, af::io_error);
    EXPECT_EQ(result.error, EINVAL);
    EXPECT_EQ(result.result, -EINVAL);
    EXPECT_EQ(result.completion_token, nullptr);
}

TEST(UtilityTests, IoUringSubmitFallbackKeepsInvalidArgumentsFatal) {
    EXPECT_TRUE(af::detail::uring_submit_error_can_fallback(ENOSYS));
    EXPECT_TRUE(af::detail::uring_submit_error_can_fallback(EBUSY));
#ifdef EOPNOTSUPP
    EXPECT_TRUE(af::detail::uring_submit_error_can_fallback(EOPNOTSUPP));
#endif
    EXPECT_FALSE(af::detail::uring_submit_error_can_fallback(EINVAL));
}
