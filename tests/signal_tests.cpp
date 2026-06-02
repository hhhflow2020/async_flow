#include <cerrno>
#include <csignal>

#include <gtest/gtest.h>

#include "af/signal.hpp"

#if !defined(_WIN32)
TEST(SignalTests, SignalSetWaitConsumesBlockedRaisedSignal) {
    af::SignalSet signals({SIGUSR1});
    ASSERT_TRUE(signals.valid()) << signals.error();

    ASSERT_EQ(std::raise(SIGUSR1), 0);
    const af::SignalWaitResult result = signals.wait();

    EXPECT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.signal, SIGUSR1);
}

TEST(SignalTests, EmptySignalSetIsInvalid) {
    af::SignalSet signals({});

    EXPECT_FALSE(signals.valid());
    EXPECT_EQ(signals.error(), EINVAL);
}
#endif
