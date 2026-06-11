#include <cerrno>
#include <chrono>
#include <csignal>

#include <gtest/gtest.h>

#include "af/signal.hpp"

TEST(SignalTests, SignalSetWaitConsumesBlockedRaisedSignal) {
    af::signal_set signals({SIGUSR1});
    ASSERT_TRUE(signals.valid()) << signals.error();

    ASSERT_EQ(std::raise(SIGUSR1), 0);
    const af::signal_wait_result result = signals.wait();

    EXPECT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.signal, SIGUSR1);
}

TEST(SignalTests, SignalSetTryWaitTimesOutWhenNoSignalIsPending) {
    af::signal_set signals({SIGUSR2});
    ASSERT_TRUE(signals.valid()) << signals.error();

    const af::signal_wait_result result = signals.try_wait();

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error, EAGAIN);
}

TEST(SignalTests, SignalSetWaitForTimesOutWhenNoSignalIsPending) {
    af::signal_set signals({SIGUSR2});
    ASSERT_TRUE(signals.valid()) << signals.error();

    const af::signal_wait_result result = signals.wait_for(std::chrono::milliseconds(1));

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error, EAGAIN);
}

TEST(SignalTests, SignalSetWaitForConsumesBlockedRaisedSignal) {
    af::signal_set signals({SIGUSR1});
    ASSERT_TRUE(signals.valid()) << signals.error();

    ASSERT_EQ(std::raise(SIGUSR1), 0);
    const af::signal_wait_result result = signals.wait_for(std::chrono::seconds(1));

    EXPECT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.signal, SIGUSR1);
}

TEST(SignalTests, SignalSetWaitUntilConsumesBlockedRaisedSignal) {
    af::signal_set signals({SIGUSR1});
    ASSERT_TRUE(signals.valid()) << signals.error();

    ASSERT_EQ(std::raise(SIGUSR1), 0);
    const af::signal_wait_result result =
        signals.wait_until(std::chrono::steady_clock::now() + std::chrono::seconds(1));

    EXPECT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.signal, SIGUSR1);
}

TEST(SignalTests, TerminationSignalSetConsumesBlockedTerminationSignal) {
    af::signal_set signals = af::make_termination_signal_set();
    ASSERT_TRUE(signals.valid()) << signals.error();

    ASSERT_EQ(std::raise(SIGTERM), 0);
    const af::signal_wait_result result = signals.wait_for(std::chrono::seconds(1));

    EXPECT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.signal, SIGTERM);
}

TEST(SignalTests, TerminationSignalSetConsumesBlockedInterruptSignal) {
    af::signal_set signals = af::make_termination_signal_set();
    ASSERT_TRUE(signals.valid()) << signals.error();

    ASSERT_EQ(std::raise(SIGINT), 0);
    const af::signal_wait_result result = signals.wait_for(std::chrono::seconds(1));

    EXPECT_TRUE(result.ok()) << result.error;
    EXPECT_EQ(result.signal, SIGINT);
}

TEST(SignalTests, EmptySignalSetIsInvalid) {
    af::signal_set signals({});

    EXPECT_FALSE(signals.valid());
    EXPECT_EQ(signals.error(), EINVAL);
}

TEST(SignalTests, UncatchableSignalsAreInvalid) {
    af::signal_set kill_signals({SIGKILL});
    af::signal_set stop_signals({SIGSTOP});

    EXPECT_FALSE(kill_signals.valid());
    EXPECT_EQ(kill_signals.error(), EINVAL);
    EXPECT_FALSE(stop_signals.valid());
    EXPECT_EQ(stop_signals.error(), EINVAL);
}
