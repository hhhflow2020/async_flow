#include "af/async_flow.hpp"

#include <type_traits>

#include <gtest/gtest.h>

TEST(PublicHeaderTests, AsyncFlowUmbrellaExposesRuntimeInstanceApi) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("header", 1)};

    af::runtime runtime(config);
    ASSERT_TRUE(runtime.start());
    EXPECT_EQ(runtime.thread_count(), 1U);
    EXPECT_EQ(runtime.thread_kind_of(runtime.cpu_threads().front()), af::thread_kind::cpu);
    runtime.stop();
}

TEST(PublicHeaderTests, NetUmbrellaExposesRuntimeNativeApi) {
    static_assert(std::is_class_v<af::net::tcp_server>);
    static_assert(std::is_class_v<af::net::tcp_client>);
    static_assert(std::is_class_v<af::net::udp_socket>);
    static_assert(std::is_class_v<af::net::unix_stream_server>);
    static_assert(std::is_class_v<af::net::unix_datagram_socket>);
}

TEST(PublicHeaderTests, LogUmbrellaExposesLowerCaseNames) {
    static_assert(std::is_same_v<af::log_overflow_policy, af::LogOverflowPolicy>);
    static_assert(std::is_same_v<af::log_ordering, af::LogOrdering>);
    static_assert(std::is_same_v<af::async_log_config, af::AsyncLogConfig>);
    static_assert(std::is_same_v<af::async_log_stats, af::AsyncLogStats>);
    static_assert(std::is_same_v<af::async_logger, af::AsyncLogger>);
    static_assert(std::is_same_v<af::log_backend, af::LogBackend>);
    static_assert(std::is_same_v<af::async_log_handle, af::AsyncLogHandle>);
}

TEST(PublicHeaderTests, UtilityUmbrellaExposesLowerCaseNames) {
    static_assert(std::is_same_v<af::buffer_view, af::BufferView>);
    static_assert(std::is_same_v<af::buffer, af::Buffer>);
    static_assert(std::is_same_v<af::buffer_chain, af::BufferChain>);
    static_assert(std::is_same_v<af::batch_submit_status, af::BatchSubmitStatus>);
    static_assert(std::is_same_v<af::ordered_batch_failure_action, af::OrderedBatchFailureAction>);
    static_assert(
        std::is_same_v<af::ordered_batch_retry_skip_options, af::OrderedBatchRetrySkipOptions>);
    static_assert(
        std::is_same_v<af::ordered_batch_failure_decision, af::OrderedBatchFailureDecision>);
    static_assert(std::is_same_v<af::ordered_batch_retry_skip_policy<std::uint64_t>,
                                 af::OrderedBatchRetrySkipPolicy<std::uint64_t>>);
    static_assert(std::is_same_v<af::batch_sequencer<int>, af::BatchSequencer<int>>);
    static_assert(std::is_same_v<af::op_type, af::OpType>);
    static_assert(std::is_same_v<af::crud_op<int, int>, af::CrudOp<int, int>>);
    static_assert(std::is_same_v<af::change_batch<int, int>, af::ChangeBatch<int, int>>);
    static_assert(std::is_same_v<af::signal_wait_result, af::SignalWaitResult>);
    static_assert(std::is_same_v<af::signal_set, af::SignalSet>);
}
