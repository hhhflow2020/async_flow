#include "af/async_flow.hpp"

#include <type_traits>

#include <gtest/gtest.h>

#if __has_include("af/detail/task/task_io_state.hpp")
#error "legacy task IO facade must not be installed"
#endif

namespace {

class runtime_task_legacy_schedule_probe final : public af::runtime_task {
public:
    runtime_task_legacy_schedule_probe(factory_token token, af::runtime &owner)
        : runtime_task(token, owner) {}

    [[nodiscard]] static constexpr bool has_short_schedule() noexcept {
        return decltype(test_schedule<runtime_task_legacy_schedule_probe>(0))::value;
    }

    [[nodiscard]] static constexpr bool has_short_pending() noexcept {
        return decltype(test_pending<runtime_task_legacy_schedule_probe>(0))::value;
    }

private:
    template <typename TaskT>
    [[nodiscard]] static auto test_schedule(int) noexcept
        -> decltype(std::declval<TaskT &>().schedule(std::declval<af::thread_ref>()),
                    std::true_type{});

    template <typename> [[nodiscard]] static std::false_type test_schedule(...) noexcept;

    template <typename TaskT>
    [[nodiscard]] static auto test_pending(int) noexcept
        -> decltype(std::declval<TaskT &>().pending(std::declval<af::thread_ref>()),
                    std::true_type{});

    template <typename> [[nodiscard]] static std::false_type test_pending(...) noexcept;

    af::task_result run_task() noexcept override {
        return done();
    }
};

} // namespace

TEST(PublicHeaderTests, AsyncFlowUmbrellaExposesRuntimeInstanceApi) {
    af::runtime_config config;
    config.threads = {af::cpu_threads("header", 1)};

    af::runtime runtime(config);
    ASSERT_TRUE(runtime.start());
    EXPECT_EQ(runtime.thread_count(), 1U);
    EXPECT_EQ(runtime.thread_kind_of(runtime.cpu_threads().front()), af::thread_kind::cpu);
    runtime.stop();
}

TEST(PublicHeaderTests, RuntimeTaskExposesOnlyExplicitScheduleNames) {
    static_assert(!runtime_task_legacy_schedule_probe::has_short_schedule());
    static_assert(!runtime_task_legacy_schedule_probe::has_short_pending());
}

TEST(PublicHeaderTests, NetUmbrellaExposesRuntimeNativeApi) {
    static_assert(std::is_class_v<af::net::tcp_server>);
    static_assert(std::is_class_v<af::net::tcp_client>);
    static_assert(std::is_class_v<af::net::udp_socket>);
    static_assert(std::is_class_v<af::net::unix_stream_server>);
    static_assert(std::is_class_v<af::net::unix_datagram_socket>);
}

TEST(PublicHeaderTests, LogUmbrellaExposesLowerCaseNames) {
    static_assert(std::is_same_v<af::LogOverflowPolicy, af::log_overflow_policy>);
    static_assert(std::is_same_v<af::LogOrdering, af::log_ordering>);
    static_assert(std::is_same_v<af::AsyncLogConfig, af::async_log_config>);
    static_assert(std::is_same_v<af::AsyncLogStats, af::async_log_stats>);
    static_assert(std::is_same_v<af::AsyncLogger, af::async_logger>);
    static_assert(std::is_same_v<af::LogBackend, af::log_backend>);
    static_assert(std::is_same_v<af::AsyncLogHandle, af::async_log_handle>);
    af::async_log_config config = af::async_log_config::ordered();
    EXPECT_EQ(config.ordering, af::log_ordering::ordered);
}

TEST(PublicHeaderTests, UtilityUmbrellaExposesLowerCaseNames) {
    static_assert(std::is_same_v<af::Span<int>, af::span<int>>);
    static_assert(std::is_same_v<af::BufferView, af::buffer_view>);
    static_assert(std::is_same_v<af::Buffer, af::buffer>);
    static_assert(std::is_same_v<af::BufferChain, af::buffer_chain>);
    af::buffer buffer = af::buffer::copy("ok", 2);
    af::buffer_chain chain;
    chain.push_back(buffer.slice(0));
    EXPECT_EQ(chain.size(), 2U);
    static_assert(std::is_same_v<af::batch_submit_status, af::BatchSubmitStatus>);
    static_assert(af::batch_submit_status::submitted == af::BatchSubmitStatus::Submitted);
    static_assert(af::batch_submit_status::buffered == af::BatchSubmitStatus::Buffered);
    static_assert(af::batch_submit_status::duplicate == af::BatchSubmitStatus::Duplicate);
    static_assert(std::is_same_v<af::ordered_batch_failure_action, af::OrderedBatchFailureAction>);
    static_assert(af::ordered_batch_failure_action::retry == af::OrderedBatchFailureAction::Retry);
    static_assert(af::ordered_batch_failure_action::skip == af::OrderedBatchFailureAction::Skip);
    static_assert(af::ordered_batch_failure_action::stop == af::OrderedBatchFailureAction::Stop);
    static_assert(
        std::is_same_v<af::ordered_batch_retry_skip_options, af::OrderedBatchRetrySkipOptions>);
    static_assert(
        std::is_same_v<af::ordered_batch_failure_decision, af::OrderedBatchFailureDecision>);
    static_assert(std::is_same_v<af::ordered_batch_retry_skip_policy<std::uint64_t>,
                                 af::OrderedBatchRetrySkipPolicy<std::uint64_t>>);
    static_assert(std::is_same_v<af::batch_sequencer<int>, af::BatchSequencer<int>>);
    static_assert(std::is_same_v<af::op_type, af::OpType>);
    static_assert(af::op_type::add == af::OpType::Add);
    static_assert(af::op_type::update == af::OpType::Update);
    static_assert(af::op_type::delete_op == af::OpType::Delete);
    static_assert(std::is_same_v<af::crud_op<int, int>, af::CrudOp<int, int>>);
    static_assert(std::is_same_v<af::change_batch<int, int>, af::ChangeBatch<int, int>>);
    static_assert(std::is_same_v<af::SignalWaitResult, af::signal_wait_result>);
    static_assert(std::is_same_v<af::SignalSet, af::signal_set>);
}
