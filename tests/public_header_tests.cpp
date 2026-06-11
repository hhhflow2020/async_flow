#include "af/async_flow.hpp"

#include <type_traits>

#include <gtest/gtest.h>

#if __has_include("af/detail/task/task_io_state.hpp")
#error "legacy task IO facade must not be installed"
#endif

namespace {

#define AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(name, value)                                            \
    template <typename EnumT, typename = void> struct name : std::false_type {};                   \
    template <typename EnumT>                                                                      \
    struct name<EnumT, std::void_t<decltype(EnumT::value)>> : std::true_type {}

AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_submitted_value, Submitted);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_buffered_value, Buffered);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_duplicate_value, Duplicate);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_retry_value, Retry);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_skip_value, Skip);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_stop_value, Stop);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_add_value, Add);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_update_value, Update);
AF_TEST_DEFINE_ENUM_VALUE_DETECTOR(has_delete_value, Delete);

#undef AF_TEST_DEFINE_ENUM_VALUE_DETECTOR

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

    [[nodiscard]] static constexpr bool has_again_helper() noexcept {
        return decltype(test_again<runtime_task_legacy_schedule_probe>(0))::value;
    }

    [[nodiscard]] static constexpr bool has_reschedule_helper() noexcept {
        return decltype(test_reschedule<runtime_task_legacy_schedule_probe>(0))::value;
    }

    [[nodiscard]] static constexpr bool has_cancel_helper() noexcept {
        return decltype(test_cancel<runtime_task_legacy_schedule_probe>(0))::value;
    }

    [[nodiscard]] static constexpr bool has_cancelled_helper() noexcept {
        return decltype(test_cancelled<runtime_task_legacy_schedule_probe>(0))::value;
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

    template <typename TaskT>
    [[nodiscard]] static auto test_again(int) noexcept
        -> decltype(std::declval<TaskT &>().again(), std::true_type{});

    template <typename> [[nodiscard]] static std::false_type test_again(...) noexcept;

    template <typename TaskT>
    [[nodiscard]] static auto test_reschedule(int) noexcept
        -> decltype(std::declval<TaskT &>().reschedule(), std::true_type{});

    template <typename> [[nodiscard]] static std::false_type test_reschedule(...) noexcept;

    template <typename TaskT>
    [[nodiscard]] static auto test_cancel(int) noexcept
        -> decltype(std::declval<TaskT &>().cancel(), std::true_type{});

    template <typename> [[nodiscard]] static std::false_type test_cancel(...) noexcept;

    template <typename TaskT>
    [[nodiscard]] static auto test_cancelled(int) noexcept
        -> decltype(std::declval<TaskT &>().cancelled(), std::true_type{});

    template <typename> [[nodiscard]] static std::false_type test_cancelled(...) noexcept;

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
    static_assert(!runtime_task_legacy_schedule_probe::has_again_helper());
    static_assert(runtime_task_legacy_schedule_probe::has_reschedule_helper());
    static_assert(runtime_task_legacy_schedule_probe::has_cancel_helper());
    static_assert(!runtime_task_legacy_schedule_probe::has_cancelled_helper());
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
    static_assert(af::batch_submit_status::submitted == af::batch_submit_status::submitted);
    static_assert(af::batch_submit_status::buffered == af::batch_submit_status::buffered);
    static_assert(af::batch_submit_status::duplicate == af::batch_submit_status::duplicate);
    static_assert(!has_submitted_value<af::batch_submit_status>::value);
    static_assert(!has_buffered_value<af::batch_submit_status>::value);
    static_assert(!has_duplicate_value<af::batch_submit_status>::value);
    static_assert(std::is_same_v<af::ordered_batch_failure_action, af::OrderedBatchFailureAction>);
    static_assert(af::ordered_batch_failure_action::retry ==
                  af::ordered_batch_failure_action::retry);
    static_assert(af::ordered_batch_failure_action::skip == af::ordered_batch_failure_action::skip);
    static_assert(af::ordered_batch_failure_action::stop == af::ordered_batch_failure_action::stop);
    static_assert(!has_retry_value<af::ordered_batch_failure_action>::value);
    static_assert(!has_skip_value<af::ordered_batch_failure_action>::value);
    static_assert(!has_stop_value<af::ordered_batch_failure_action>::value);
    static_assert(
        std::is_same_v<af::ordered_batch_retry_skip_options, af::OrderedBatchRetrySkipOptions>);
    static_assert(
        std::is_same_v<af::ordered_batch_failure_decision, af::OrderedBatchFailureDecision>);
    static_assert(std::is_same_v<af::ordered_batch_retry_skip_policy<std::uint64_t>,
                                 af::OrderedBatchRetrySkipPolicy<std::uint64_t>>);
    static_assert(std::is_same_v<af::batch_sequencer<int>, af::BatchSequencer<int>>);
    static_assert(std::is_same_v<af::op_type, af::OpType>);
    static_assert(af::op_type::add == af::op_type::add);
    static_assert(af::op_type::update == af::op_type::update);
    static_assert(af::op_type::delete_op == af::op_type::delete_op);
    static_assert(!has_add_value<af::op_type>::value);
    static_assert(!has_update_value<af::op_type>::value);
    static_assert(!has_delete_value<af::op_type>::value);
    static_assert(std::is_same_v<af::crud_op<int, int>, af::CrudOp<int, int>>);
    static_assert(std::is_same_v<af::change_batch<int, int>, af::ChangeBatch<int, int>>);
    static_assert(std::is_same_v<af::SignalWaitResult, af::signal_wait_result>);
    static_assert(std::is_same_v<af::SignalSet, af::signal_set>);
}
