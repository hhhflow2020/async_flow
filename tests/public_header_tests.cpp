#include "af/async_flow.hpp"

#include <array>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <type_traits>

#include <gtest/gtest.h>

#if __has_include("af/detail/task/task_io_state.hpp")
#error "legacy task IO facade must not be installed"
#endif

namespace {

struct forbidden_source_snippet {
    const char *relative_path;
    const char *snippet;
};

[[nodiscard]] std::string read_source_file(std::string_view relative_path) {
    const std::string path = std::string(ASYNCFLOW_SOURCE_DIR) + "/" + std::string(relative_path);
    std::ifstream input(path);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

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

TEST(PublicHeaderTests, NetPublicHeadersDoNotExposeCamelCaseTypeAliases) {
    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/net/tcp_endpoint.hpp", "using AddressFamily ="},
        forbidden_source_snippet{"include/af/net/tcp_endpoint.hpp", "using IpEndpoint ="},
        forbidden_source_snippet{"include/af/net/tcp_endpoint.hpp", "using TcpEndpoint ="},
        forbidden_source_snippet{"include/af/net/tcp_endpoint.hpp", "using UdpEndpoint ="},
        forbidden_source_snippet{"include/af/net/tcp_endpoint.hpp", "using UnixEndpoint ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using SendResult ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using CloseReason ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using AcceptStrategy ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using ListenerState ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using RemoveListenerPolicy ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using TcpListenerOptions ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using TcpConnectionConfig ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using TcpListenerConfig ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using TcpServerConfig ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using ListenerId ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using TcpListenerHandle ="},
        forbidden_source_snippet{"include/af/net/tcp_types.hpp", "using ListenerResult ="},
        forbidden_source_snippet{"include/af/net/tcp_client_types.hpp", "using TcpClientOptions ="},
        forbidden_source_snippet{"include/af/net/tcp_client_types.hpp",
                                 "using TcpClientRuntimeConfig ="},
        forbidden_source_snippet{"include/af/net/udp_types.hpp", "using UdpSendResult ="},
        forbidden_source_snippet{"include/af/net/udp_types.hpp", "using UdpSocketOptions ="},
        forbidden_source_snippet{"include/af/net/udp_types.hpp", "using UdpSocketRuntimeConfig ="},
        forbidden_source_snippet{"include/af/net/udp_types.hpp", "using UdpPeer ="},
    };

    for (const forbidden_source_snippet item : forbidden) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_EQ(content.find(item.snippet), std::string::npos)
            << item.relative_path << " still contains " << item.snippet;
    }
}

TEST(PublicHeaderTests, UtilityPublicHeadersDoNotExposeCamelCaseTypeAliases) {
    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/buffer/buffer.hpp", "using BufferView ="},
        forbidden_source_snippet{"include/af/buffer/buffer.hpp", "using Buffer ="},
        forbidden_source_snippet{"include/af/buffer/buffer.hpp", "using BufferChain ="},
        forbidden_source_snippet{"include/af/span.hpp", "using Span ="},
        forbidden_source_snippet{"include/af/signal.hpp", "using SignalWaitResult ="},
        forbidden_source_snippet{"include/af/signal.hpp", "using SignalSet ="},
        forbidden_source_snippet{"include/af/parallel.hpp", "using ParallelMode ="},
        forbidden_source_snippet{"include/af/parallel.hpp", "using OrderedBatchReplayPolicy ="},
        forbidden_source_snippet{"include/af/parallel.hpp", "using OrderedBatchOptions ="},
        forbidden_source_snippet{"include/af/parallel.hpp", "using ShardedOps ="},
        forbidden_source_snippet{"include/af/batch_sequencer.hpp", "using BatchSubmitStatus ="},
        forbidden_source_snippet{"include/af/batch_sequencer.hpp",
                                 "using OrderedBatchFailureAction ="},
        forbidden_source_snippet{"include/af/batch_sequencer.hpp",
                                 "using OrderedBatchRetrySkipOptions ="},
        forbidden_source_snippet{"include/af/batch_sequencer.hpp",
                                 "using OrderedBatchFailureDecision ="},
        forbidden_source_snippet{"include/af/batch_sequencer.hpp",
                                 "using OrderedBatchRetrySkipPolicy ="},
        forbidden_source_snippet{"include/af/batch_sequencer.hpp", "using BatchSequencer ="},
        forbidden_source_snippet{"include/af/crud_batch.hpp", "using OpType ="},
        forbidden_source_snippet{"include/af/crud_batch.hpp", "using CrudOp ="},
        forbidden_source_snippet{"include/af/crud_batch.hpp", "using ChangeBatch ="},
    };

    for (const forbidden_source_snippet item : forbidden) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_EQ(content.find(item.snippet), std::string::npos)
            << item.relative_path << " still contains " << item.snippet;
    }
}

TEST(PublicHeaderTests, LogPublicHeadersDoNotExposeCamelCaseTypeAliases) {
    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/detail/log/async_log_config.hpp",
                                 "using LogOverflowPolicy ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_config.hpp",
                                 "using LogOrdering ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_config.hpp",
                                 "using AsyncLogConfig ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_config.hpp",
                                 "using AsyncLogStats ="},
        forbidden_source_snippet{"include/af/detail/log/async_logger.hpp",
                                 "using RuntimeInstanceAbslAsyncLogSink ="},
        forbidden_source_snippet{"include/af/detail/log/async_logger.hpp", "using AsyncLogger ="},
        forbidden_source_snippet{"include/af/detail/log/absl_log_sink.hpp",
                                 "using AsyncLogHandle ="},
        forbidden_source_snippet{"include/af/detail/log/file_log_backend.hpp",
                                 "using FileLogBackendConfig ="},
        forbidden_source_snippet{"include/af/detail/log/file_log_backend.hpp",
                                 "using FileLogBackend ="},
        forbidden_source_snippet{"include/af/detail/log/log_backend.hpp", "using LogBackend ="},
        forbidden_source_snippet{"include/af/detail/log/network_log_backend.hpp",
                                 "using UdpLogBackendConfig ="},
        forbidden_source_snippet{"include/af/detail/log/network_log_backend.hpp",
                                 "using TcpLogBackendConfig ="},
        forbidden_source_snippet{"include/af/detail/log/network_log_backend.hpp",
                                 "using UdpLogBackend ="},
        forbidden_source_snippet{"include/af/detail/log/network_log_backend.hpp",
                                 "using TcpLogBackend ="},
    };

    for (const forbidden_source_snippet item : forbidden) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_EQ(content.find(item.snippet), std::string::npos)
            << item.relative_path << " still contains " << item.snippet;
    }
}

TEST(PublicHeaderTests, LogUmbrellaExposesLowerCaseNames) {
    static_assert(std::is_enum_v<af::log_overflow_policy>);
    static_assert(std::is_enum_v<af::log_ordering>);
    static_assert(std::is_class_v<af::async_log_config>);
    static_assert(std::is_class_v<af::async_log_stats>);
    static_assert(std::is_class_v<af::async_logger>);
    static_assert(std::is_class_v<af::log_backend>);
    static_assert(std::is_class_v<af::async_log_handle>);
    af::async_log_config config = af::async_log_config::ordered();
    EXPECT_EQ(config.ordering, af::log_ordering::ordered);
}

TEST(PublicHeaderTests, UtilityUmbrellaExposesLowerCaseNames) {
    static_assert(std::is_class_v<af::span<int>>);
    static_assert(std::is_class_v<af::buffer_view>);
    static_assert(std::is_class_v<af::buffer>);
    static_assert(std::is_class_v<af::buffer_chain>);
    af::buffer buffer = af::buffer::copy("ok", 2);
    af::buffer_chain chain;
    chain.push_back(buffer.slice(0));
    EXPECT_EQ(chain.size(), 2U);
    static_assert(std::is_enum_v<af::batch_submit_status>);
    static_assert(af::batch_submit_status::submitted == af::batch_submit_status::submitted);
    static_assert(af::batch_submit_status::buffered == af::batch_submit_status::buffered);
    static_assert(af::batch_submit_status::duplicate == af::batch_submit_status::duplicate);
    static_assert(!has_submitted_value<af::batch_submit_status>::value);
    static_assert(!has_buffered_value<af::batch_submit_status>::value);
    static_assert(!has_duplicate_value<af::batch_submit_status>::value);
    static_assert(std::is_enum_v<af::ordered_batch_failure_action>);
    static_assert(af::ordered_batch_failure_action::retry ==
                  af::ordered_batch_failure_action::retry);
    static_assert(af::ordered_batch_failure_action::skip == af::ordered_batch_failure_action::skip);
    static_assert(af::ordered_batch_failure_action::stop == af::ordered_batch_failure_action::stop);
    static_assert(!has_retry_value<af::ordered_batch_failure_action>::value);
    static_assert(!has_skip_value<af::ordered_batch_failure_action>::value);
    static_assert(!has_stop_value<af::ordered_batch_failure_action>::value);
    static_assert(std::is_class_v<af::ordered_batch_retry_skip_options>);
    static_assert(std::is_class_v<af::ordered_batch_failure_decision>);
    static_assert(std::is_class_v<af::ordered_batch_retry_skip_policy<std::uint64_t>>);
    static_assert(std::is_class_v<af::batch_sequencer<int>>);
    static_assert(std::is_enum_v<af::op_type>);
    static_assert(af::op_type::add == af::op_type::add);
    static_assert(af::op_type::update == af::op_type::update);
    static_assert(af::op_type::delete_op == af::op_type::delete_op);
    static_assert(!has_add_value<af::op_type>::value);
    static_assert(!has_update_value<af::op_type>::value);
    static_assert(!has_delete_value<af::op_type>::value);
    static_assert(std::is_class_v<af::crud_op<int, int>>);
    static_assert(std::is_class_v<af::change_batch<int, int>>);
    static_assert(std::is_class_v<af::signal_wait_result>);
    static_assert(std::is_class_v<af::signal_set>);
}
