#include "af/async_flow.hpp"

#include <array>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

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
    static_assert(std::is_same_v<decltype(af::make_task<runtime_task_legacy_schedule_probe>(
                                     std::declval<af::runtime &>())),
                                 runtime_task_legacy_schedule_probe *>);
    static_assert(std::is_same_v<decltype(af::try_make_task<runtime_task_legacy_schedule_probe>(
                                     std::declval<af::runtime &>())),
                                 runtime_task_legacy_schedule_probe *>);
}

TEST(PublicHeaderTests, TaskPublicHeadersDoNotExposeCamelCaseTypeAliases) {
    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/detail/task/task_types.hpp", "using TaskResult ="},
        forbidden_source_snippet{"include/af/detail/task/task_types.hpp", "using ShutdownPolicy ="},
        forbidden_source_snippet{"include/af/detail/task/task_types.hpp", "using TaskState ="},
    };

    for (const forbidden_source_snippet item : forbidden) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_EQ(content.find(item.snippet), std::string::npos)
            << item.relative_path << " still contains " << item.snippet;
    }
}

TEST(PublicHeaderTests, RuntimeParallelPublicHeaderDoesNotExposeCamelCaseTypeAliases) {
    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/runtime/parallel.hpp",
                                 "using RuntimeInstanceParallelGroup ="},
        forbidden_source_snippet{"include/af/runtime/parallel.hpp",
                                 "using RuntimeInstanceParallelGroupPool ="},
    };

    for (const forbidden_source_snippet item : forbidden) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_EQ(content.find(item.snippet), std::string::npos)
            << item.relative_path << " still contains " << item.snippet;
    }
}

TEST(PublicHeaderTests, ObjectPoolDetailHeadersDoNotExposeCamelCaseTypeAliases) {
    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/detail/memory/object_pool.hpp", "using ObjectPool ="},
        forbidden_source_snippet{"include/af/detail/memory/object_pool_core.hpp",
                                 "using ObjectPoolCore ="},
        forbidden_source_snippet{"include/af/detail/memory/object_pool_block.hpp",
                                 "using ObjectPoolBlockLayout ="},
        forbidden_source_snippet{"include/af/detail/memory/object_pool_local_cache.hpp",
                                 "using ObjectPoolLocalCache ="},
        forbidden_source_snippet{"include/af/detail/memory/object_pool_local_cache.hpp",
                                 "using ObjectPoolDirectReleaseSet ="},
        forbidden_source_snippet{"include/af/detail/memory/object_pool_local_cache.hpp",
                                 "using ObjectPoolSingleLocalCacheSet ="},
        forbidden_source_snippet{"include/af/detail/memory/object_pool_local_cache.hpp",
                                 "using ObjectPoolMultiLocalCacheSet ="},
        forbidden_source_snippet{"include/af/detail/memory/object_pool_local_cache.hpp",
                                 "using ObjectPoolLocalCacheSet ="},
    };

    for (const forbidden_source_snippet item : forbidden) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_EQ(content.find(item.snippet), std::string::npos)
            << item.relative_path << " still contains " << item.snippet;
    }
}

TEST(PublicHeaderTests, InfrastructureDetailHeadersDoNotExposeCamelCaseTypeAliases) {
    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/detail/memory/contiguous_object_storage.hpp",
                                 "using ContiguousObjectStorage ="},
        forbidden_source_snippet{"include/af/detail/net/socket_address.hpp",
                                 "using SocketAddress ="},
        forbidden_source_snippet{"include/af/detail/queue/bounded_mpsc_queue.hpp",
                                 "using BoundedMpscQueue ="},
        forbidden_source_snippet{"include/af/detail/queue/bounded_mpmc_queue.hpp",
                                 "using BoundedMpmcQueue ="},
        forbidden_source_snippet{"include/af/detail/queue/intrusive_mpsc_queue.hpp",
                                 "using IntrusiveMpscNode ="},
        forbidden_source_snippet{"include/af/detail/queue/intrusive_mpsc_queue.hpp",
                                 "using IntrusiveMpscQueue ="},
        forbidden_source_snippet{"include/af/detail/queue/queue_backoff.hpp",
                                 "using QueueFullBackoff ="},
        forbidden_source_snippet{"include/af/detail/runtime/runtime_common_state.hpp",
                                 "using CacheLineAtomic ="},
        forbidden_source_snippet{"include/af/detail/runtime/runtime_common_state.hpp",
                                 "using OrderedBatchState ="},
        forbidden_source_snippet{"include/af/detail/runtime/runtime_service_task.hpp",
                                 "using RuntimeServiceTask ="},
        forbidden_source_snippet{"include/af/runtime/detail/pooled_object.hpp",
                                 "using RuntimePooledObjectPool ="},
        forbidden_source_snippet{"include/af/runtime/detail/pooled_object.hpp",
                                 "using RuntimePooledObjectPoolHolder ="},
        forbidden_source_snippet{"include/af/runtime/detail/task_pool.hpp",
                                 "using RuntimeTaskPool ="},
        forbidden_source_snippet{"include/af/runtime/detail/task_pool.hpp",
                                 "using RuntimeTaskPoolHolder ="},
        forbidden_source_snippet{"include/af/runtime/detail/timer_backend.hpp",
                                 "using RuntimeTimerEntry ="},
        forbidden_source_snippet{"include/af/runtime/detail/timer_backend.hpp",
                                 "using RuntimeTimerHeap ="},
        forbidden_source_snippet{"include/af/runtime/detail/timer_backend.hpp",
                                 "using RuntimeHierarchicalTimerWheel ="},
    };

    for (const forbidden_source_snippet item : forbidden) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_EQ(content.find(item.snippet), std::string::npos)
            << item.relative_path << " still contains " << item.snippet;
    }
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

TEST(PublicHeaderTests, ThreadLayoutPublicHeaderDoesNotExposeCamelCaseTypeAliases) {
    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using Layout ="},
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using ThreadId ="},
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using Thread ="},
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using ThreadGroup ="},
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using Tag ="},
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using ThreadGroupSpec ="},
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using ThreadGroupShape ="},
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using ThreadLayoutShape ="},
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using ThreadLayoutEntry ="},
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using ThreadShape ="},
        forbidden_source_snippet{"include/af/thread_layout.hpp", "using ThreadLayout ="},
    };

    for (const forbidden_source_snippet item : forbidden) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_EQ(content.find(item.snippet), std::string::npos)
            << item.relative_path << " still contains " << item.snippet;
    }
}

TEST(PublicHeaderTests, PublicHeadersDoNotExposeLegacyRuntimeTemplateThreadHelpers) {
    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/platform.hpp", "RuntimeT::Thread"},
        forbidden_source_snippet{"include/af/platform.hpp", "runtime_io_backend_name("},
        forbidden_source_snippet{"include/af/net/thread_list.hpp", "typename Runtime::Thread"},
        forbidden_source_snippet{"include/af/net/thread_list.hpp", "template <typename Runtime"},
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

TEST(PublicHeaderTests, LogDetailHeadersDoNotExposeCamelCaseTypeAliases) {
    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/detail/log/log_record.hpp", "using LogRecord ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_drain_waiter.hpp",
                                 "using AsyncLogDrainWaiter ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_record_pool.hpp",
                                 "using AsyncLogRecordPoolKind ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_record_pool.hpp",
                                 "using AsyncLogRecordPoolSlot ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_record_pool.hpp",
                                 "using AsyncLogRecordPool ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_lanes.hpp",
                                 "using AsyncLogStatCounter ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_lanes.hpp",
                                 "using AsyncLogQueueShard ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_lanes.hpp",
                                 "using AsyncLogOrderedQueue ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_lanes.hpp",
                                 "using AsyncLogProducerShard ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_lanes.hpp",
                                 "using AsyncLogRuntimeLane ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_lanes.hpp",
                                 "using AsyncLogQueueShardStorage ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_lanes.hpp",
                                 "using AsyncLogProducerShardStorage ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_lanes.hpp",
                                 "using AsyncLogRuntimeLaneStorage ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_consumer.hpp",
                                 "using AsyncLogConsumerWakeTarget ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_consumer.hpp",
                                 "using AsyncLogConsumerController ="},
        forbidden_source_snippet{"include/af/detail/log/async_log_consumer.hpp",
                                 "using RuntimeInstanceAsyncLogConsumerController ="},
        forbidden_source_snippet{"include/af/detail/log/network_log_backend.hpp",
                                 "using LogMmsgHeader ="},
        forbidden_source_snippet{"include/af/detail/log/runtime_instance_async_log_consumer.hpp",
                                 "using RuntimeInstanceAsyncLogConsumerControlOperation ="},
        forbidden_source_snippet{"include/af/detail/log/runtime_instance_async_log_consumer.hpp",
                                 "using RuntimeInstanceAsyncLogConsumerControlCompletion ="},
        forbidden_source_snippet{"include/af/detail/log/runtime_instance_async_log_consumer.hpp",
                                 "using RuntimeInstanceAsyncLogConsumerControlTask ="},
        forbidden_source_snippet{"include/af/detail/log/runtime_bound_log_backend.hpp",
                                 "using Batch ="},
        forbidden_source_snippet{"include/af/detail/log/runtime_bound_log_backend.hpp",
                                 "using ControlOperation ="},
        forbidden_source_snippet{"include/af/detail/log/runtime_bound_log_backend.hpp",
                                 "using ControlCompletion ="},
        forbidden_source_snippet{"include/af/detail/log/runtime_bound_log_backend.hpp",
                                 "using RuntimeBoundLogBatch ="},
        forbidden_source_snippet{"include/af/detail/log/runtime_bound_log_backend.hpp",
                                 "using RuntimeBoundLogBackendConfig ="},
        forbidden_source_snippet{"include/af/detail/log/runtime_bound_log_backend.hpp",
                                 "using RuntimeBoundLogBackendStats ="},
        forbidden_source_snippet{"include/af/detail/log/runtime_bound_log_backend.hpp",
                                 "using RuntimeBoundLogBackend ="},
    };

    for (const forbidden_source_snippet item : forbidden) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_EQ(content.find(item.snippet), std::string::npos)
            << item.relative_path << " still contains " << item.snippet;
    }
}

TEST(PublicHeaderTests, LogBenchmarksCoverHighConcurrencyOrderedProducerRegression) {
    const std::string content = read_source_file("benchmarks/log_benchmarks.cpp");
    ASSERT_FALSE(content.empty());

    const std::size_t ordered_begin =
        content.find("BENCHMARK(BM_AsyncLoggerOrderedExternalProducers)");
    ASSERT_NE(ordered_begin, std::string::npos);
    const std::size_t relaxed_begin =
        content.find("BENCHMARK(BM_AsyncLoggerRelaxedExternalProducers)");
    ASSERT_NE(relaxed_begin, std::string::npos);
    ASSERT_LT(ordered_begin, relaxed_begin);

    const std::string ordered_block = content.substr(ordered_begin, relaxed_begin - ordered_begin);
    EXPECT_NE(ordered_block.find("->Args({16, 4096})"), std::string::npos);
    EXPECT_NE(ordered_block.find("->Args({32, 2048})"), std::string::npos);
}

TEST(PublicHeaderTests, ReactorBenchmarksCoverPollBatchSizes) {
    const std::string cmake = read_source_file("CMakeLists.txt");
    ASSERT_FALSE(cmake.empty());
    EXPECT_NE(cmake.find("benchmarks/reactor_benchmarks.cpp"), std::string::npos);

    const std::string content = read_source_file("benchmarks/reactor_benchmarks.cpp");
    ASSERT_FALSE(content.empty());
    EXPECT_NE(content.find("BM_ReactorSelectReadyBatchDispatch"), std::string::npos);
    EXPECT_NE(content.find("BM_ReactorAutoReadyBatchDispatch"), std::string::npos);
    EXPECT_NE(content.find("BM_ReactorEpollReadyBatchDispatch"), std::string::npos);
    EXPECT_NE(content.find("BM_ReactorKqueueReadyBatchDispatch"), std::string::npos);
}

TEST(PublicHeaderTests, EpollReactorBatchesReadySourcesBeforeDispatch) {
    const std::string content = read_source_file("include/af/runtime/detail/epoll_reactor.hpp");
    ASSERT_FALSE(content.empty());
    EXPECT_NE(content.find("ready_sources_"), std::string::npos);
    EXPECT_NE(content.find("ready_events_"), std::string::npos);
    EXPECT_NE(content.find("append_ready"), std::string::npos);
}

TEST(PublicHeaderTests, HotRuntimeAtomicsUseCacheLineWrappers) {
    constexpr std::array wrapped{
        forbidden_source_snippet{"include/af/runtime/detail/executor.hpp",
                                 "cache_line_atomic<std::size_t> queued_work_count_"},
        forbidden_source_snippet{"include/af/runtime/detail/executor.hpp",
                                 "cache_line_atomic<std::uint32_t> wake_epoch_"},
        forbidden_source_snippet{"include/af/runtime/detail/executor.hpp",
                                 "cache_line_atomic<bool> stop_requested_"},
        forbidden_source_snippet{"include/af/detail/log/async_logger.hpp",
                                 "cache_line_atomic<bool> started_"},
        forbidden_source_snippet{"include/af/detail/log/async_logger.hpp",
                                 "cache_line_atomic<bool> accepting_"},
        forbidden_source_snippet{"include/af/detail/log/async_logger.hpp",
                                 "cache_line_atomic<bool> stopping_"},
        forbidden_source_snippet{
            "include/af/detail/log/async_logger.hpp",
            "cache_line_atomic<detail::async_log_consumer_wake_target *> consumer_wake_target_"},
        forbidden_source_snippet{"include/af/detail/log/async_logger.hpp",
                                 "cache_line_atomic<std::size_t> next_ordered_producer_shard_"},
        forbidden_source_snippet{"include/af/detail/log/async_logger.hpp",
                                 "cache_line_atomic<std::size_t> next_producer_shard_"},
        forbidden_source_snippet{"include/af/detail/log/async_logger.hpp",
                                 "cache_line_atomic<std::size_t> pending_"},
        forbidden_source_snippet{"include/af/detail/log/async_logger.hpp",
                                 "cache_line_atomic<std::size_t> ready_"},
        forbidden_source_snippet{"include/af/runtime/detail/pooled_object.hpp",
                                 "cache_line_atomic<std::size_t> reserved_slots"},
    };

    for (const forbidden_source_snippet item : wrapped) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_NE(content.find(item.snippet), std::string::npos)
            << item.relative_path << " should contain " << item.snippet;
    }

    constexpr std::array forbidden{
        forbidden_source_snippet{"include/af/runtime/detail/executor.hpp",
                                 "alignas(hardware_cache_line_size) std::atomic"},
        forbidden_source_snippet{"include/af/detail/log/async_logger.hpp",
                                 "alignas(detail::hardware_cache_line_size) std::atomic"},
    };

    for (const forbidden_source_snippet item : forbidden) {
        const std::string content = read_source_file(item.relative_path);
        ASSERT_FALSE(content.empty()) << item.relative_path;
        EXPECT_EQ(content.find(item.snippet), std::string::npos)
            << item.relative_path << " still contains " << item.snippet;
    }
}

TEST(PublicHeaderTests, TcpBenchmarksCoverEchoRoundTrip) {
    const std::string cmake = read_source_file("CMakeLists.txt");
    ASSERT_FALSE(cmake.empty());
    EXPECT_NE(cmake.find("benchmarks/tcp_benchmarks.cpp"), std::string::npos);

    const std::string content = read_source_file("benchmarks/tcp_benchmarks.cpp");
    ASSERT_FALSE(content.empty());
    EXPECT_NE(content.find("BM_TcpEchoRoundTrip"), std::string::npos);
    EXPECT_NE(content.find("BM_TcpEchoMultiConnectionRoundTrip"), std::string::npos);
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
