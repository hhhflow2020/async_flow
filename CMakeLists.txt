cmake_minimum_required(VERSION 3.24)

project(AsyncFlow LANGUAGES CXX)

add_library(asyncflow INTERFACE)
add_library(AsyncFlow::AsyncFlow ALIAS asyncflow)

target_compile_features(asyncflow INTERFACE cxx_std_20)
target_include_directories(
    asyncflow
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
find_package(absl CONFIG REQUIRED)
target_link_libraries(
    asyncflow
    INTERFACE
        absl::flat_hash_map
        absl::log
        absl::log_initialize
        absl::log_internal_globals
        absl::log_sink
        absl::log_sink_registry
)

option(ASYNCFLOW_BUILD_EXAMPLES "Build AsyncFlow examples" ON)
option(ASYNCFLOW_BUILD_TESTS "Build AsyncFlow tests" ON)
option(ASYNCFLOW_BUILD_BENCHMARKS "Build AsyncFlow benchmarks" ON)
option(ASYNCFLOW_ENABLE_TSAN "Build targets with ThreadSanitizer instrumentation" OFF)

if(ASYNCFLOW_ENABLE_TSAN)
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
        add_link_options(-fsanitize=thread)
    else()
        message(FATAL_ERROR "ASYNCFLOW_ENABLE_TSAN requires Clang or GCC")
    endif()
endif()

if(ASYNCFLOW_BUILD_EXAMPLES)
    add_executable(asyncflow_basic_example examples/basic.cpp)
    target_link_libraries(asyncflow_basic_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_parallel_shards_example examples/parallel_shards.cpp)
    target_link_libraries(asyncflow_parallel_shards_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_ordered_batches_example examples/ordered_batches.cpp)
    target_link_libraries(asyncflow_ordered_batches_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_crud_apply_example examples/crud_apply.cpp)
    target_link_libraries(asyncflow_crud_apply_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_async_logging_example examples/async_logging.cpp)
    target_link_libraries(asyncflow_async_logging_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_runtime_async_logging_example examples/runtime_async_logging.cpp)
    target_link_libraries(asyncflow_runtime_async_logging_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_runtime_udp_async_logging_example
                   examples/runtime_udp_async_logging.cpp)
    target_link_libraries(asyncflow_runtime_udp_async_logging_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_epoll_example examples/io_epoll.cpp)
    target_link_libraries(asyncflow_io_epoll_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_native_readiness_example examples/io_native_readiness.cpp)
    target_link_libraries(asyncflow_io_native_readiness_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_timer_example examples/io_timer.cpp)
    target_link_libraries(asyncflow_io_timer_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_timeout_example examples/io_timeout.cpp)
    target_link_libraries(asyncflow_io_timeout_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_event_example examples/io_event.cpp)
    target_link_libraries(asyncflow_io_event_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_sendfile_static_example examples/io_sendfile_static.cpp)
    target_link_libraries(asyncflow_io_sendfile_static_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_shutdown_example examples/io_shutdown.cpp)
    target_link_libraries(asyncflow_io_shutdown_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_socket_lifecycle_example examples/io_socket_lifecycle.cpp)
    target_link_libraries(asyncflow_io_socket_lifecycle_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_adapters_example examples/io_adapters.cpp)
    target_link_libraries(asyncflow_io_adapters_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_pollable_client_example examples/io_pollable_client.cpp)
    target_link_libraries(asyncflow_io_pollable_client_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_rpc_length_prefixed_example examples/io_rpc_length_prefixed.cpp)
    target_link_libraries(asyncflow_io_rpc_length_prefixed_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_file_example examples/io_uring_file.cpp)
    target_link_libraries(asyncflow_io_uring_file_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_file_lifecycle_example examples/io_uring_file_lifecycle.cpp)
    target_link_libraries(asyncflow_io_uring_file_lifecycle_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_filesystem_ops_example examples/io_uring_filesystem_ops.cpp)
    target_link_libraries(asyncflow_io_uring_filesystem_ops_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_tuned_setup_example examples/io_uring_tuned_setup.cpp)
    target_link_libraries(asyncflow_io_uring_tuned_setup_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_fixed_buffer_example examples/io_uring_fixed_buffer.cpp)
    target_link_libraries(asyncflow_io_uring_fixed_buffer_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_fixed_file_example examples/io_uring_fixed_file.cpp)
    target_link_libraries(asyncflow_io_uring_fixed_file_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_multishot_accept_example examples/io_uring_multishot_accept.cpp)
    target_link_libraries(asyncflow_io_uring_multishot_accept_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_recv_multishot_example examples/io_uring_recv_multishot.cpp)
    target_link_libraries(asyncflow_io_uring_recv_multishot_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_udp_recv_multishot_example examples/io_uring_udp_recv_multishot.cpp)
    target_link_libraries(asyncflow_io_uring_udp_recv_multishot_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_udp_recvmsg_multishot_example examples/io_uring_udp_recvmsg_multishot.cpp)
    target_link_libraries(asyncflow_io_uring_udp_recvmsg_multishot_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_openat_example examples/io_uring_openat.cpp)
    target_link_libraries(asyncflow_io_uring_openat_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_openat_direct_example examples/io_uring_openat_direct.cpp)
    target_link_libraries(asyncflow_io_uring_openat_direct_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_accept_direct_example examples/io_uring_accept_direct.cpp)
    target_link_libraries(asyncflow_io_uring_accept_direct_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_datagram_example examples/io_uring_datagram.cpp)
    target_link_libraries(asyncflow_io_uring_datagram_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_send_zc_example examples/io_uring_send_zc.cpp)
    target_link_libraries(asyncflow_io_uring_send_zc_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_sendmsg_zc_example examples/io_uring_sendmsg_zc.cpp)
    target_link_libraries(asyncflow_io_uring_sendmsg_zc_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_timeout_example examples/io_uring_timeout.cpp)
    target_link_libraries(asyncflow_io_uring_timeout_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_tcp_connect_accept_example examples/io_tcp_connect_accept.cpp)
    target_link_libraries(asyncflow_io_tcp_connect_accept_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_tcp_echo_server_example examples/io_tcp_echo_server.cpp)
    target_link_libraries(asyncflow_io_tcp_echo_server_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_vectored_example examples/io_vectored.cpp)
    target_link_libraries(asyncflow_io_vectored_example PRIVATE AsyncFlow::AsyncFlow)
endif()

if(ASYNCFLOW_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    include(GoogleTest)

    add_executable(
        asyncflow_runtime_tests
        tests/runtime_lifecycle_basic_tests.cpp
        tests/runtime_config_tests.cpp
        tests/runtime_backpressure_tests.cpp
        tests/runtime_shutdown_policy_tests.cpp
        tests/runtime_io_setup_tests.cpp
        tests/runtime_io_epoll_tests.cpp
        tests/runtime_io_epoll_readiness_tests.cpp
        tests/runtime_io_epoll_cancel_timeout_tests.cpp
        tests/runtime_io_epoll_boundary_tests.cpp
        tests/runtime_io_epoll_socket_lifecycle_tests.cpp
        tests/runtime_io_epoll_event_timer_tests.cpp
        tests/runtime_io_kqueue_tests.cpp
        tests/runtime_io_stream_tests.cpp
        tests/runtime_io_stream_vectored_tests.cpp
        tests/runtime_io_stream_zero_copy_tests.cpp
        tests/runtime_io_stream_transfer_tests.cpp
        tests/runtime_io_stream_connect_accept_tests.cpp
        tests/runtime_io_uring_socket_tests.cpp
        tests/runtime_io_uring_socket_accept_tests.cpp
        tests/runtime_io_uring_socket_datagram_tests.cpp
        tests/runtime_io_uring_socket_stream_multishot_tests.cpp
        tests/runtime_io_uring_socket_udp_multishot_tests.cpp
        tests/runtime_io_uring_socket_recvmsg_multishot_tests.cpp
        tests/runtime_io_uring_socket_stream_tests.cpp
        tests/runtime_io_uring_file_batch_tests.cpp
        tests/runtime_io_uring_file_fixed_tests.cpp
        tests/runtime_io_uring_file_lifecycle_tests.cpp
        tests/runtime_io_uring_file_tests.cpp
        tests/runtime_io_datagram_tests.cpp
        tests/runtime_io_datagram_readiness_tests.cpp
        tests/runtime_io_datagram_send_tests.cpp
        tests/runtime_io_shutdown_tests.cpp
        tests/runtime_parallel_shards_tests.cpp
        tests/runtime_ordered_start_tests.cpp
        tests/runtime_ordered_batch_tests.cpp
        tests/log_tests.cpp
        tests/queue_tests.cpp
        tests/pool_tests.cpp
        tests/io_state_utility_tests.cpp
        tests/batch_utility_tests.cpp
    )
    target_link_libraries(asyncflow_runtime_tests PRIVATE AsyncFlow::AsyncFlow GTest::gtest_main)

    add_executable(
        asyncflow_runtime_stress_tests
        tests/runtime_lifecycle_stress_tests.cpp
        tests/runtime_self_post_stress_tests.cpp
        tests/runtime_cross_thread_stress_tests.cpp
        tests/runtime_parallel_stress_tests.cpp
        tests/runtime_running_pending_stress_tests.cpp
    )
    target_link_libraries(asyncflow_runtime_stress_tests PRIVATE AsyncFlow::AsyncFlow GTest::gtest_main)

    gtest_discover_tests(asyncflow_runtime_tests DISCOVERY_TIMEOUT 30)
    gtest_discover_tests(
        asyncflow_runtime_stress_tests
        DISCOVERY_TIMEOUT 30
        PROPERTIES
            PROCESSORS 8
            RESOURCE_LOCK asyncflow_runtime_stress
    )
endif()

if(ASYNCFLOW_BUILD_BENCHMARKS)
    find_package(benchmark CONFIG REQUIRED)

    add_executable(
        asyncflow_runtime_benchmarks
        benchmarks/benchmark_main.cpp
        benchmarks/io_adapter_datagram_benchmarks.cpp
        benchmarks/io_adapter_resource_benchmarks.cpp
        benchmarks/io_adapter_stream_benchmarks.cpp
        benchmarks/io_filesystem_benchmarks.cpp
        benchmarks/io_zero_copy_benchmarks.cpp
        benchmarks/io_file_benchmarks.cpp
        benchmarks/io_vectored_benchmarks.cpp
        benchmarks/queue_benchmarks.cpp
        benchmarks/runtime_external_start_benchmarks.cpp
        benchmarks/runtime_hop_benchmarks.cpp
        benchmarks/runtime_parallel_benchmarks.cpp
    )
    target_link_libraries(asyncflow_runtime_benchmarks PRIVATE AsyncFlow::AsyncFlow benchmark::benchmark)
endif()
