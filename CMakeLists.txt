cmake_minimum_required(VERSION 3.24)

project(AsyncFlow LANGUAGES CXX)

add_library(asyncflow INTERFACE)
add_library(AsyncFlow::AsyncFlow ALIAS asyncflow)

target_compile_features(asyncflow INTERFACE cxx_std_17)
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

    add_executable(asyncflow_runtime_async_logging_example examples/runtime_async_logging.cpp)
    target_link_libraries(asyncflow_runtime_async_logging_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_runtime_file_async_logging_example
                   examples/runtime_file_async_logging.cpp)
    target_link_libraries(asyncflow_runtime_file_async_logging_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_runtime_udp_async_logging_example
                   examples/runtime_udp_async_logging.cpp)
    target_link_libraries(asyncflow_runtime_udp_async_logging_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_runtime_tcp_async_logging_example
                   examples/runtime_tcp_async_logging.cpp)
    target_link_libraries(asyncflow_runtime_tcp_async_logging_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_net_tcp_echo_server_example examples/net_tcp_echo_server.cpp)
    target_link_libraries(asyncflow_net_tcp_echo_server_example PRIVATE AsyncFlow::AsyncFlow)

    find_package(Protobuf QUIET)
    if(Protobuf_FOUND)
        set(ASYNCFLOW_LOGIN_PROTOC_HINTS)
        foreach(ASYNCFLOW_PROTOBUF_INCLUDE_DIR IN LISTS Protobuf_INCLUDE_DIRS protobuf_INCLUDE_DIRS)
            get_filename_component(ASYNCFLOW_PROTOBUF_PACKAGE_ROOT
                                   "${ASYNCFLOW_PROTOBUF_INCLUDE_DIR}" DIRECTORY)
            list(APPEND ASYNCFLOW_LOGIN_PROTOC_HINTS "${ASYNCFLOW_PROTOBUF_PACKAGE_ROOT}/bin")
        endforeach()
        find_program(
            ASYNCFLOW_LOGIN_PROTOC_PROGRAM
            NAMES protoc
            HINTS ${ASYNCFLOW_LOGIN_PROTOC_HINTS}
            NO_CACHE
        )
        if(ASYNCFLOW_LOGIN_PROTOC_PROGRAM)
            set(
                Protobuf_PROTOC_EXECUTABLE
                "${ASYNCFLOW_LOGIN_PROTOC_PROGRAM}"
                CACHE FILEPATH "The protoc compiler" FORCE
            )
        endif()

        set(ASYNCFLOW_LOGIN_PROTOC_EXE)
        if(Protobuf_PROTOC_EXECUTABLE)
            set(ASYNCFLOW_LOGIN_PROTOC_EXE ${Protobuf_PROTOC_EXECUTABLE})
        elseif(TARGET protobuf::protoc)
            set(ASYNCFLOW_LOGIN_PROTOC_EXE protobuf::protoc)
        elseif(TARGET Protobuf::protoc)
            set(ASYNCFLOW_LOGIN_PROTOC_EXE Protobuf::protoc)
        endif()
        if(ASYNCFLOW_LOGIN_PROTOC_EXE)
            set(ASYNCFLOW_LOGIN_PROTOC_ARGS PROTOC_EXE ${ASYNCFLOW_LOGIN_PROTOC_EXE})
        else()
            set(ASYNCFLOW_LOGIN_PROTOC_ARGS)
        endif()

        protobuf_generate(
            LANGUAGE cpp
            OUT_VAR ASYNCFLOW_LOGIN_PROTO_SRCS
            PROTOS examples/net/login.proto
            IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/examples/net
            PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}
            ${ASYNCFLOW_LOGIN_PROTOC_ARGS}
        )
        add_executable(
            asyncflow_net_tcp_login_server_example
            examples/net_tcp_login_server.cpp
            ${ASYNCFLOW_LOGIN_PROTO_SRCS}
        )
        target_include_directories(
            asyncflow_net_tcp_login_server_example
            PRIVATE
                ${CMAKE_CURRENT_BINARY_DIR}
        )
        target_link_libraries(asyncflow_net_tcp_login_server_example PRIVATE AsyncFlow::AsyncFlow)
        if(TARGET protobuf::libprotobuf)
            target_link_libraries(asyncflow_net_tcp_login_server_example PRIVATE protobuf::libprotobuf)
        elseif(TARGET Protobuf::libprotobuf)
            target_link_libraries(asyncflow_net_tcp_login_server_example PRIVATE Protobuf::libprotobuf)
        else()
            target_include_directories(
                asyncflow_net_tcp_login_server_example
                PRIVATE
                    ${Protobuf_INCLUDE_DIRS}
            )
            target_link_libraries(
                asyncflow_net_tcp_login_server_example
                PRIVATE
                    ${Protobuf_LIBRARIES}
            )
        endif()
    endif()

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
        tests/runtime_parallel_shards_tests.cpp
        tests/runtime_ordered_start_tests.cpp
        tests/runtime_ordered_batch_tests.cpp
        tests/queue_tests.cpp
        tests/pool_tests.cpp
        tests/signal_tests.cpp
        tests/batch_utility_tests.cpp
        tests/net_buffer_tests.cpp
        tests/net_socket_address_tests.cpp
        tests/net_tcp_client_tests.cpp
        tests/net_tcp_server_tests.cpp
        tests/net_udp_socket_tests.cpp
        tests/net_unix_socket_tests.cpp
    )
    target_link_libraries(asyncflow_runtime_tests PRIVATE AsyncFlow::AsyncFlow GTest::gtest_main)

    add_executable(asyncflow_log_tests tests/log_tests.cpp)
    target_link_libraries(asyncflow_log_tests PRIVATE AsyncFlow::AsyncFlow GTest::gtest_main)

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
        asyncflow_log_tests
        DISCOVERY_TIMEOUT 30
        PROPERTIES
            RESOURCE_LOCK asyncflow_log_tests
    )
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
        benchmarks/log_benchmarks.cpp
        benchmarks/queue_benchmarks.cpp
        benchmarks/runtime_external_start_benchmarks.cpp
        benchmarks/runtime_hop_benchmarks.cpp
        benchmarks/runtime_parallel_benchmarks.cpp
    )
    target_link_libraries(asyncflow_runtime_benchmarks PRIVATE AsyncFlow::AsyncFlow benchmark::benchmark)
endif()
