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
target_link_libraries(asyncflow INTERFACE absl::flat_hash_map)

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

    add_executable(asyncflow_io_epoll_example examples/io_epoll.cpp)
    target_link_libraries(asyncflow_io_epoll_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_timer_example examples/io_timer.cpp)
    target_link_libraries(asyncflow_io_timer_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_event_example examples/io_event.cpp)
    target_link_libraries(asyncflow_io_event_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_adapters_example examples/io_adapters.cpp)
    target_link_libraries(asyncflow_io_adapters_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_file_example examples/io_uring_file.cpp)
    target_link_libraries(asyncflow_io_uring_file_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_file_lifecycle_example examples/io_uring_file_lifecycle.cpp)
    target_link_libraries(asyncflow_io_uring_file_lifecycle_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_openat_example examples/io_uring_openat.cpp)
    target_link_libraries(asyncflow_io_uring_openat_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_uring_datagram_example examples/io_uring_datagram.cpp)
    target_link_libraries(asyncflow_io_uring_datagram_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_tcp_connect_accept_example examples/io_tcp_connect_accept.cpp)
    target_link_libraries(asyncflow_io_tcp_connect_accept_example PRIVATE AsyncFlow::AsyncFlow)

    add_executable(asyncflow_io_vectored_example examples/io_vectored.cpp)
    target_link_libraries(asyncflow_io_vectored_example PRIVATE AsyncFlow::AsyncFlow)
endif()

if(ASYNCFLOW_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    include(GoogleTest)

    add_executable(
        asyncflow_runtime_tests
        tests/runtime_lifecycle_tests.cpp
        tests/runtime_io_tests.cpp
        tests/runtime_parallel_tests.cpp
        tests/runtime_stress_tests.cpp
        tests/utility_tests.cpp
    )
    target_link_libraries(asyncflow_runtime_tests PRIVATE AsyncFlow::AsyncFlow GTest::gtest_main)

    gtest_discover_tests(asyncflow_runtime_tests DISCOVERY_TIMEOUT 30)
endif()

if(ASYNCFLOW_BUILD_BENCHMARKS)
    find_package(benchmark CONFIG REQUIRED)

    add_executable(
        asyncflow_runtime_benchmarks
        benchmarks/benchmark_main.cpp
        benchmarks/io_benchmarks.cpp
        benchmarks/queue_benchmarks.cpp
        benchmarks/runtime_benchmarks.cpp
    )
    target_link_libraries(asyncflow_runtime_benchmarks PRIVATE AsyncFlow::AsyncFlow benchmark::benchmark)
endif()
