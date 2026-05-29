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

option(ASYNCFLOW_BUILD_EXAMPLES "Build AsyncFlow examples" ON)
option(ASYNCFLOW_BUILD_TESTS "Build AsyncFlow tests" ON)
option(ASYNCFLOW_BUILD_BENCHMARKS "Build AsyncFlow benchmarks" ON)

if(ASYNCFLOW_BUILD_EXAMPLES)
    add_executable(asyncflow_basic_example examples/basic.cpp)
    target_link_libraries(asyncflow_basic_example PRIVATE AsyncFlow::AsyncFlow)
endif()

if(ASYNCFLOW_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    include(GoogleTest)

    add_executable(asyncflow_runtime_tests tests/runtime_tests.cpp)
    target_link_libraries(asyncflow_runtime_tests PRIVATE AsyncFlow::AsyncFlow GTest::gtest_main)

    gtest_discover_tests(asyncflow_runtime_tests)
endif()

if(ASYNCFLOW_BUILD_BENCHMARKS)
    find_package(benchmark CONFIG REQUIRED)

    add_executable(asyncflow_runtime_benchmarks benchmarks/runtime_benchmarks.cpp)
    target_link_libraries(asyncflow_runtime_benchmarks PRIVATE AsyncFlow::AsyncFlow benchmark::benchmark)
endif()
