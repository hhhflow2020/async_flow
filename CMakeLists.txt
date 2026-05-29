cmake_minimum_required(VERSION 3.24)

project(cpp_async_framework LANGUAGES CXX)

add_library(caf INTERFACE)
add_library(caf::caf ALIAS caf)

target_compile_features(caf INTERFACE cxx_std_20)
target_include_directories(
    caf
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

option(CAF_BUILD_EXAMPLES "Build caf examples" ON)
option(CAF_BUILD_TESTS "Build caf tests" ON)
option(CAF_BUILD_BENCHMARKS "Build caf benchmarks" ON)

if(CAF_BUILD_EXAMPLES)
    add_executable(caf_basic_example examples/basic.cpp)
    target_link_libraries(caf_basic_example PRIVATE caf::caf)
endif()

if(CAF_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    include(GoogleTest)

    add_executable(caf_runtime_tests tests/runtime_tests.cpp)
    target_link_libraries(caf_runtime_tests PRIVATE caf::caf GTest::gtest_main)

    gtest_discover_tests(caf_runtime_tests)
endif()

if(CAF_BUILD_BENCHMARKS)
    find_package(benchmark CONFIG REQUIRED)

    add_executable(caf_runtime_benchmarks benchmarks/runtime_benchmarks.cpp)
    target_link_libraries(caf_runtime_benchmarks PRIVATE caf::caf benchmark::benchmark)
endif()
