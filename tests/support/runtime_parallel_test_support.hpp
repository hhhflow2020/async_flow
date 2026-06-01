#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "af/async_flow.hpp"

namespace af::test::runtime_parallel {

#define AF_RUNTIME_PARALLEL_TEST_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_parallel_core_support.hpp"
#undef AF_RUNTIME_PARALLEL_TEST_SUPPORT_DETAIL_INCLUDE

#define AF_RUNTIME_PARALLEL_TEST_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_parallel_shard_tasks_support.hpp"
#undef AF_RUNTIME_PARALLEL_TEST_SUPPORT_DETAIL_INCLUDE

#define AF_RUNTIME_PARALLEL_TEST_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_parallel_ordered_batch_support.hpp"
#undef AF_RUNTIME_PARALLEL_TEST_SUPPORT_DETAIL_INCLUDE

#define AF_RUNTIME_PARALLEL_TEST_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_parallel_ordered_start_support.hpp"
#undef AF_RUNTIME_PARALLEL_TEST_SUPPORT_DETAIL_INCLUDE

} // namespace af::test::runtime_parallel
