#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <type_traits>

#include <gtest/gtest.h>

#include "af/async_flow.hpp"

namespace af::test::runtime_lifecycle {

#define AF_RUNTIME_LIFECYCLE_TEST_SUPPORT_FRAGMENT_INCLUDE 1
#include "runtime_lifecycle_base_support_fragment.hpp"
#undef AF_RUNTIME_LIFECYCLE_TEST_SUPPORT_FRAGMENT_INCLUDE

#define AF_RUNTIME_LIFECYCLE_TEST_SUPPORT_FRAGMENT_INCLUDE 1
#include "runtime_lifecycle_backpressure_support_fragment.hpp"
#undef AF_RUNTIME_LIFECYCLE_TEST_SUPPORT_FRAGMENT_INCLUDE

#define AF_RUNTIME_LIFECYCLE_TEST_SUPPORT_FRAGMENT_INCLUDE 1
#include "runtime_lifecycle_shutdown_support_fragment.hpp"
#undef AF_RUNTIME_LIFECYCLE_TEST_SUPPORT_FRAGMENT_INCLUDE

} // namespace af::test::runtime_lifecycle
