#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "af/async_flow.hpp"

namespace af::test::runtime_scheduler_stress {

#define AF_RUNTIME_SCHEDULER_STRESS_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_scheduler_stress_self_post.hpp"
#include "runtime_scheduler_stress_repeat_hop.hpp"
#include "runtime_scheduler_stress_wide_hop.hpp"
#include "runtime_scheduler_stress_parallel_resume.hpp"
#include "runtime_scheduler_stress_running_pending.hpp"
#include "runtime_scheduler_stress_running_wake_terminal.hpp"
#include "runtime_scheduler_stress_wait.hpp"
#undef AF_RUNTIME_SCHEDULER_STRESS_SUPPORT_DETAIL_INCLUDE

} // namespace af::test::runtime_scheduler_stress
