#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "af/async_flow.hpp"

namespace af::test::runtime_scheduler_stress {

#define AF_RUNTIME_SCHEDULER_STRESS_SUPPORT_FRAGMENT_INCLUDE 1
#include "runtime_scheduler_stress_self_post_fragment.hpp"
#include "runtime_scheduler_stress_repeat_hop_fragment.hpp"
#include "runtime_scheduler_stress_wide_hop_fragment.hpp"
#include "runtime_scheduler_stress_parallel_resume_fragment.hpp"
#include "runtime_scheduler_stress_running_pending_fragment.hpp"
#include "runtime_scheduler_stress_running_wake_terminal_fragment.hpp"
#include "runtime_scheduler_stress_wait_fragment.hpp"
#undef AF_RUNTIME_SCHEDULER_STRESS_SUPPORT_FRAGMENT_INCLUDE

} // namespace af::test::runtime_scheduler_stress
