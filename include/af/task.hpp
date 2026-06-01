#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <type_traits>

#include "af/detail/config.hpp"

namespace af {

#define AF_TASK_DETAIL_INCLUDE
// clang-format off
#include "af/detail/task_types.hpp"
#include "af/detail/task_io_state.hpp"
#include "af/detail/task_registry.hpp"
#include "af/detail/basic_task.hpp"
// clang-format on
#undef AF_TASK_DETAIL_INCLUDE

} // namespace af
