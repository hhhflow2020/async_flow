#pragma once

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <type_traits>

#include "af/detail/config.hpp"
#include "af/thread_kind.hpp"

namespace af {

// clang-format off
#include "af/detail/task/task_types.hpp"
// clang-format on

} // namespace af

#include "af/thread_layout.hpp"

namespace af {

// clang-format off
#include "af/detail/task/task_io_state.hpp"
#include "af/detail/task/task_registry.hpp"
#include "af/detail/task/basic_task.hpp"
// clang-format on

} // namespace af
