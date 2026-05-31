#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <type_traits>

#include "af/detail/config.hpp"

namespace af {

#define AF_TASK_FRAGMENT_INCLUDE
#include "af/detail/task_types_fragment.hpp"
#include "af/detail/task_io_state_fragment.hpp"
#include "af/detail/task_registry_fragment.hpp"
#include "af/detail/basic_task_fragment.hpp"
#undef AF_TASK_FRAGMENT_INCLUDE

} // namespace af
