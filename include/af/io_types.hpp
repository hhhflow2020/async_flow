#pragma once
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <limits>
#include <type_traits>
#include <utility>

#include "af/task.hpp"
#include "af/detail/io/types/io_types_platform.hpp"

namespace af {

#include "af/detail/io/types/io_types_base.hpp"
#include "af/detail/io/types/io_types_status.hpp"
#include "af/detail/io/types/io_types_unique_fd.hpp"

} // namespace af
