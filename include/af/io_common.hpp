#pragma once

#include "af/io_types.hpp"

namespace af {

#define AF_IO_COMMON_DETAIL_INCLUDE 1
// clang-format off
#include "af/detail/io_common_base.hpp"
#include "af/detail/io_common_state.hpp"
#include "af/detail/io_common_fixed_file.hpp"
#include "af/detail/io_common_linux_event_timer.hpp"
#include "af/detail/io_common_deadline.hpp"
// clang-format on
#undef AF_IO_COMMON_DETAIL_INCLUDE

} // namespace af
