#pragma once

#include "af/io_event_timer.hpp"

namespace af {

#define AF_IO_TIMEOUT_DETAIL_INCLUDE 1
// clang-format off
#include "af/detail/io_timeout_status.hpp"
#include "af/detail/io_timeout_wait.hpp"
#include "af/detail/io_timeout_deadline.hpp"
// clang-format on
#undef AF_IO_TIMEOUT_DETAIL_INCLUDE

} // namespace af
