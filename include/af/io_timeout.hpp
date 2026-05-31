#pragma once

#include "af/io_event_timer.hpp"

namespace af {

#define AF_IO_TIMEOUT_FRAGMENT_INCLUDE 1
#include "af/detail/io_timeout_status_fragment.hpp"
#include "af/detail/io_timeout_wait_fragment.hpp"
#include "af/detail/io_timeout_deadline_fragment.hpp"
#undef AF_IO_TIMEOUT_FRAGMENT_INCLUDE

} // namespace af
