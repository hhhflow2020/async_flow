#pragma once

#include "af/io_types.hpp"

namespace af {

#define AF_IO_COMMON_FRAGMENT_INCLUDE 1
#include "af/detail/io_common_detail_base_fragment.hpp"
#include "af/detail/io_common_detail_state_fragment.hpp"
#include "af/detail/io_common_fixed_file_fragment.hpp"
#include "af/detail/io_common_linux_event_timer_fragment.hpp"
#include "af/detail/io_common_deadline_fragment.hpp"
#undef AF_IO_COMMON_FRAGMENT_INCLUDE

} // namespace af
