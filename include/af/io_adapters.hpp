#pragma once

#include "af/io_timeout.hpp"

namespace af {

#define AF_IO_ADAPTERS_DETAIL_INCLUDE 1
// clang-format off
#include "af/detail/io_adapters_descriptor.hpp"
#include "af/detail/io_adapters_file.hpp"
#include "af/detail/io_adapters_stream_listener.hpp"
#include "af/detail/io_adapters_datagram.hpp"
#include "af/detail/io_adapters_aliases.hpp"
#include "af/detail/io_adapters_event_timer.hpp"
// clang-format on
#undef AF_IO_ADAPTERS_DETAIL_INCLUDE

} // namespace af
