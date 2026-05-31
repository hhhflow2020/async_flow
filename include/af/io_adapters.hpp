#pragma once

#include "af/io_timeout.hpp"

namespace af {

#define AF_IO_ADAPTERS_FRAGMENT_INCLUDE 1
#include "af/detail/io_adapters_descriptor_fragment.hpp"
#undef AF_IO_ADAPTERS_FRAGMENT_INCLUDE

#define AF_IO_ADAPTERS_FRAGMENT_INCLUDE 1
#include "af/detail/io_adapters_file_fragment.hpp"
#undef AF_IO_ADAPTERS_FRAGMENT_INCLUDE

#define AF_IO_ADAPTERS_FRAGMENT_INCLUDE 1
#include "af/detail/io_adapters_stream_listener_fragment.hpp"
#undef AF_IO_ADAPTERS_FRAGMENT_INCLUDE

#define AF_IO_ADAPTERS_FRAGMENT_INCLUDE 1
#include "af/detail/io_adapters_datagram_fragment.hpp"
#undef AF_IO_ADAPTERS_FRAGMENT_INCLUDE

#define AF_IO_ADAPTERS_FRAGMENT_INCLUDE 1
#include "af/detail/io_adapters_aliases_fragment.hpp"
#undef AF_IO_ADAPTERS_FRAGMENT_INCLUDE

#define AF_IO_ADAPTERS_FRAGMENT_INCLUDE 1
#include "af/detail/io_adapters_event_timer_fragment.hpp"
#undef AF_IO_ADAPTERS_FRAGMENT_INCLUDE

} // namespace af
