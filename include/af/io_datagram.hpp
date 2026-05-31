#pragma once

#include "af/io_file.hpp"

namespace af {

#define AF_IO_DATAGRAM_FRAGMENT_INCLUDE 1
#include "af/detail/io_datagram_recv_fragment.hpp"
#undef AF_IO_DATAGRAM_FRAGMENT_INCLUDE

#define AF_IO_DATAGRAM_FRAGMENT_INCLUDE 1
#include "af/detail/io_datagram_send_fragment.hpp"
#undef AF_IO_DATAGRAM_FRAGMENT_INCLUDE

#define AF_IO_DATAGRAM_FRAGMENT_INCLUDE 1
#include "af/detail/io_datagram_vectored_fragment.hpp"
#undef AF_IO_DATAGRAM_FRAGMENT_INCLUDE

#define AF_IO_DATAGRAM_FRAGMENT_INCLUDE 1
#include "af/detail/io_datagram_zero_copy_fragment.hpp"
#undef AF_IO_DATAGRAM_FRAGMENT_INCLUDE

} // namespace af
