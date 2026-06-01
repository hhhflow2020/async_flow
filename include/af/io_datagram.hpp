#pragma once

#include "af/io_file.hpp"

namespace af {

#define AF_IO_DATAGRAM_DETAIL_INCLUDE 1
#include "af/detail/io_datagram_recv.hpp"
#include "af/detail/io_datagram_send.hpp"
#include "af/detail/io_datagram_vectored.hpp"
#include "af/detail/io_datagram_zero_copy.hpp"
#undef AF_IO_DATAGRAM_DETAIL_INCLUDE

} // namespace af
