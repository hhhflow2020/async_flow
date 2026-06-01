#pragma once

#include "af/io_socket.hpp"

namespace af {

#define AF_IO_FILE_DETAIL_INCLUDE 1
// clang-format off
#include "af/detail/io_file_read.hpp"
#include "af/detail/io_file_positioned.hpp"
#include "af/detail/io_file_fixed_buffer.hpp"
#include "af/detail/io_file_lifecycle.hpp"
#include "af/detail/io_file_write.hpp"
// clang-format on
#undef AF_IO_FILE_DETAIL_INCLUDE

} // namespace af
