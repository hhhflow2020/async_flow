#pragma once

#include "af/io_socket.hpp"

namespace af {

#define AF_IO_FILE_FRAGMENT_INCLUDE 1
#include "af/detail/io_file_read_fragment.hpp"
#undef AF_IO_FILE_FRAGMENT_INCLUDE

#define AF_IO_FILE_FRAGMENT_INCLUDE 1
#include "af/detail/io_file_positioned_fragment.hpp"
#undef AF_IO_FILE_FRAGMENT_INCLUDE

#define AF_IO_FILE_FRAGMENT_INCLUDE 1
#include "af/detail/io_file_fixed_buffer_fragment.hpp"
#undef AF_IO_FILE_FRAGMENT_INCLUDE

#define AF_IO_FILE_FRAGMENT_INCLUDE 1
#include "af/detail/io_file_lifecycle_fragment.hpp"
#undef AF_IO_FILE_FRAGMENT_INCLUDE

#define AF_IO_FILE_FRAGMENT_INCLUDE 1
#include "af/detail/io_file_write_fragment.hpp"
#undef AF_IO_FILE_FRAGMENT_INCLUDE

} // namespace af
