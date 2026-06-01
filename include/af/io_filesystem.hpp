#pragma once

#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "af/io_common.hpp"

#if defined(__linux__)
#include <linux/openat2.h>
#endif

#if !defined(__linux__)
struct open_how;
#endif

namespace af {

#define AF_IO_FILESYSTEM_DETAIL_INCLUDE 1
// clang-format off
#include "af/detail/io_filesystem_open.hpp"
#include "af/detail/io_filesystem_namespace.hpp"
#include "af/detail/io_filesystem_allocation.hpp"
#include "af/detail/io_filesystem_directory.hpp"
// clang-format on
#undef AF_IO_FILESYSTEM_DETAIL_INCLUDE

} // namespace af
