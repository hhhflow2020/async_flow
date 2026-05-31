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

#define AF_IO_FILESYSTEM_FRAGMENT_INCLUDE 1
#include "af/detail/io_filesystem_open_fragment.hpp"
#undef AF_IO_FILESYSTEM_FRAGMENT_INCLUDE

#define AF_IO_FILESYSTEM_FRAGMENT_INCLUDE 1
#include "af/detail/io_filesystem_namespace_fragment.hpp"
#undef AF_IO_FILESYSTEM_FRAGMENT_INCLUDE

#define AF_IO_FILESYSTEM_FRAGMENT_INCLUDE 1
#include "af/detail/io_filesystem_allocation_fragment.hpp"
#undef AF_IO_FILESYSTEM_FRAGMENT_INCLUDE

#define AF_IO_FILESYSTEM_FRAGMENT_INCLUDE 1
#include "af/detail/io_filesystem_directory_fragment.hpp"
#undef AF_IO_FILESYSTEM_FRAGMENT_INCLUDE

} // namespace af
