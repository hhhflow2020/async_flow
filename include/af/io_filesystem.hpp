#pragma once

#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "af/io_common.hpp"
#include "af/detail/io/filesystem/io_filesystem_platform.hpp"

namespace af {

// clang-format off
#include "af/detail/io/filesystem/io_filesystem_open.hpp"
#include "af/detail/io/filesystem/io_filesystem_namespace.hpp"
#include "af/detail/io/filesystem/io_filesystem_allocation.hpp"
#include "af/detail/io/filesystem/io_filesystem_directory.hpp"
// clang-format on

} // namespace af
