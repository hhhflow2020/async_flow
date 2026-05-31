#pragma once
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <limits>
#include <type_traits>
#include <utility>

#include "af/task.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

#if defined(__linux__)
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include <sys/timerfd.h>
#endif

#if !defined(__linux__)
struct statx;
#endif

namespace af {

#define AF_IO_TYPES_FRAGMENT_INCLUDE
#include "af/detail/io_types_base_fragment.hpp"
#include "af/detail/io_types_provided_buffer_fragment.hpp"
#include "af/detail/io_types_status_fragment.hpp"
#include "af/detail/io_types_unique_fd_fragment.hpp"
#undef AF_IO_TYPES_FRAGMENT_INCLUDE

} // namespace af
