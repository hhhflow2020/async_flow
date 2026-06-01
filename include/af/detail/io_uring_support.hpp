#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#if defined(__linux__)
#include <linux/io_uring.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

// clang-format off
#include "af/detail/io_uring_support_abi.hpp"
#include "af/detail/io_uring_support_opcode.hpp"
#include "af/detail/io_uring_support_types.hpp"
#include "af/detail/io_uring_support_syscall.hpp"
#include "af/detail/io_uring_support_sqe.hpp"
// clang-format on
#endif
