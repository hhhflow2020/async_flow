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

#define AF_IO_URING_SUPPORT_FRAGMENT_INCLUDE
#include "af/detail/io_uring_support_abi_fragment.hpp"
#include "af/detail/io_uring_support_opcode_fragment.hpp"
#include "af/detail/io_uring_support_types_fragment.hpp"
#include "af/detail/io_uring_support_syscall_fragment.hpp"
#include "af/detail/io_uring_support_sqe_fragment.hpp"
#undef AF_IO_URING_SUPPORT_FRAGMENT_INCLUDE
#endif
