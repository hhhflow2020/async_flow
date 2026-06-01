#pragma once

#include <cassert>
#include <cstddef>

#ifndef AF_ENABLE_ASSERTS
#if defined(NDEBUG)
#define AF_ENABLE_ASSERTS 0
#else
#define AF_ENABLE_ASSERTS 1
#endif
#endif

#if AF_ENABLE_ASSERTS
#define AF_ASSERT(expr) assert(expr)
#else
#define AF_ASSERT(expr) static_cast<void>(sizeof(expr))
#endif

#if defined(__GNUC__) || defined(__clang__)
#define AF_DETAIL_NOINLINE __attribute__((noinline))
#else
#define AF_DETAIL_NOINLINE
#endif

#if defined(__linux__)
#define AF_DETAIL_HAS_EPOLL 1
#else
#define AF_DETAIL_HAS_EPOLL 0
#endif

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#define AF_DETAIL_HAS_KQUEUE 1
#else
#define AF_DETAIL_HAS_KQUEUE 0
#endif

#if AF_DETAIL_HAS_EPOLL || AF_DETAIL_HAS_KQUEUE
#define AF_DETAIL_HAS_NATIVE_IO_WAIT 1
#else
#define AF_DETAIL_HAS_NATIVE_IO_WAIT 0
#endif

namespace af::detail {

inline constexpr std::size_t hardware_cache_line_size = 64;

} // namespace af::detail
