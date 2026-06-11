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

#ifndef AF_DETAIL_HAS_EPOLL
#if defined(__linux__)
#define AF_DETAIL_HAS_EPOLL 1
#else
#define AF_DETAIL_HAS_EPOLL 0
#endif
#endif

#ifndef AF_DETAIL_HAS_KQUEUE
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#define AF_DETAIL_HAS_KQUEUE 1
#else
#define AF_DETAIL_HAS_KQUEUE 0
#endif
#endif

#ifndef AF_DETAIL_HAS_SELECT
#if !defined(_WIN32)
#define AF_DETAIL_HAS_SELECT 1
#else
#define AF_DETAIL_HAS_SELECT 0
#endif
#endif

#ifndef AF_DETAIL_HAS_REACTOR_BACKEND
#if AF_DETAIL_HAS_EPOLL || AF_DETAIL_HAS_KQUEUE || AF_DETAIL_HAS_SELECT
#define AF_DETAIL_HAS_REACTOR_BACKEND 1
#else
#define AF_DETAIL_HAS_REACTOR_BACKEND 0
#endif
#endif

namespace af::detail {

inline constexpr bool platform_windows = false;

#if defined(__linux__)
inline constexpr bool platform_linux = true;
#else
inline constexpr bool platform_linux = false;
#endif

#if defined(__APPLE__)
inline constexpr bool platform_apple = true;
#else
inline constexpr bool platform_apple = false;
#endif

#if defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
inline constexpr bool platform_bsd = true;
#else
inline constexpr bool platform_bsd = false;
#endif

inline constexpr bool platform_posix = true;
inline constexpr bool supports_epoll = AF_DETAIL_HAS_EPOLL != 0;
inline constexpr bool supports_kqueue = AF_DETAIL_HAS_KQUEUE != 0;
inline constexpr bool supports_select = AF_DETAIL_HAS_SELECT != 0;
inline constexpr bool supports_reactor_backend = AF_DETAIL_HAS_REACTOR_BACKEND != 0;
inline constexpr bool supports_thread_affinity = platform_linux;
inline constexpr bool supports_thread_priority = platform_linux;

} // namespace af::detail
