#pragma once

#include "af/detail/config.hpp"

namespace af {

#if defined(_WIN32)
inline constexpr bool platform_windows = true;
#else
inline constexpr bool platform_windows = false;
#endif

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

inline constexpr bool platform_posix = !platform_windows;
inline constexpr bool supports_epoll = AF_DETAIL_HAS_EPOLL != 0;
inline constexpr bool supports_kqueue = AF_DETAIL_HAS_KQUEUE != 0;
inline constexpr bool supports_native_io_wait = AF_DETAIL_HAS_NATIVE_IO_WAIT != 0;
inline constexpr bool supports_io_uring = platform_linux;
inline constexpr bool supports_eventfd = platform_linux;
inline constexpr bool supports_timerfd = platform_linux;
inline constexpr bool supports_openat2 = platform_linux;
inline constexpr bool supports_sendfile = platform_linux;
inline constexpr bool supports_splice = platform_linux;
inline constexpr bool supports_zero_copy_send = platform_linux;

} // namespace af
