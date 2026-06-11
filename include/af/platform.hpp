#pragma once

#include "af/detail/config.hpp"
#include "af/thread_kind.hpp"

namespace af {

inline constexpr bool platform_windows = detail::platform_windows;
inline constexpr bool platform_linux = detail::platform_linux;
inline constexpr bool platform_apple = detail::platform_apple;
inline constexpr bool platform_bsd = detail::platform_bsd;
inline constexpr bool platform_posix = detail::platform_posix;
inline constexpr bool supports_epoll = detail::supports_epoll;
inline constexpr bool supports_kqueue = detail::supports_kqueue;
inline constexpr bool supports_native_io_wait = detail::supports_native_io_wait;
inline constexpr bool supports_thread_affinity = detail::supports_thread_affinity;
inline constexpr bool supports_thread_priority = detail::supports_thread_priority;
inline constexpr bool supports_eventfd = platform_linux;
inline constexpr bool supports_timerfd = platform_linux;
inline constexpr bool supports_openat2 = platform_linux;
inline constexpr bool supports_sendfile = platform_linux;
inline constexpr bool supports_splice = platform_linux;
inline constexpr bool supports_zero_copy_send = platform_linux;

} // namespace af
