#pragma once

#include "af/detail/config.hpp"
#include "af/thread_kind.hpp"

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
inline constexpr ThreadKind native_io_thread_kind =
    supports_epoll ? ThreadKind::Epoll : (supports_kqueue ? ThreadKind::Kqueue : ThreadKind::Io);
inline constexpr ThreadKind preferred_io_thread_kind =
    supports_io_uring ? ThreadKind::IoUring : native_io_thread_kind;

template <typename RuntimeT>
[[nodiscard]] const char *runtime_io_backend_name(typename RuntimeT::Thread thread) noexcept {
    if constexpr (supports_io_uring) {
        if (RuntimeT::thread_kind(thread) == ThreadKind::IoUring) {
            return RuntimeT::io_uring_backend_available(thread) ? "io_uring" : "epoll-fallback";
        }
    }

    if constexpr (supports_epoll) {
        return "epoll";
    } else if constexpr (supports_kqueue) {
        return "kqueue";
    } else {
        return "native-readiness";
    }
}

} // namespace af
