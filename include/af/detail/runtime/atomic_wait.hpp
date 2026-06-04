#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
#include <ctime>
#include <cstdint>
#include <cstring>
#include <limits>
#include <thread>
#include <type_traits>

#if defined(__linux__)
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace af::detail {

template <typename T>
inline constexpr bool atomic_futex_compatible_v =
    sizeof(T) == sizeof(std::uint32_t) && (std::is_integral<T>::value || std::is_enum<T>::value);

template <typename T> [[nodiscard]] inline std::uint32_t atomic_wait_bits(T value) noexcept {
    static_assert(sizeof(T) == sizeof(std::uint32_t));
    std::uint32_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    return bits;
}

template <typename T>
void atomic_wait_value(const std::atomic<T> &value, T old, std::memory_order order) noexcept {
#if defined(__cpp_lib_atomic_wait) && __cpp_lib_atomic_wait >= 201907L
    value.wait(old, order);
#else
    if constexpr (atomic_futex_compatible_v<T>) {
#if defined(__linux__)
        const auto *address = reinterpret_cast<const std::uint32_t *>(&value);
        const std::uint32_t old_bits = atomic_wait_bits(old);
        while (value.load(order) == old) {
            const long result = ::syscall(SYS_futex, const_cast<std::uint32_t *>(address),
                                          FUTEX_WAIT_PRIVATE, old_bits, nullptr, nullptr, 0);
            if (result == 0 || errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN) {
                return;
            }
            return;
        }
#else
        while (value.load(order) == old) {
            std::this_thread::yield();
        }
#endif
    } else {
        while (value.load(order) == old) {
            std::this_thread::yield();
        }
    }
#endif
}

template <typename Rep, typename Period>
[[nodiscard]] inline timespec
duration_to_timespec(std::chrono::duration<Rep, Period> timeout) noexcept {
    if (timeout <= timeout.zero()) {
        return {};
    }

    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(timeout);
    const auto sec = std::chrono::duration_cast<std::chrono::seconds>(ns);
    timespec result{};
    result.tv_sec = static_cast<time_t>(sec.count());
    result.tv_nsec = static_cast<long>((ns - sec).count());
    return result;
}

template <typename T, typename Rep, typename Period>
[[nodiscard]] bool atomic_wait_value_for(const std::atomic<T> &value, T old,
                                         std::chrono::duration<Rep, Period> timeout,
                                         std::memory_order order) noexcept {
    if (value.load(order) != old) {
        return true;
    }
    if (timeout <= timeout.zero()) {
        return false;
    }

    if constexpr (atomic_futex_compatible_v<T>) {
#if defined(__linux__)
        const auto *address = reinterpret_cast<const std::uint32_t *>(&value);
        const std::uint32_t old_bits = atomic_wait_bits(old);
        timespec relative_timeout = duration_to_timespec(timeout);
        while (value.load(order) == old) {
            const long result =
                ::syscall(SYS_futex, const_cast<std::uint32_t *>(address), FUTEX_WAIT_PRIVATE,
                          old_bits, &relative_timeout, nullptr, 0);
            if (result == 0 || errno == EINTR || errno == EAGAIN) {
                break;
            }
            if (errno == ETIMEDOUT) {
                break;
            }
            break;
        }
        return value.load(order) != old;
#else
        auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(timeout);
        constexpr auto poll_interval = std::chrono::milliseconds(1);
        while (value.load(order) == old && remaining > std::chrono::nanoseconds(0)) {
            const auto sleep_for = remaining < poll_interval ? remaining : poll_interval;
            std::this_thread::sleep_for(sleep_for);
            remaining -= sleep_for;
        }
        return value.load(order) != old;
#endif
    } else {
        auto remaining = std::chrono::duration_cast<std::chrono::nanoseconds>(timeout);
        constexpr auto poll_interval = std::chrono::milliseconds(1);
        while (value.load(order) == old && remaining > std::chrono::nanoseconds(0)) {
            const auto sleep_for = remaining < poll_interval ? remaining : poll_interval;
            std::this_thread::sleep_for(sleep_for);
            remaining -= sleep_for;
        }
        return value.load(order) != old;
    }
}

template <typename T> void atomic_notify_one(std::atomic<T> &value) noexcept {
#if defined(__cpp_lib_atomic_wait) && __cpp_lib_atomic_wait >= 201907L
    value.notify_one();
#else
    if constexpr (atomic_futex_compatible_v<T>) {
#if defined(__linux__)
        auto *address = reinterpret_cast<std::uint32_t *>(&value);
        static_cast<void>(
            ::syscall(SYS_futex, address, FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0));
#else
        static_cast<void>(value);
#endif
    } else {
        static_cast<void>(value);
    }
#endif
}

template <typename T> void atomic_notify_all(std::atomic<T> &value) noexcept {
#if defined(__cpp_lib_atomic_wait) && __cpp_lib_atomic_wait >= 201907L
    value.notify_all();
#else
    if constexpr (atomic_futex_compatible_v<T>) {
#if defined(__linux__)
        auto *address = reinterpret_cast<std::uint32_t *>(&value);
        static_cast<void>(::syscall(SYS_futex, address, FUTEX_WAKE_PRIVATE,
                                    std::numeric_limits<int>::max(), nullptr, nullptr, 0));
#else
        static_cast<void>(value);
#endif
    } else {
        static_cast<void>(value);
    }
#endif
}

} // namespace af::detail
