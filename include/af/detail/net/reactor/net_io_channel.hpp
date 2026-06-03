#pragma once

#include <cstdint>

namespace af::detail {

inline constexpr std::uint32_t net_io_readable = 1U << 0U;
inline constexpr std::uint32_t net_io_writable = 1U << 1U;
inline constexpr std::uint32_t net_io_error = 1U << 2U;
inline constexpr std::uint32_t net_io_hangup = 1U << 3U;

struct NetIoChannel {
    using EventFn = void (*)(void *owner, std::uint32_t events) noexcept;

    int fd{-1};
    std::uint32_t interests{0};
    bool active{false};
    void *backend_token{nullptr};
    void *owner{nullptr};
    EventFn on_event{nullptr};
};

} // namespace af::detail
