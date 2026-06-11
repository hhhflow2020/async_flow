#pragma once

#include <cstddef>
#include <cstdint>

namespace af {

namespace detail {
class epoll_reactor;
class kqueue_reactor;
class select_reactor;
} // namespace detail

inline constexpr std::uint32_t reactor_readable = 1U << 0U;
inline constexpr std::uint32_t reactor_writable = 1U << 1U;
inline constexpr std::uint32_t reactor_error = 1U << 2U;
inline constexpr std::uint32_t reactor_hangup = 1U << 3U;

struct fd_event_source;

using fd_event_callback = void (*)(void *owner, fd_event_source &source,
                                   std::uint32_t events) noexcept;

struct fd_event_source {
    int fd{-1};
    std::uint32_t interests{0};
    void *owner{nullptr};
    fd_event_callback on_event{nullptr};

private:
    bool active_{false};
    std::uint32_t backend_interests_{0};
    std::size_t backend_index_{invalid_backend_index};
    static constexpr std::size_t invalid_backend_index = static_cast<std::size_t>(-1);

    friend class detail::epoll_reactor;
    friend class detail::kqueue_reactor;
    friend class detail::select_reactor;
};

} // namespace af
