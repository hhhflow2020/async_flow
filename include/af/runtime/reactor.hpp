#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "af/detail/config.hpp"
#include "af/runtime/config_types.hpp"

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

class reactor {
public:
    reactor() = default;
    reactor(const reactor &) = delete;
    reactor &operator=(const reactor &) = delete;
    virtual ~reactor() = default;

    [[nodiscard]] virtual bool add(fd_event_source *source) noexcept = 0;
    [[nodiscard]] virtual bool mod(fd_event_source *source) noexcept = 0;
    [[nodiscard]] virtual bool del(fd_event_source *source) noexcept = 0;
    [[nodiscard]] virtual bool poll(std::chrono::nanoseconds timeout) noexcept = 0;
    virtual void wake() noexcept = 0;
};

namespace detail {
[[nodiscard]] inline std::unique_ptr<reactor> make_epoll_reactor(const reactor_config &config);
[[nodiscard]] inline std::unique_ptr<reactor> make_kqueue_reactor(const reactor_config &config);
[[nodiscard]] inline std::unique_ptr<reactor> make_select_reactor(const reactor_config &config);
} // namespace detail

} // namespace af

#include "af/runtime/detail/epoll_reactor.hpp"
#include "af/runtime/detail/kqueue_reactor.hpp"
#include "af/runtime/detail/select_reactor.hpp"

namespace af {

[[nodiscard]] inline std::unique_ptr<reactor> make_reactor(const reactor_config &config) {
    switch (config.backend) {
    case reactor_backend::auto_select:
#if AF_DETAIL_HAS_EPOLL
        if (auto result = detail::make_epoll_reactor(config)) {
            return result;
        }
#endif
#if AF_DETAIL_HAS_KQUEUE
        if (auto result = detail::make_kqueue_reactor(config)) {
            return result;
        }
#endif
        return detail::make_select_reactor(config);
    case reactor_backend::epoll:
        return detail::make_epoll_reactor(config);
    case reactor_backend::kqueue:
        return detail::make_kqueue_reactor(config);
    case reactor_backend::select:
        return detail::make_select_reactor(config);
    }
    return nullptr;
}

[[nodiscard]] inline std::unique_ptr<reactor> make_reactor(reactor_backend backend) {
    reactor_config config;
    config.backend = backend;
    return make_reactor(config);
}

} // namespace af
