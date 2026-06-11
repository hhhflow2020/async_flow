#pragma once

#include <chrono>
#include <memory>

#include "af/detail/config.hpp"
#include "af/reactor/fd_event_source.hpp"
#include "af/runtime/config_types.hpp"

namespace af {

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

#include "af/reactor/detail/epoll_reactor.hpp"
#include "af/reactor/detail/kqueue_reactor.hpp"
#include "af/reactor/detail/select_reactor.hpp"

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
