#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "af/detail/config.hpp"
#include "af/runtime/reactor.hpp"

#if AF_DETAIL_HAS_EPOLL
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#endif

namespace af::detail {

#if AF_DETAIL_HAS_EPOLL
class epoll_reactor final : public reactor {
public:
    explicit epoll_reactor(const reactor_config &config) noexcept
        : edge_triggered_(config.edge_triggered) {
        init_backend(config.event_capacity);
    }

    ~epoll_reactor() override {
        close_backend();
    }

    epoll_reactor(const epoll_reactor &) = delete;
    epoll_reactor &operator=(const epoll_reactor &) = delete;

    [[nodiscard]] bool available() const noexcept {
        return epoll_fd_ >= 0 && wake_fd_ >= 0 && !events_.empty();
    }

    [[nodiscard]] bool add(fd_event_source *source) noexcept override {
        if (!available() || !source_supported(source) || source->active_ ||
            has_active_source_for_fd(source->fd)) {
            return false;
        }
        if (!track_source(source)) {
            return false;
        }

        epoll_event event{};
        event.events = native_events_for_interests(source->interests);
        event.data.ptr = source;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, source->fd, &event) != 0) {
            const int saved_errno = errno;
            if (saved_errno != EEXIST ||
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, source->fd, &event) != 0) {
                untrack_source(source);
                return false;
            }
        }
        source->backend_interests_ = source->interests;
        return true;
    }

    [[nodiscard]] bool mod(fd_event_source *source) noexcept override {
        if (!available() || source == nullptr || !source->active_) {
            return false;
        }
        if ((source->interests & (reactor_readable | reactor_writable)) == 0U) {
            return del(source);
        }

        epoll_event event{};
        event.events = native_events_for_interests(source->interests);
        event.data.ptr = source;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, source->fd, &event) == 0) {
            source->backend_interests_ = source->interests;
            return true;
        }
        if (errno == ENOENT) {
            const bool added = ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, source->fd, &event) == 0;
            if (added) {
                source->backend_interests_ = source->interests;
            }
            return added;
        }
        return false;
    }

    [[nodiscard]] bool del(fd_event_source *source) noexcept override {
        if (source == nullptr || !source->active_) {
            return false;
        }

        const bool removed = ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, source->fd, nullptr) == 0 ||
                             errno == ENOENT || errno == EBADF;
        if (removed) {
            untrack_source(source);
        }
        return removed;
    }

    [[nodiscard]] bool poll(std::chrono::nanoseconds timeout) noexcept override {
        if (!available()) {
            return false;
        }

        const int timeout_ms = timeout_to_milliseconds(timeout);
        const int count =
            ::epoll_wait(epoll_fd_, events_.data(), static_cast<int>(events_.size()), timeout_ms);
        if (count <= 0) {
            return false;
        }

        bool did_work = false;
        for (int i = 0; i < count; ++i) {
            const epoll_event &event = events_[static_cast<std::size_t>(i)];
            if (event.data.ptr == nullptr) {
                drain_wake_fd();
                did_work = true;
                continue;
            }

            auto *source = static_cast<fd_event_source *>(event.data.ptr);
            const std::uint32_t events = reactor_events_from_native(event.events);
            if (source != nullptr && source->active_ && source->on_event != nullptr &&
                events != 0U) {
                source->on_event(source->owner, *source, events);
                did_work = true;
            }
        }
        return did_work;
    }

    void wake() noexcept override {
        if (!available()) {
            return;
        }

        bool expected = false;
        if (wake_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            if (!write_wake_event()) {
                wake_pending_.store(false, std::memory_order_relaxed);
            }
        }
    }

private:
    void init_backend(std::size_t event_capacity) noexcept {
        try {
            events_.resize(event_capacity == 0U ? 1U : event_capacity);
        } catch (...) {
            events_.clear();
            return;
        }

        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ < 0) {
            close_backend();
            return;
        }

        wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (wake_fd_ < 0) {
            close_backend();
            return;
        }

        epoll_event event{};
        event.events = EPOLLIN;
        event.data.ptr = nullptr;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &event) != 0) {
            close_backend();
        }
    }

    void close_backend() noexcept {
        for (fd_event_source *source : sources_) {
            if (source != nullptr) {
                source->active_ = false;
                source->backend_interests_ = 0;
                source->backend_index_ = fd_event_source::invalid_backend_index;
            }
        }
        sources_.clear();
        events_.clear();

        if (wake_fd_ >= 0) {
            ::close(wake_fd_);
            wake_fd_ = -1;
        }
        if (epoll_fd_ >= 0) {
            ::close(epoll_fd_);
            epoll_fd_ = -1;
        }
        wake_pending_.store(false, std::memory_order_relaxed);
    }

    [[nodiscard]] static bool source_supported(const fd_event_source *source) noexcept {
        return source != nullptr && source->fd >= 0 && source->on_event != nullptr &&
               (source->interests & (reactor_readable | reactor_writable)) != 0U;
    }

    [[nodiscard]] bool has_active_source_for_fd(int fd) const noexcept {
        for (const fd_event_source *source : sources_) {
            if (source != nullptr && source->active_ && source->fd == fd) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool track_source(fd_event_source *source) noexcept {
        source->active_ = true;
        source->backend_index_ = sources_.size();
        try {
            sources_.push_back(source);
        } catch (...) {
            source->active_ = false;
            source->backend_interests_ = 0;
            source->backend_index_ = fd_event_source::invalid_backend_index;
            return false;
        }
        return true;
    }

    void untrack_source(fd_event_source *source) noexcept {
        std::size_t index = source->backend_index_;
        if (index >= sources_.size() || sources_[index] != source) {
            index = find_source(source);
            if (index == fd_event_source::invalid_backend_index) {
                source->active_ = false;
                source->backend_interests_ = 0;
                source->backend_index_ = fd_event_source::invalid_backend_index;
                return;
            }
        }

        fd_event_source *last = sources_.back();
        sources_[index] = last;
        sources_.pop_back();
        if (index < sources_.size() && last != nullptr) {
            last->backend_index_ = index;
        }
        source->active_ = false;
        source->backend_interests_ = 0;
        source->backend_index_ = fd_event_source::invalid_backend_index;
    }

    [[nodiscard]] std::size_t find_source(const fd_event_source *source) const noexcept {
        for (std::size_t i = 0; i < sources_.size(); ++i) {
            if (sources_[i] == source) {
                return i;
            }
        }
        return fd_event_source::invalid_backend_index;
    }

    [[nodiscard]] std::uint32_t
    native_events_for_interests(std::uint32_t interests) const noexcept {
        std::uint32_t events = EPOLLERR | EPOLLHUP;
#ifdef EPOLLRDHUP
        events |= EPOLLRDHUP;
#endif
        if ((interests & reactor_readable) != 0U) {
            events |= EPOLLIN;
        }
        if ((interests & reactor_writable) != 0U) {
            events |= EPOLLOUT;
        }
        if (edge_triggered_) {
            events |= EPOLLET;
        }
        return events;
    }

    [[nodiscard]] static std::uint32_t reactor_events_from_native(std::uint32_t events) noexcept {
        std::uint32_t result = 0;
        if ((events & EPOLLIN) != 0U) {
            result |= reactor_readable;
        }
        if ((events & EPOLLOUT) != 0U) {
            result |= reactor_writable;
        }
        if ((events & EPOLLERR) != 0U) {
            result |= reactor_error;
        }
        if ((events & EPOLLHUP) != 0U) {
            result |= reactor_hangup;
        }
#ifdef EPOLLRDHUP
        if ((events & EPOLLRDHUP) != 0U) {
            result |= reactor_hangup;
        }
#endif
        return result;
    }

    [[nodiscard]] static int timeout_to_milliseconds(std::chrono::nanoseconds timeout) noexcept {
        if (timeout == std::chrono::nanoseconds::max()) {
            return -1;
        }
        if (timeout <= std::chrono::nanoseconds(0)) {
            return 0;
        }

        constexpr std::int64_t nanos_per_millisecond = 1000000;
        const auto count = timeout.count();
        constexpr std::int64_t max_timeout_ns =
            static_cast<std::int64_t>(INT_MAX) * nanos_per_millisecond;
        if (count >= max_timeout_ns) {
            return INT_MAX;
        }
        const auto rounded = (count + nanos_per_millisecond - 1) / nanos_per_millisecond;
        return static_cast<int>(rounded);
    }

    [[nodiscard]] bool write_wake_event() noexcept {
        const std::uint64_t value = 1;
        for (;;) {
            const ssize_t written = ::write(wake_fd_, &value, sizeof(value));
            if (written == static_cast<ssize_t>(sizeof(value))) {
                return true;
            }
            if (written < 0 && errno == EINTR) {
                continue;
            }
            if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return true;
            }
            return false;
        }
    }

    void drain_wake_fd() noexcept {
        std::uint64_t value = 0;
        for (;;) {
            const ssize_t read_bytes = ::read(wake_fd_, &value, sizeof(value));
            if (read_bytes == static_cast<ssize_t>(sizeof(value))) {
                continue;
            }
            if (read_bytes < 0 && errno == EINTR) {
                continue;
            }
            break;
        }
        wake_pending_.store(false, std::memory_order_relaxed);
    }

    std::vector<fd_event_source *> sources_;
    std::vector<epoll_event> events_;
    std::atomic<bool> wake_pending_{false};
    int epoll_fd_{-1};
    int wake_fd_{-1};
    bool edge_triggered_{false};
};
#endif

[[nodiscard]] inline std::unique_ptr<reactor> make_epoll_reactor(const reactor_config &config) {
#if AF_DETAIL_HAS_EPOLL
    auto result = std::make_unique<epoll_reactor>(config);
    if (!result->available()) {
        return nullptr;
    }
    return result;
#else
    static_cast<void>(config);
    return nullptr;
#endif
}

} // namespace af::detail
