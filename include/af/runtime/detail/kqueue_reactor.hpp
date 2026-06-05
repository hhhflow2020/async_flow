#pragma once

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "af/detail/config.hpp"
#include "af/runtime/reactor.hpp"

#if AF_DETAIL_HAS_KQUEUE
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>
#endif

namespace af::detail {

#if AF_DETAIL_HAS_KQUEUE
class kqueue_reactor final : public reactor {
public:
    explicit kqueue_reactor(const reactor_config &config) noexcept
        : event_budget_(normalize_event_budget(config.event_budget)),
          edge_triggered_(config.edge_triggered) {
        init_backend(config.event_capacity);
    }

    ~kqueue_reactor() override {
        close_backend();
    }

    kqueue_reactor(const kqueue_reactor &) = delete;
    kqueue_reactor &operator=(const kqueue_reactor &) = delete;

    [[nodiscard]] bool available() const noexcept {
        return kqueue_fd_ >= 0 && !events_.empty();
    }

    [[nodiscard]] bool add(fd_event_source *source) noexcept override {
        if (!available() || !source_supported(source) || source->active_ ||
            has_active_source_for_fd(source->fd)) {
            return false;
        }
        if (!track_source(source)) {
            return false;
        }

        std::array<struct kevent, 2> changes{};
        const int count = fill_interest_changes(0, source->interests, source, changes);
        if (count == 0 || ::kevent(kqueue_fd_, changes.data(), count, nullptr, 0, nullptr) != 0) {
            untrack_source(source);
            return false;
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

        std::array<struct kevent, 2> changes{};
        const int count =
            fill_interest_changes(source->backend_interests_, source->interests, source, changes);
        if (count != 0 && ::kevent(kqueue_fd_, changes.data(), count, nullptr, 0, nullptr) != 0) {
            return false;
        }

        source->backend_interests_ = source->interests;
        return true;
    }

    [[nodiscard]] bool del(fd_event_source *source) noexcept override {
        if (source == nullptr || !source->active_) {
            return false;
        }

        std::array<struct kevent, 2> changes{};
        const int count = fill_interest_changes(source->backend_interests_, 0, source, changes);
        const bool removed =
            count == 0 || ::kevent(kqueue_fd_, changes.data(), count, nullptr, 0, nullptr) == 0 ||
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

        timespec timeout_value{};
        timespec *timeout_ptr = timeout_to_timespec(timeout, timeout_value);
        const int count =
            ::kevent(kqueue_fd_, nullptr, 0, events_.data(), poll_event_limit(), timeout_ptr);
        if (count <= 0) {
            return false;
        }

        bool did_work = false;
        ready_sources_.clear();
        ready_events_.clear();

        for (int i = 0; i < count; ++i) {
            const struct kevent &event = events_[static_cast<std::size_t>(i)];
            if (event.filter == EVFILT_USER && event.ident == wake_ident) {
                wake_pending_.store(false, std::memory_order_relaxed);
                did_work = true;
                continue;
            }

            auto *source = static_cast<fd_event_source *>(event.udata);
            const std::uint32_t events = reactor_events_from_native(event);
            if (source == nullptr || !source->active_ || source->on_event == nullptr ||
                events == 0U) {
                continue;
            }
            if (!append_ready(source, events)) {
                return did_work;
            }
        }

        for (std::size_t i = 0; i < ready_sources_.size(); ++i) {
            fd_event_source *source = ready_sources_[i];
            if (source != nullptr && source->active_ && source->on_event != nullptr) {
                source->on_event(source->owner, *source, ready_events_[i]);
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
        if (!wake_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
            return;
        }

        struct kevent event{};
        EV_SET(&event, wake_ident, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
        if (::kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
            wake_pending_.store(false, std::memory_order_relaxed);
        }
    }

private:
    static constexpr uintptr_t wake_ident = 1;

    [[nodiscard]] static std::size_t normalize_event_budget(std::size_t value) noexcept {
        return value == 0U ? 1U : value;
    }

    [[nodiscard]] int poll_event_limit() const noexcept {
        std::size_t limit = events_.size();
        if (event_budget_ < limit) {
            limit = event_budget_;
        }
        if (limit == 0U) {
            limit = 1U;
        }
        constexpr auto max_int = static_cast<std::size_t>(std::numeric_limits<int>::max());
        if (limit > max_int) {
            return std::numeric_limits<int>::max();
        }
        return static_cast<int>(limit);
    }

    void init_backend(std::size_t event_capacity) noexcept {
        try {
            const std::size_t capacity = event_capacity == 0U ? 1U : event_capacity;
            events_.resize(capacity);
            ready_sources_.reserve(capacity);
            ready_events_.reserve(capacity);
        } catch (...) {
            events_.clear();
            ready_sources_.clear();
            ready_events_.clear();
            return;
        }

        kqueue_fd_ = ::kqueue();
        if (kqueue_fd_ < 0) {
            close_backend();
            return;
        }

        struct kevent event{};
        EV_SET(&event, wake_ident, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
        if (::kevent(kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
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
        ready_sources_.clear();
        ready_events_.clear();

        if (kqueue_fd_ >= 0) {
            ::close(kqueue_fd_);
            kqueue_fd_ = -1;
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
        source->backend_interests_ = 0;
        source->backend_index_ = sources_.size();
        try {
            sources_.push_back(source);
        } catch (...) {
            source->active_ = false;
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

    [[nodiscard]] int fill_interest_changes(std::uint32_t old_interests,
                                            std::uint32_t new_interests, fd_event_source *source,
                                            std::array<struct kevent, 2> &changes) const noexcept {
        int count = 0;
        fill_filter_change(reactor_readable, EVFILT_READ, old_interests, new_interests, source,
                           changes, count);
        fill_filter_change(reactor_writable, EVFILT_WRITE, old_interests, new_interests, source,
                           changes, count);
        return count;
    }

    void fill_filter_change(std::uint32_t bit, int16_t filter, std::uint32_t old_interests,
                            std::uint32_t new_interests, fd_event_source *source,
                            std::array<struct kevent, 2> &changes, int &count) const noexcept {
        const bool had = (old_interests & bit) != 0U;
        const bool wants = (new_interests & bit) != 0U;
        if (had == wants) {
            return;
        }

        const std::uint16_t flags = wants ? add_filter_flags() : EV_DELETE;
        EV_SET(&changes[static_cast<std::size_t>(count++)], static_cast<uintptr_t>(source->fd),
               filter, flags, 0, 0, wants ? source : nullptr);
    }

    [[nodiscard]] std::uint16_t add_filter_flags() const noexcept {
        return static_cast<std::uint16_t>(EV_ADD | (edge_triggered_ ? EV_CLEAR : 0));
    }

    [[nodiscard]] static std::uint32_t
    reactor_events_from_native(const struct kevent &event) noexcept {
        if ((event.flags & EV_ERROR) != 0) {
            return reactor_error;
        }

        std::uint32_t result = 0;
        if (event.filter == EVFILT_READ) {
            result |= reactor_readable;
        } else if (event.filter == EVFILT_WRITE) {
            result |= reactor_writable;
        }
        if ((event.flags & EV_EOF) != 0) {
            result |= reactor_hangup;
        }
        return result;
    }

    [[nodiscard]] static timespec *timeout_to_timespec(std::chrono::nanoseconds timeout,
                                                       timespec &out) noexcept {
        if (timeout == std::chrono::nanoseconds::max()) {
            return nullptr;
        }
        if (timeout <= std::chrono::nanoseconds(0)) {
            out = {};
            return &out;
        }

        const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
        const auto nanoseconds = timeout - seconds;
        constexpr auto max_time_t = std::numeric_limits<time_t>::max();
        out.tv_sec =
            seconds.count() > max_time_t ? max_time_t : static_cast<time_t>(seconds.count());
        out.tv_nsec = static_cast<long>(nanoseconds.count());
        return &out;
    }

    [[nodiscard]] bool append_ready(fd_event_source *source, std::uint32_t events) noexcept {
        for (std::size_t i = 0; i < ready_sources_.size(); ++i) {
            if (ready_sources_[i] == source) {
                ready_events_[i] |= events;
                return true;
            }
        }

        try {
            ready_sources_.push_back(source);
            ready_events_.push_back(events);
        } catch (...) {
            ready_sources_.clear();
            ready_events_.clear();
            return false;
        }
        return true;
    }

    std::vector<fd_event_source *> sources_;
    std::vector<struct kevent> events_;
    std::vector<fd_event_source *> ready_sources_;
    std::vector<std::uint32_t> ready_events_;
    std::atomic<bool> wake_pending_{false};
    std::size_t event_budget_{1};
    int kqueue_fd_{-1};
    bool edge_triggered_{false};
};
#endif

[[nodiscard]] inline std::unique_ptr<reactor> make_kqueue_reactor(const reactor_config &config) {
#if AF_DETAIL_HAS_KQUEUE
    auto result = std::make_unique<kqueue_reactor>(config);
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
