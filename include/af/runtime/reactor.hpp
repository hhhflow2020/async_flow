#pragma once

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "af/detail/config.hpp"
#include "af/runtime/config_types.hpp"

#if AF_DETAIL_HAS_NATIVE_IO_WAIT
#include <fcntl.h>
#include <sys/select.h>
#include <unistd.h>
#endif

namespace af {

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
    std::size_t backend_index_{invalid_backend_index};
    static constexpr std::size_t invalid_backend_index = static_cast<std::size_t>(-1);

    friend class select_reactor;
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

class select_reactor final : public reactor {
public:
    select_reactor() noexcept {
        init_wake_pipe();
    }

    ~select_reactor() override {
        close_wake_pipe();
    }

    select_reactor(const select_reactor &) = delete;
    select_reactor &operator=(const select_reactor &) = delete;

    [[nodiscard]] bool available() const noexcept {
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
        return wake_read_fd_ >= 0 && wake_write_fd_ >= 0;
#else
        return false;
#endif
    }

    [[nodiscard]] bool add(fd_event_source *source) noexcept override {
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
        if (!available() || !source_supported(source) || source->active_) {
            return false;
        }
        source->active_ = true;
        source->backend_index_ = sources_.size();
        try {
            sources_.push_back(source);
        } catch (...) {
            source->active_ = false;
            source->backend_index_ = fd_event_source::invalid_backend_index;
            return false;
        }
        return true;
#else
        static_cast<void>(source);
        return false;
#endif
    }

    [[nodiscard]] bool mod(fd_event_source *source) noexcept override {
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
        if (!available() || source == nullptr || !source->active_) {
            return false;
        }
        if ((source->interests & (reactor_readable | reactor_writable)) == 0U) {
            return del(source);
        }
        return fd_supported(source->fd);
#else
        static_cast<void>(source);
        return false;
#endif
    }

    [[nodiscard]] bool del(fd_event_source *source) noexcept override {
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
        if (source == nullptr || !source->active_) {
            return false;
        }

        std::size_t index = source->backend_index_;
        if (index >= sources_.size() || sources_[index] != source) {
            index = find_source(source);
            if (index == fd_event_source::invalid_backend_index) {
                source->active_ = false;
                source->backend_index_ = fd_event_source::invalid_backend_index;
                return false;
            }
        }

        fd_event_source *last = sources_.back();
        sources_[index] = last;
        sources_.pop_back();
        if (index < sources_.size() && last != nullptr) {
            last->backend_index_ = index;
        }
        source->active_ = false;
        source->backend_index_ = fd_event_source::invalid_backend_index;
        return true;
#else
        static_cast<void>(source);
        return false;
#endif
    }

    [[nodiscard]] bool poll(std::chrono::nanoseconds timeout) noexcept override {
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
        if (!available()) {
            return false;
        }

        fd_set readfds;
        fd_set writefds;
        fd_set exceptfds;
        FD_ZERO(&readfds);
        FD_ZERO(&writefds);
        FD_ZERO(&exceptfds);

        int max_fd = wake_read_fd_;
        FD_SET(wake_read_fd_, &readfds);

        for (fd_event_source *source : sources_) {
            if (source == nullptr || !source->active_ || !fd_supported(source->fd)) {
                continue;
            }
            if ((source->interests & reactor_readable) != 0U) {
                FD_SET(source->fd, &readfds);
            }
            if ((source->interests & reactor_writable) != 0U) {
                FD_SET(source->fd, &writefds);
            }
            FD_SET(source->fd, &exceptfds);
            if (source->fd > max_fd) {
                max_fd = source->fd;
            }
        }

        timeval timeout_value{};
        timeval *timeout_ptr = timeout_to_timeval(timeout, timeout_value);
        const int count = ::select(max_fd + 1, &readfds, &writefds, &exceptfds, timeout_ptr);
        if (count <= 0) {
            return false;
        }

        bool did_work = false;
        if (FD_ISSET(wake_read_fd_, &readfds)) {
            drain_wake_pipe();
            did_work = true;
        }

        ready_sources_.clear();
        ready_events_.clear();
        try {
            ready_sources_.reserve(sources_.size());
            ready_events_.reserve(sources_.size());
        } catch (...) {
            ready_sources_.clear();
            ready_events_.clear();
            return did_work;
        }
        for (fd_event_source *source : sources_) {
            if (source == nullptr || !source->active_ || !fd_supported(source->fd)) {
                continue;
            }

            const std::uint32_t events = selected_events(*source, readfds, writefds, exceptfds);
            if (events != 0U) {
                try {
                    ready_sources_.push_back(source);
                    ready_events_.push_back(events);
                } catch (...) {
                    ready_sources_.clear();
                    ready_events_.clear();
                    return did_work;
                }
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
#else
        static_cast<void>(timeout);
        return false;
#endif
    }

    void wake() noexcept override {
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
        if (!available()) {
            return;
        }

        bool expected = false;
        if (wake_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
            if (!write_wake_byte()) {
                wake_pending_.store(false, std::memory_order_relaxed);
            }
        }
#endif
    }

private:
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    [[nodiscard]] static bool fd_supported(int fd) noexcept {
        return fd >= 0 && fd < FD_SETSIZE;
    }

    [[nodiscard]] static bool source_supported(const fd_event_source *source) noexcept {
        return source != nullptr && fd_supported(source->fd) && source->on_event != nullptr &&
               (source->interests & (reactor_readable | reactor_writable)) != 0U;
    }

    void init_wake_pipe() noexcept {
        int pipe_fds[2]{-1, -1};
        if (::pipe(pipe_fds) != 0) {
            return;
        }

        wake_read_fd_ = pipe_fds[0];
        wake_write_fd_ = pipe_fds[1];
        if (!set_nonblocking_cloexec(wake_read_fd_) || !set_nonblocking_cloexec(wake_write_fd_) ||
            !fd_supported(wake_read_fd_) || !fd_supported(wake_write_fd_)) {
            close_wake_pipe();
        }
    }

    static bool set_nonblocking_cloexec(int fd) noexcept {
        int flags = ::fcntl(fd, F_GETFD, 0);
        if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0) {
            return false;
        }

        flags = ::fcntl(fd, F_GETFL, 0);
        return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
    }

    void close_wake_pipe() noexcept {
        for (fd_event_source *source : sources_) {
            if (source != nullptr) {
                source->active_ = false;
                source->backend_index_ = fd_event_source::invalid_backend_index;
            }
        }
        sources_.clear();
        ready_sources_.clear();
        ready_events_.clear();

        if (wake_read_fd_ >= 0) {
            ::close(wake_read_fd_);
            wake_read_fd_ = -1;
        }
        if (wake_write_fd_ >= 0) {
            ::close(wake_write_fd_);
            wake_write_fd_ = -1;
        }
        wake_pending_.store(false, std::memory_order_relaxed);
    }

    [[nodiscard]] bool write_wake_byte() noexcept {
        const std::uint8_t value = 1;
        for (;;) {
            const ssize_t written = ::write(wake_write_fd_, &value, sizeof(value));
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

    void drain_wake_pipe() noexcept {
        std::array<std::uint8_t, 64> buffer{};
        for (;;) {
            const ssize_t n = ::read(wake_read_fd_, buffer.data(), buffer.size());
            if (n > 0) {
                continue;
            }
            if (n < 0 && errno == EINTR) {
                continue;
            }
            break;
        }
        wake_pending_.store(false, std::memory_order_relaxed);
    }

    [[nodiscard]] static timeval *timeout_to_timeval(std::chrono::nanoseconds timeout,
                                                     timeval &out) noexcept {
        if (timeout == std::chrono::nanoseconds::max()) {
            return nullptr;
        }
        if (timeout <= std::chrono::nanoseconds(0)) {
            out = {};
            return &out;
        }

        const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
        const auto microseconds =
            std::chrono::duration_cast<std::chrono::microseconds>(timeout - seconds);
        out.tv_sec = static_cast<time_t>(seconds.count());
        out.tv_usec = static_cast<suseconds_t>(microseconds.count());
        return &out;
    }

    [[nodiscard]] static std::uint32_t selected_events(fd_event_source &source, fd_set &readfds,
                                                       fd_set &writefds,
                                                       fd_set &exceptfds) noexcept {
        std::uint32_t events = 0;
        if (FD_ISSET(source.fd, &readfds)) {
            events |= reactor_readable;
        }
        if (FD_ISSET(source.fd, &writefds)) {
            events |= reactor_writable;
        }
        if (FD_ISSET(source.fd, &exceptfds)) {
            events |= reactor_error;
        }
        return events;
    }

    [[nodiscard]] std::size_t find_source(const fd_event_source *source) const noexcept {
        for (std::size_t i = 0; i < sources_.size(); ++i) {
            if (sources_[i] == source) {
                return i;
            }
        }
        return fd_event_source::invalid_backend_index;
    }

    std::vector<fd_event_source *> sources_;
    std::vector<fd_event_source *> ready_sources_;
    std::vector<std::uint32_t> ready_events_;
    std::atomic<bool> wake_pending_{false};
    int wake_read_fd_{-1};
    int wake_write_fd_{-1};
#endif
};

[[nodiscard]] inline std::unique_ptr<reactor> make_reactor(reactor_backend backend) {
    switch (backend) {
    case reactor_backend::auto_select:
    case reactor_backend::select: {
        auto result = std::make_unique<select_reactor>();
        if (!result->available()) {
            return nullptr;
        }
        return result;
    }
    case reactor_backend::epoll:
    case reactor_backend::kqueue:
        return nullptr;
    }
    return nullptr;
}

} // namespace af
