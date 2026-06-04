#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT> class alignas(hardware_cache_line_size) Executor {
    using Thread = typename RuntimeT::Thread;
    using Task = BasicTask<RuntimeT>;
    template <typename T> using CacheLineAtomic = detail::CacheLineAtomic<T>;
    template <typename T> using IoObjectPool = detail::ObjectPool<T, 256, 1, false, 1>;

    static constexpr std::uint16_t thread_count = RuntimeT::thread_count;
    static constexpr std::uint16_t invalid_thread_index = RuntimeT::invalid_thread_index;
    static constexpr std::size_t io_wait_reserve = RuntimeT::io_wait_reserve;
    static constexpr std::size_t timer_drain_budget = RuntimeT::timer_drain_budget;
    static constexpr std::size_t timer_reserve = RuntimeT::timer_reserve;
    static constexpr std::size_t service_task_budget = RuntimeT::service_task_budget;

    [[nodiscard]] static constexpr Thread thread_from_index(std::uint16_t index) noexcept {
        return RuntimeT::thread_from_index(index);
    }

    [[nodiscard]] static constexpr af::thread_kind thread_kind(Thread thread) noexcept {
        return RuntimeT::thread_kind(thread);
    }

    [[nodiscard]] static std::string_view thread_name(Thread thread) noexcept {
        return RuntimeT::thread_name(thread);
    }

    [[nodiscard]] static constexpr std::uint16_t thread_group_offset(Thread thread) noexcept {
        return RuntimeT::thread_group_offset(thread);
    }

    static void on_task_finished(Task *task) noexcept {
        RuntimeT::on_task_finished(task);
    }

    static void enqueue_pending_blocking(std::uint16_t index, Task *task,
                                         ScheduleMode mode = ScheduleMode::Auto) noexcept {
        RuntimeT::enqueue_pending_blocking(index, task, mode);
    }

    static void
    enqueue_ready_blocking_from_runtime_thread(std::uint16_t source, std::uint16_t target,
                                               Task *task,
                                               ScheduleMode mode = ScheduleMode::Auto) noexcept {
        RuntimeT::enqueue_ready_blocking_from_runtime_thread(source, target, task, mode);
    }

    [[nodiscard]] static bool fail_io_result(IoResult *result, int fd, int error) noexcept {
        if (result != nullptr) {
            detail::set_io_result_error(*result, fd, error);
        }
        return false;
    }

#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    struct IoWaitRegistration;
#endif
#if AF_DETAIL_HAS_KQUEUE
    struct KqueueTimeoutRegistration;
#endif
    struct TimerEntry;

public:
    explicit Executor(std::uint16_t index);
    Executor(const Executor &) = delete;
    Executor &operator=(const Executor &) = delete;
    ~Executor();

    void start();
    void request_stop() noexcept;
    void join();
    void notify() noexcept;

    [[nodiscard]] bool io_backend_available() const noexcept {
        return native_io_backend_available();
    }

    [[nodiscard]] bool register_io_wait(int fd, std::uint32_t events, Task *task,
                                        IoResult *result) noexcept;
    [[nodiscard]] bool cancel_io(IoOpState &state) noexcept;

    [[nodiscard]] bool register_net_channel(detail::NetIoChannel *channel,
                                            std::uint32_t events) noexcept;
    [[nodiscard]] bool update_net_channel(detail::NetIoChannel *channel,
                                          std::uint32_t events) noexcept;
    [[nodiscard]] bool unregister_net_channel(detail::NetIoChannel *channel) noexcept;

    [[nodiscard]] bool register_service_task(detail::RuntimeServiceTask *service) noexcept;
    [[nodiscard]] bool unregister_service_task(detail::RuntimeServiceTask *service) noexcept;

    [[nodiscard]] bool register_timer_wait(std::chrono::nanoseconds timeout, Task *task,
                                           IoResult *result) noexcept {
#if AF_DETAIL_HAS_KQUEUE
        return register_kqueue_timeout(timeout, task, result);
#else
        static_cast<void>(timeout);
        static_cast<void>(task);
        return fail_io_result(result, -1, ENOSYS);
#endif
    }

    void enqueue(Task *task) noexcept;
    [[nodiscard]] Task *try_pop_inbox() noexcept;
    void execute(Task *task) noexcept;

private:
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    struct IoWaitRegistration {
        int fd{-1};
        std::uint32_t events{0};
        Task *task{nullptr};
        IoResult *result{nullptr};
    };

    struct IoWaitEntry {
        IoWaitRegistration *read{nullptr};
        IoWaitRegistration *write{nullptr};
    };

    [[nodiscard]] static bool io_wait_entry_empty(const IoWaitEntry &entry) noexcept;
    [[nodiscard]] static bool
    io_wait_entry_contains(const IoWaitEntry &entry,
                           const IoWaitRegistration *registration) noexcept;
    [[nodiscard]] static bool io_wait_events_conflict(const IoWaitEntry &entry,
                                                      std::uint32_t events) noexcept;
    static void add_io_wait_registration(IoWaitEntry &entry,
                                         IoWaitRegistration *registration) noexcept;
    static void remove_io_wait_registration(IoWaitEntry &entry,
                                            const IoWaitRegistration *registration) noexcept;
    [[nodiscard]] static IoWaitRegistration *
    find_io_wait_registration(IoWaitEntry &entry, const IoResult *result) noexcept;
    [[nodiscard]] static bool io_wait_registration_ready(const IoWaitRegistration &registration,
                                                         std::uint32_t ready_events) noexcept;
    [[nodiscard]] static bool
    io_wait_registration_uses_native_backend(const IoWaitRegistration *registration) noexcept;
#endif

#if AF_DETAIL_HAS_KQUEUE
    struct KqueueTimeoutRegistration {
        Task *task{nullptr};
        IoResult *result{nullptr};
        KqueueTimeoutRegistration *prev{nullptr};
        KqueueTimeoutRegistration *next{nullptr};
        uintptr_t ident{0};
    };
#endif

    struct TimerEntry {
        std::int64_t deadline_ns{0};
        std::uint64_t sequence{0};
        Task *task{nullptr};
    };

    [[nodiscard]] bool io_thread() const noexcept {
        return native_io_thread();
    }

    [[nodiscard]] bool native_io_thread() const noexcept {
#if AF_DETAIL_HAS_EPOLL
        return kind_ == af::thread_kind::io;
#elif AF_DETAIL_HAS_KQUEUE
        return kind_ == af::thread_kind::io;
#else
        return false;
#endif
    }

#if AF_DETAIL_HAS_EPOLL
    [[nodiscard]] static std::uint64_t epoll_wait_token(int fd) noexcept;
    [[nodiscard]] static std::uint64_t epoll_wake_token() noexcept;
    [[nodiscard]] static std::uint64_t epoll_channel_token(detail::NetIoChannel *channel) noexcept;
    [[nodiscard]] static bool epoll_token_is_wake(std::uint64_t token) noexcept;
    [[nodiscard]] static bool epoll_token_is_channel(std::uint64_t token) noexcept;
    [[nodiscard]] static int epoll_token_fd(std::uint64_t token) noexcept;
    [[nodiscard]] static detail::NetIoChannel *epoll_token_channel(std::uint64_t token) noexcept;
    [[nodiscard]] static std::uint32_t native_poll_events(std::uint32_t events) noexcept;
    [[nodiscard]] static std::uint32_t io_events_from_poll(std::uint32_t events) noexcept;
    [[nodiscard]] static std::uint32_t net_events_from_poll(std::uint32_t events) noexcept;
    [[nodiscard]] static std::uint32_t io_events_from_native(std::uint32_t events) noexcept;
    [[nodiscard]] static std::uint32_t
    epoll_events_for_net_channel(const detail::NetIoChannel &channel,
                                 std::uint32_t events) noexcept;
    [[nodiscard]] bool update_epoll_net_channel_interest(detail::NetIoChannel *channel,
                                                         std::uint32_t events) noexcept;
    [[nodiscard]] bool update_net_channel_interest(detail::NetIoChannel *channel,
                                                   std::uint32_t events) noexcept;
    [[nodiscard]] bool native_io_backend_available() const noexcept;
    [[nodiscard]] bool notify_native_io_backend() noexcept;
    [[nodiscard]] bool init_native_io_backend() noexcept;
    void close_native_io_backend() noexcept;
    void clear_io_waits() noexcept;
    void reserve_io_backend_storage() noexcept;
    void drain_io_wake() noexcept;
    [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept;
    [[nodiscard]] static std::uint32_t epoll_events_for_entry(const IoWaitEntry &entry) noexcept;
    [[nodiscard]] bool update_epoll_interest(int fd, const IoWaitEntry &entry) noexcept;
    [[nodiscard]] bool register_native_io_wait(int fd, std::uint32_t events, Task *task,
                                               IoResult *result) noexcept;
    [[nodiscard]] bool cancel_native_io_wait(IoOpState &state) noexcept;
#elif AF_DETAIL_HAS_KQUEUE
    static constexpr uintptr_t kqueue_wake_ident = 1;

    [[nodiscard]] bool native_io_backend_available() const noexcept;
    [[nodiscard]] bool notify_native_io_backend() noexcept;
    [[nodiscard]] bool init_native_io_backend() noexcept;
    void close_native_io_backend() noexcept;
    [[nodiscard]] static intptr_t kqueue_timeout_data(std::chrono::nanoseconds timeout) noexcept;
    [[nodiscard]] static std::uint32_t kqueue_timeout_unit_flags() noexcept;
    [[nodiscard]] static intptr_t clamp_kqueue_timer_value(std::int64_t value) noexcept;
    void clear_kqueue_timeouts() noexcept;
    void track_kqueue_timeout(KqueueTimeoutRegistration *registration) noexcept;
    void untrack_kqueue_timeout(KqueueTimeoutRegistration *registration) noexcept;
    [[nodiscard]] uintptr_t next_kqueue_timeout_ident() noexcept;
    [[nodiscard]] bool register_kqueue_timeout(std::chrono::nanoseconds timeout, Task *task,
                                               IoResult *result) noexcept;
    [[nodiscard]] bool cancel_kqueue_timeout(IoOpState &state) noexcept;
    [[nodiscard]] bool complete_kqueue_timeout(KqueueTimeoutRegistration *registration,
                                               const struct kevent &event) noexcept;
    [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept;
    [[nodiscard]] bool update_net_channel_interest(detail::NetIoChannel *channel,
                                                   std::uint32_t events) noexcept;
    [[nodiscard]] static std::uint32_t net_events_from_kqueue(const struct kevent &event) noexcept;
    void clear_io_waits() noexcept;
    void reserve_native_io_wait_storage() noexcept;
    [[nodiscard]] bool register_native_io_wait(int fd, std::uint32_t events, Task *task,
                                               IoResult *result) noexcept;
    [[nodiscard]] bool cancel_native_io_wait(IoOpState &state) noexcept;
    [[nodiscard]] static int fill_kqueue_changes(int fd, std::uint32_t events,
                                                 IoWaitRegistration *registration,
                                                 std::array<struct kevent, 2> &changes) noexcept;
    void remove_kqueue_filters(const IoWaitRegistration &registration) noexcept;
    [[nodiscard]] static std::uint32_t io_events_from_kqueue(const struct kevent &event) noexcept;
    [[nodiscard]] static int io_error_from_kqueue(const struct kevent &event) noexcept;
#else
    [[nodiscard]] bool native_io_backend_available() const noexcept {
        return false;
    }

    [[nodiscard]] bool notify_native_io_backend() noexcept {
        return false;
    }

    [[nodiscard]] bool init_native_io_backend() noexcept {
        return false;
    }

    void close_native_io_backend() noexcept {}

    [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept {
        static_cast<void>(timeout_ms);
        return did_work;
    }

    [[nodiscard]] bool register_native_io_wait(int fd, std::uint32_t events, Task *task,
                                               IoResult *result) noexcept {
        static_cast<void>(events);
        static_cast<void>(task);
        return fail_io_result(result, fd, fd < 0 ? EBADF : ENOSYS);
    }

    [[nodiscard]] bool update_net_channel_interest(detail::NetIoChannel *channel,
                                                   std::uint32_t events) noexcept {
        static_cast<void>(channel);
        static_cast<void>(events);
        return false;
    }
#endif

    void notify_force() noexcept;
    void set_current_thread_name() noexcept;
    void run_loop() noexcept;
    void init_io_backend() noexcept;
    void close_io_backend() noexcept;
    [[nodiscard]] bool poll_io(int timeout_ms) noexcept;
    [[nodiscard]] static std::int64_t steady_now_ns() noexcept;
    [[nodiscard]] static bool timer_entry_after(const TimerEntry &left,
                                                const TimerEntry &right) noexcept;
    [[nodiscard]] int timer_poll_timeout_ms() const noexcept;
    [[nodiscard]] bool wait_for_wake_or_timer(std::uint32_t observed, int timeout_ms) noexcept;
    [[nodiscard]] bool handle_inbox_task(Task *task) noexcept;
    [[nodiscard]] bool arm_timer_from_inbox(Task *task) noexcept;
    [[nodiscard]] bool push_timer(Task *task) noexcept;
    [[nodiscard]] bool run_due_timers() noexcept;
    void cancel_timer_task(Task *task) noexcept;
    void cancel_timer_tasks() noexcept;
    [[nodiscard]] bool run_service_tasks() noexcept;

    Task *pop_one() noexcept;
    void finish_done(Task *task) noexcept;
    void finish_pending(Task *task) noexcept;
    void finish_again(Task *task) noexcept;

    std::uint16_t index_;
    af::thread_kind kind_{af::thread_kind::cpu};
    detail::IntrusiveMpscQueue<Task> inbox_;
    CacheLineAtomic<std::uint32_t> wake_epoch_{0};
    CacheLineAtomic<bool> sleeping_{false};
    CacheLineAtomic<bool> stop_requested_{false};
    Task *running_task_{nullptr};
    std::vector<TimerEntry> timers_;
    std::uint64_t next_timer_sequence_{0};
    std::vector<detail::RuntimeServiceTask *> service_tasks_;
    std::size_t next_service_task_{0};
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    absl::flat_hash_map<int, IoWaitEntry> io_waits_;
    IoObjectPool<IoWaitRegistration> io_wait_pool_;
    absl::flat_hash_map<int, detail::NetIoChannel *> net_channels_;
#endif
#if AF_DETAIL_HAS_EPOLL
    int io_epoll_fd_{-1};
    int io_wake_fd_{-1};
#endif
#if AF_DETAIL_HAS_KQUEUE
    int io_kqueue_fd_{-1};
    KqueueTimeoutRegistration *io_kqueue_timeouts_{nullptr};
    std::uint32_t io_kqueue_timeout_count_{0};
    uintptr_t io_kqueue_next_timeout_ident_{2};
    IoObjectPool<KqueueTimeoutRegistration> io_kqueue_timeout_pool_;
#endif
#if AF_DETAIL_HAS_EPOLL || AF_DETAIL_HAS_KQUEUE
    CacheLineAtomic<bool> io_wake_pending_{false};
#endif
    std::thread worker_;
};

} // namespace af::detail

#include "af/detail/runtime/runtime_executor_epoll_backend.hpp"
#include "af/detail/runtime/runtime_executor_kqueue_backend.hpp"
#include "af/detail/runtime/runtime_executor_io_backend.hpp"
#include "af/detail/runtime/runtime_executor_lifecycle.hpp"
#include "af/detail/runtime/runtime_executor_net_channel.hpp"
#include "af/detail/runtime/runtime_executor_service.hpp"
#include "af/detail/runtime/runtime_executor_timer.hpp"
#include "af/detail/runtime/runtime_executor_scheduler.hpp"
