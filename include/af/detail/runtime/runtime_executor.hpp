#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT> class alignas(hardware_cache_line_size) Executor {
    using Thread = typename RuntimeT::Thread;
    using Task = BasicTask<RuntimeT>;
    using RuntimeStatus = detail::RuntimeStatus;
    using ExternalQueue = typename RuntimeT::ExternalQueue;
    template <typename T> using CacheLineAtomic = detail::CacheLineAtomic<T>;
    template <typename T> using IoObjectPool = detail::ObjectPool<T, 256, 1, false, 1>;

    static constexpr std::uint16_t thread_count = RuntimeT::thread_count;
    static constexpr std::uint16_t invalid_thread_index = RuntimeT::invalid_thread_index;
    static constexpr std::size_t spsc_queue_capacity = RuntimeT::spsc_queue_capacity;
    static constexpr std::size_t io_wait_reserve = RuntimeT::io_wait_reserve;
    static constexpr unsigned io_uring_entries = RuntimeT::io_uring_entries;
    static constexpr unsigned io_uring_submit_batch_threshold =
        RuntimeT::io_uring_submit_batch_threshold;
    static constexpr unsigned io_uring_cq_entries = RuntimeT::io_uring_cq_entries;
    static constexpr unsigned io_uring_setup_flags = RuntimeT::io_uring_setup_flags;
    static constexpr bool io_uring_setup_sqpoll = RuntimeT::io_uring_setup_sqpoll;
    static constexpr unsigned io_uring_sqpoll_idle_ms = RuntimeT::io_uring_sqpoll_idle_ms;
    static constexpr int io_uring_sqpoll_cpu = RuntimeT::io_uring_sqpoll_cpu;
    static constexpr bool io_uring_setup_submit_all = RuntimeT::io_uring_setup_submit_all;
    static constexpr bool io_uring_setup_coop_taskrun = RuntimeT::io_uring_setup_coop_taskrun;
    static constexpr bool io_uring_setup_single_issuer = RuntimeT::io_uring_setup_single_issuer;
    static constexpr bool io_uring_setup_defer_taskrun = RuntimeT::io_uring_setup_defer_taskrun;
    static constexpr std::size_t io_uring_provided_buffer_group_reserve =
        RuntimeT::io_uring_provided_buffer_group_reserve;

    [[nodiscard]] static constexpr Thread thread_from_index(std::uint16_t index) noexcept {
        return RuntimeT::thread_from_index(index);
    }

    [[nodiscard]] static constexpr ThreadKind thread_kind(Thread thread) noexcept {
        return RuntimeT::thread_kind(thread);
    }

    [[nodiscard]] static constexpr std::string_view thread_name(Thread thread) noexcept {
        return RuntimeT::thread_name(thread);
    }

    [[nodiscard]] static constexpr std::uint16_t thread_group_offset(Thread thread) noexcept {
        return RuntimeT::thread_group_offset(thread);
    }

    [[nodiscard]] static auto &spsc_queue(std::uint16_t source, std::uint16_t target) noexcept {
        return RuntimeT::spsc_queue(source, target);
    }

    static void on_task_finished(Task *task) noexcept {
        RuntimeT::on_task_finished(task);
    }

    static void enqueue_pending_blocking(std::uint16_t index, Task *task) noexcept {
        RuntimeT::enqueue_pending_blocking(index, task);
    }

    static void enqueue_ready_blocking_from_runtime_thread(std::uint16_t source,
                                                           std::uint16_t target,
                                                           Task *task) noexcept {
        RuntimeT::enqueue_ready_blocking_from_runtime_thread(source, target, task);
    }

#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    struct IoWaitRegistration;
#endif
#if AF_DETAIL_HAS_KQUEUE
    struct KqueueTimeoutRegistration;
#endif
#if defined(__linux__)
    struct IoUringOperation;
#endif

public:
    explicit Executor(std::uint16_t index);
    Executor(const Executor &) = delete;
    Executor &operator=(const Executor &) = delete;
    ~Executor();

    void start();
    void request_stop() noexcept;
    void join();
    void notify() noexcept;

    [[nodiscard]] bool io_backend_available() const noexcept;
    [[nodiscard]] bool io_uring_backend_available() const noexcept;
    [[nodiscard]] int io_uring_backend_error() const noexcept;
    [[nodiscard]] bool io_uring_poll_available() const noexcept;

#if !defined(_WIN32)
    [[nodiscard]] bool register_io_uring_buffers(const iovec *buffers, unsigned buffer_count,
                                                 int *error) noexcept;
    [[nodiscard]] bool unregister_io_uring_buffers(int *error) noexcept;
#endif
    [[nodiscard]] bool register_io_uring_provided_buffer_ring(void *ring, unsigned ring_entries,
                                                              std::uint16_t buffer_group,
                                                              int *error) noexcept;
    [[nodiscard]] bool unregister_io_uring_provided_buffer_ring(std::uint16_t buffer_group,
                                                                int *error) noexcept;
    [[nodiscard]] bool register_io_uring_files(const int *files, unsigned file_count,
                                               int *error) noexcept;
    [[nodiscard]] bool unregister_io_uring_files(int *error) noexcept;
    [[nodiscard]] bool update_io_uring_files(unsigned offset, const int *files, unsigned file_count,
                                             int *error) noexcept;

    [[nodiscard]] bool register_io_wait(int fd, std::uint32_t events, Task *task, IoResult *result,
                                        bool prefer_rearm = false) noexcept {
        AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
                  "io_wait must be called from its IO thread");
        if (RuntimeT::current_thread_index_ != index_ || task == nullptr || result == nullptr) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = EINVAL;
            }
            return false;
        }

        return register_native_io_wait(fd, events, task, result, prefer_rearm);
    }

#if defined(__linux__)
    [[nodiscard]] bool cancel_io_completion(IoOpState &state) noexcept {
        if (io_uring_fd_ < 0) {
            state.wait.events = io_error;
            state.wait.error = ENOSYS;
            state.wait.result = -ENOSYS;
            return false;
        }

        auto *operation = static_cast<IoUringOperation *>(state.wait.completion_token);
        if (operation == nullptr || operation->result != &state.wait || operation->poll_wait) {
            state.wait.events = io_error;
            state.wait.error = ENOENT;
            state.wait.result = -ENOENT;
            return false;
        }
        if (operation->opcode == IORING_OP_CLOSE) {
            state.wait.events = io_error;
            state.wait.error = EOPNOTSUPP;
            state.wait.result = -EOPNOTSUPP;
            return false;
        }
        if (operation->cancel_requested) {
            return true;
        }

        const int submit_error = submit_io_uring_cancel(operation);
        if (submit_error != 0) {
            state.wait.events = io_error;
            state.wait.error = submit_error;
            state.wait.result = -submit_error;
            return false;
        }

        operation->cancel_requested = true;
        return true;
    }
#endif

    [[nodiscard]] bool cancel_io(IoOpState &state) noexcept {
        AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
                  "cancel_io must be called from its IO thread");
        if (state.waiting && state.wait.error == ECANCELED) {
            return true;
        }
        if (RuntimeT::current_thread_index_ != index_) {
            state.wait.events = io_error;
            state.wait.error = EINVAL;
            state.wait.result = -EINVAL;
            return false;
        }
        if (!state.waiting) {
            state.wait.events = io_error;
            state.wait.error = ENOENT;
            state.wait.result = -ENOENT;
            return false;
        }

#if defined(__linux__)
        if (state.wait_kind == IoWaitKind::Readiness) {
            return cancel_native_io_wait(state);
        }
        if (state.wait_kind == IoWaitKind::Completion) {
            return cancel_io_completion(state);
        }
#elif AF_DETAIL_HAS_NATIVE_IO_WAIT
        if (state.wait_kind == IoWaitKind::Readiness) {
            return cancel_native_io_wait(state);
        }
#if AF_DETAIL_HAS_KQUEUE
        if (state.wait_kind == IoWaitKind::Completion) {
            return cancel_kqueue_timeout(state);
        }
#endif
#endif
        state.wait.events = io_error;
        state.wait.error = ENOSYS;
        state.wait.result = -ENOSYS;
        return false;
    }

    [[nodiscard]] bool submit_io_uring_read(int fd, void *data, std::size_t size,
                                            std::uint64_t offset, Task *task,
                                            IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_buffer_op(IORING_OP_READ, fd, data, size, offset, 0, io_readable,
                                         task, result);
#else
        static_cast<void>(fd);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(offset);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_write(int fd, const void *data, std::size_t size,
                                             std::uint64_t offset, Task *task,
                                             IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_buffer_op(IORING_OP_WRITE, fd, const_cast<void *>(data), size,
                                         offset, 0, io_writable, task, result);
#else
        static_cast<void>(fd);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(offset);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
    [[nodiscard]] bool submit_io_timeout(std::chrono::nanoseconds timeout, Task *task,
                                         IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_timeout(timeout, task, result);
#elif AF_DETAIL_HAS_KQUEUE
        return submit_kqueue_timeout(timeout, task, result);
#else
        static_cast<void>(timeout);
        static_cast<void>(task);
        if (result != nullptr) {
            result->fd = -1;
            result->events = io_error;
            result->error = ENOSYS;
            result->result = -ENOSYS;
            result->completion_token = nullptr;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_timeout(std::chrono::nanoseconds timeout, Task *task,
                                               IoResult *result) noexcept {
#if defined(__linux__)
        AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
                  "io_uring timeout submit must be called from its IO thread");
        if (result != nullptr) {
            result->completion_token = nullptr;
        }
        if (RuntimeT::current_thread_index_ != index_ || task == nullptr || result == nullptr ||
            timeout.count() <= 0) {
            if (result != nullptr) {
                result->fd = -1;
                result->events = io_error;
                result->error = EINVAL;
                result->result = -EINVAL;
            }
            return false;
        }
        if (io_uring_fd_ < 0) {
            result->fd = -1;
            result->events = io_error;
            result->error = ENOSYS;
            result->result = -ENOSYS;
            return false;
        }

        IoUringOperation *operation = nullptr;
        try {
            operation = io_uring_op_pool_.create();
        } catch (...) {
            result->fd = -1;
            result->events = io_error;
            result->error = ENOMEM;
            result->result = -ENOMEM;
            return false;
        }

        const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(timeout);
        const auto nanoseconds = timeout - seconds;
        operation->task = task;
        operation->result = result;
        operation->prev = nullptr;
        operation->next = nullptr;
        operation->msg = nullptr;
        operation->socket_address = nullptr;
        operation->wait_registration = nullptr;
        operation->timeout.tv_sec = seconds.count();
        operation->timeout.tv_nsec = nanoseconds.count();
        operation->complete_events = io_readable;
        operation->direct_file_index = -1;
        operation->opcode = IORING_OP_TIMEOUT;
        operation->cancel_requested = false;
        operation->multishot = false;
        operation->poll_wait = false;
        operation->zero_copy_send = false;
        operation->zero_copy_primary_done = false;
        operation->zero_copy_notification_done = false;

        int reserve_error = 0;
        io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
        if (sqe == nullptr) {
            destroy_io_uring_operation(operation);
            result->fd = -1;
            result->events = io_error;
            result->error = reserve_error == 0 ? EBUSY : reserve_error;
            result->result = -result->error;
            return false;
        }

        track_io_uring_operation(operation);

        *sqe = io_uring_sqe{};
        sqe->opcode = IORING_OP_TIMEOUT;
        sqe->fd = -1;
        sqe->addr = reinterpret_cast<std::uint64_t>(&operation->timeout);
        sqe->len = 1U;
        sqe->off = 0U;
        sqe->timeout_flags = 0U;
        sqe->user_data = reinterpret_cast<std::uint64_t>(operation);

        result->fd = -1;
        result->events = 0;
        result->error = 0;
        result->result = 0;
        result->completion_token = operation;

        if (io_uring_pending_submissions_ >= io_uring_submit_batch_threshold) {
            const int submit_error = flush_io_uring_submissions();
            if (submit_error == 0) {
                return true;
            }
            result->events = io_error;
            result->error = submit_error;
            result->result = -submit_error;
            fail_io_uring_backend(submit_error, operation);
            return false;
        }
        return true;
#else
        static_cast<void>(timeout);
        static_cast<void>(task);
        if (result != nullptr) {
            result->fd = -1;
            result->events = io_error;
            result->error = ENOSYS;
            result->result = -ENOSYS;
            result->completion_token = nullptr;
        }
        return false;
#endif
    }
    [[nodiscard]] bool submit_io_uring_read_fixed_file(int file_index, void *data, std::size_t size,
                                                       std::uint64_t offset, Task *task,
                                                       IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fixed_file_rw(IORING_OP_READ, file_index, data, size, offset,
                                             io_readable, task, result);
#else
        static_cast<void>(file_index);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(offset);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_write_fixed_file(int file_index, const void *data,
                                                        std::size_t size, std::uint64_t offset,
                                                        Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fixed_file_rw(IORING_OP_WRITE, file_index, const_cast<void *>(data),
                                             size, offset, io_writable, task, result);
#else
        static_cast<void>(file_index);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(offset);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
#if !defined(_WIN32)
    [[nodiscard]] bool submit_io_uring_readv_fixed_file(int file_index, const iovec *iov,
                                                        int iov_count, std::uint64_t offset,
                                                        Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fixed_file_rw(IORING_OP_READV, file_index, const_cast<iovec *>(iov),
                                             static_cast<std::size_t>(iov_count), offset,
                                             io_readable, task, result);
#else
        static_cast<void>(file_index);
        static_cast<void>(iov);
        static_cast<void>(iov_count);
        static_cast<void>(offset);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_writev_fixed_file(int file_index, const iovec *iov,
                                                         int iov_count, std::uint64_t offset,
                                                         Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fixed_file_rw(IORING_OP_WRITEV, file_index, const_cast<iovec *>(iov),
                                             static_cast<std::size_t>(iov_count), offset,
                                             io_writable, task, result);
#else
        static_cast<void>(file_index);
        static_cast<void>(iov);
        static_cast<void>(iov_count);
        static_cast<void>(offset);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

#endif
#if !defined(_WIN32)
    [[nodiscard]] bool submit_io_uring_read_fixed_file(int file_index, void *data, std::size_t size,
                                                       std::uint64_t offset,
                                                       std::uint16_t buffer_index, Task *task,
                                                       IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fixed_file_rw(IORING_OP_READ_FIXED, file_index, data, size, offset,
                                             io_readable, task, result, buffer_index, true);
#else
        static_cast<void>(file_index);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(offset);
        static_cast<void>(buffer_index);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_write_fixed_file(int file_index, const void *data,
                                                        std::size_t size, std::uint64_t offset,
                                                        std::uint16_t buffer_index, Task *task,
                                                        IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fixed_file_rw(IORING_OP_WRITE_FIXED, file_index,
                                             const_cast<void *>(data), size, offset, io_writable,
                                             task, result, buffer_index, true);
#else
        static_cast<void>(file_index);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(offset);
        static_cast<void>(buffer_index);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

#endif
#if !defined(_WIN32)
    [[nodiscard]] bool submit_io_uring_read_fixed(int fd, void *data, std::size_t size,
                                                  std::uint64_t offset, std::uint16_t buffer_index,
                                                  Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_READ_FIXED, fd, data, size, offset, 0, io_readable,
                                  task, result, nullptr, 0, nullptr, nullptr, 0, nullptr, nullptr,
                                  nullptr, 0, 0, -1, buffer_index, false);
#else
        static_cast<void>(fd);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(offset);
        static_cast<void>(buffer_index);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_write_fixed(int fd, const void *data, std::size_t size,
                                                   std::uint64_t offset, std::uint16_t buffer_index,
                                                   Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_WRITE_FIXED, fd, const_cast<void *>(data), size, offset,
                                  0, io_writable, task, result, nullptr, 0, nullptr, nullptr, 0,
                                  nullptr, nullptr, nullptr, 0, 0, -1, buffer_index, false);
#else
        static_cast<void>(fd);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(offset);
        static_cast<void>(buffer_index);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

#endif
#if !defined(_WIN32)
    [[nodiscard]] bool submit_io_uring_readv(int fd, const iovec *iov, int iov_count,
                                             std::uint64_t offset, Task *task,
                                             IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_READV, fd, const_cast<iovec *>(iov),
                                  static_cast<std::size_t>(iov_count), offset, 0, io_readable, task,
                                  result);
#else
        static_cast<void>(fd);
        static_cast<void>(iov);
        static_cast<void>(iov_count);
        static_cast<void>(offset);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_writev(int fd, const iovec *iov, int iov_count,
                                              std::uint64_t offset, Task *task,
                                              IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_WRITEV, fd, const_cast<iovec *>(iov),
                                  static_cast<std::size_t>(iov_count), offset, 0, io_writable, task,
                                  result);
#else
        static_cast<void>(fd);
        static_cast<void>(iov);
        static_cast<void>(iov_count);
        static_cast<void>(offset);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
#endif
    [[nodiscard]] bool submit_io_uring_fsync(int fd, std::uint32_t flags, Task *task,
                                             IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_FSYNC, fd, nullptr, 0, 0, flags, io_writable, task,
                                  result);
#else
        static_cast<void>(fd);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_fsync_fixed_file(int file_index, std::uint32_t flags,
                                                        Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_FSYNC, file_index, nullptr, 0, 0, flags, io_writable,
                                  task, result, nullptr, 0, nullptr, nullptr, 0, nullptr, nullptr,
                                  nullptr, 0, 0, -1, 0, true);
#else
        static_cast<void>(file_index);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_openat(int dir_fd, const char *path, int flags,
                                              std::uint32_t mode, Task *task,
                                              IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_OPENAT, dir_fd, const_cast<char *>(path), mode, 0,
                                  static_cast<std::uint32_t>(flags), io_readable, task, result);
#else
        static_cast<void>(dir_fd);
        static_cast<void>(path);
        static_cast<void>(flags);
        static_cast<void>(mode);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_openat_direct(int dir_fd, const char *path, int flags,
                                                     std::uint32_t mode, int file_index, Task *task,
                                                     IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_OPENAT, dir_fd, const_cast<char *>(path), mode, 0,
                                  static_cast<std::uint32_t>(flags), io_readable, task, result,
                                  nullptr, 0, nullptr, nullptr, 0, nullptr, nullptr, nullptr, 0, 0,
                                  -1, 0, false, false, false, 0, false, file_index);
#else
        static_cast<void>(dir_fd);
        static_cast<void>(path);
        static_cast<void>(flags);
        static_cast<void>(mode);
        static_cast<void>(file_index);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_openat2(int dir_fd, const char *path,
                                               const struct open_how *how, Task *task,
                                               IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fast_sqe(detail::io_uring_op_openat2, dir_fd, io_readable, task,
                                        result, [&](io_uring_sqe &sqe) noexcept {
                                            sqe.fd = dir_fd;
                                            sqe.addr = reinterpret_cast<std::uint64_t>(path);
                                            sqe.len = static_cast<unsigned>(sizeof(*how));
                                            sqe.off = reinterpret_cast<std::uint64_t>(how);
                                        });
#else
        static_cast<void>(dir_fd);
        static_cast<void>(path);
        static_cast<void>(how);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
    [[nodiscard]] bool submit_io_uring_close(int fd, Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_CLOSE, fd, nullptr, 0, 0, 0, io_writable, task, result);
#else
        static_cast<void>(fd);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_shutdown(int fd, int how, Task *task,
                                                IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_SHUTDOWN, fd, nullptr, static_cast<std::size_t>(how), 0,
                                  0, io_writable, task, result);
#else
        static_cast<void>(fd);
        static_cast<void>(how);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
    [[nodiscard]] bool submit_io_uring_statx(int dir_fd, const char *path, int flags,
                                             std::uint32_t mask, struct statx *output, Task *task,
                                             IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fast_sqe(IORING_OP_STATX, dir_fd, io_readable, task, result,
                                        [&](io_uring_sqe &sqe) noexcept {
                                            sqe.fd = dir_fd;
                                            sqe.addr = reinterpret_cast<std::uint64_t>(path);
                                            sqe.len = mask;
                                            sqe.off = reinterpret_cast<std::uint64_t>(output);
                                            sqe.statx_flags = static_cast<std::uint32_t>(flags);
                                        });
#else
        static_cast<void>(dir_fd);
        static_cast<void>(path);
        static_cast<void>(flags);
        static_cast<void>(mask);
        static_cast<void>(output);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
    [[nodiscard]] bool submit_io_uring_fallocate(int fd, int mode, std::uint64_t offset,
                                                 std::uint64_t length, Task *task,
                                                 IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fast_sqe(IORING_OP_FALLOCATE, fd, io_writable, task, result,
                                        [&](io_uring_sqe &sqe) noexcept {
                                            sqe.fd = fd;
                                            sqe.addr = length;
                                            sqe.len = static_cast<unsigned>(mode);
                                            sqe.off = offset;
                                        });
#else
        static_cast<void>(fd);
        static_cast<void>(mode);
        static_cast<void>(offset);
        static_cast<void>(length);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_ftruncate(int fd, std::uint64_t length, Task *task,
                                                 IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fast_sqe(detail::io_uring_op_ftruncate, fd, io_writable, task,
                                        result, [&](io_uring_sqe &sqe) noexcept {
                                            sqe.fd = fd;
                                            sqe.off = length;
                                        });
#else
        static_cast<void>(fd);
        static_cast<void>(length);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
    [[nodiscard]] bool submit_io_uring_renameat(int old_dir_fd, const char *old_path,
                                                int new_dir_fd, const char *new_path,
                                                std::uint32_t flags, Task *task,
                                                IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fast_sqe(IORING_OP_RENAMEAT, old_dir_fd, io_writable, task, result,
                                        [&](io_uring_sqe &sqe) noexcept {
                                            sqe.fd = old_dir_fd;
                                            sqe.addr = reinterpret_cast<std::uint64_t>(old_path);
                                            sqe.len = static_cast<unsigned>(new_dir_fd);
                                            sqe.off = reinterpret_cast<std::uint64_t>(new_path);
                                            sqe.rename_flags = flags;
                                        });
#else
        static_cast<void>(old_dir_fd);
        static_cast<void>(old_path);
        static_cast<void>(new_dir_fd);
        static_cast<void>(new_path);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_unlinkat(int dir_fd, const char *path, int flags, Task *task,
                                                IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fast_sqe(IORING_OP_UNLINKAT, dir_fd, io_writable, task, result,
                                        [&](io_uring_sqe &sqe) noexcept {
                                            sqe.fd = dir_fd;
                                            sqe.addr = reinterpret_cast<std::uint64_t>(path);
                                            sqe.unlink_flags = static_cast<std::uint32_t>(flags);
                                        });
#else
        static_cast<void>(dir_fd);
        static_cast<void>(path);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_mkdirat(int dir_fd, const char *path, std::uint32_t mode,
                                               Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fast_sqe(detail::io_uring_op_mkdirat, dir_fd, io_writable, task,
                                        result, [&](io_uring_sqe &sqe) noexcept {
                                            sqe.fd = dir_fd;
                                            sqe.addr = reinterpret_cast<std::uint64_t>(path);
                                            sqe.len = mode;
                                        });
#else
        static_cast<void>(dir_fd);
        static_cast<void>(path);
        static_cast<void>(mode);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_symlinkat(const char *target, int new_dir_fd,
                                                 const char *link_path, Task *task,
                                                 IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fast_sqe(detail::io_uring_op_symlinkat, new_dir_fd, io_writable,
                                        task, result, [&](io_uring_sqe &sqe) noexcept {
                                            sqe.fd = new_dir_fd;
                                            sqe.addr = reinterpret_cast<std::uint64_t>(target);
                                            sqe.off = reinterpret_cast<std::uint64_t>(link_path);
                                        });
#else
        static_cast<void>(target);
        static_cast<void>(new_dir_fd);
        static_cast<void>(link_path);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_linkat(int old_dir_fd, const char *old_path, int new_dir_fd,
                                              const char *new_path, std::uint32_t flags, Task *task,
                                              IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_fast_sqe(detail::io_uring_op_linkat, old_dir_fd, io_writable, task,
                                        result, [&](io_uring_sqe &sqe) noexcept {
                                            sqe.fd = old_dir_fd;
                                            sqe.addr = reinterpret_cast<std::uint64_t>(old_path);
                                            sqe.len = static_cast<unsigned>(new_dir_fd);
                                            sqe.off = reinterpret_cast<std::uint64_t>(new_path);
                                            sqe.rw_flags = flags;
                                        });
#else
        static_cast<void>(old_dir_fd);
        static_cast<void>(old_path);
        static_cast<void>(new_dir_fd);
        static_cast<void>(new_path);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
    [[nodiscard]] bool submit_io_uring_splice(int in_fd, std::int64_t off_in, int out_fd,
                                              std::int64_t off_out, std::size_t count,
                                              unsigned int flags, Task *task,
                                              IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_SPLICE, out_fd, nullptr, count,
                                  static_cast<std::uint64_t>(off_out), flags, io_writable, task,
                                  result, nullptr, 0, nullptr, nullptr, 0, nullptr, nullptr,
                                  nullptr, 0, static_cast<std::uint64_t>(off_in), in_fd);
#else
        static_cast<void>(in_fd);
        static_cast<void>(off_in);
        static_cast<void>(out_fd);
        static_cast<void>(off_out);
        static_cast<void>(count);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_recv(int fd, void *data, std::size_t size,
                                            std::uint32_t flags, Task *task,
                                            IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_buffer_op(IORING_OP_RECV, fd, data, size, 0, flags, io_readable,
                                         task, result);
#else
        static_cast<void>(fd);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_recv_fixed_file(int file_index, void *data, std::size_t size,
                                                       std::uint32_t flags, Task *task,
                                                       IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_RECV, file_index, data, size, 0, flags, io_readable,
                                  task, result, nullptr, 0, nullptr, nullptr, 0, nullptr, nullptr,
                                  nullptr, 0, 0, -1, 0, true);
#else
        static_cast<void>(file_index);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
#if defined(__linux__)
    [[nodiscard]] bool submit_io_uring_recv_multishot(int fd, std::uint16_t buffer_group,
                                                      std::uint32_t flags, Task *task,
                                                      IoResult *result) noexcept {
        if (!provided_buffer_group_registered(buffer_group)) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = ENOBUFS;
            }
            return false;
        }
        return submit_io_uring_op(IORING_OP_RECV, fd, nullptr, 0, 0, flags, io_readable, task,
                                  result, nullptr, 0, nullptr, nullptr, 0, nullptr, nullptr,
                                  nullptr, 0, 0, -1, 0, false, true, false, buffer_group, true);
    }

    [[nodiscard]] bool submit_io_uring_recvmsg_multishot(int fd, std::uint16_t buffer_group,
                                                         socklen_t name_capacity,
                                                         std::size_t control_capacity,
                                                         std::uint32_t flags, Task *task,
                                                         IoResult *result) noexcept {
        if (!provided_buffer_group_registered(buffer_group)) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = ENOBUFS;
            }
            return false;
        }
        return submit_io_uring_op(IORING_OP_RECVMSG, fd, nullptr, control_capacity, 0, flags,
                                  io_readable, task, result, nullptr, name_capacity, nullptr,
                                  nullptr, 0, nullptr, nullptr, nullptr, 0, 0, -1, 0, false, true,
                                  false, buffer_group, true);
    }
#endif
    [[nodiscard]] bool submit_io_uring_send(int fd, const void *data, std::size_t size,
                                            std::uint32_t flags, Task *task,
                                            IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_buffer_op(IORING_OP_SEND, fd, const_cast<void *>(data), size, 0,
                                         flags, io_writable, task, result);
#else
        static_cast<void>(fd);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_send_fixed_file(int file_index, const void *data,
                                                       std::size_t size, std::uint32_t flags,
                                                       Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_SEND, file_index, const_cast<void *>(data), size, 0,
                                  flags, io_writable, task, result, nullptr, 0, nullptr, nullptr, 0,
                                  nullptr, nullptr, nullptr, 0, 0, -1, 0, true);
#else
        static_cast<void>(file_index);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
#if defined(__linux__)
    [[nodiscard]] bool submit_io_uring_send_zc(int fd, const void *data, std::size_t size,
                                               std::uint32_t flags, Task *task,
                                               IoResult *result) noexcept {
        if (!io_uring_send_zc_available_) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = ENOSYS;
            }
            return false;
        }
        return submit_io_uring_op(detail::io_uring_op_send_zc, fd, const_cast<void *>(data), size,
                                  0, flags, io_writable, task, result, nullptr, 0, nullptr, nullptr,
                                  0, nullptr, nullptr, nullptr, 0, 0, -1, 0, false, false, true);
    }

    [[nodiscard]] bool submit_io_uring_sendmsg_zc(int fd, const void *data, std::size_t size,
                                                  const sockaddr *address, socklen_t address_size,
                                                  std::uint32_t flags, Task *task,
                                                  IoResult *result) noexcept {
        if (!io_uring_sendmsg_zc_available_) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = ENOSYS;
            }
            return false;
        }
        return submit_io_uring_op(detail::io_uring_op_sendmsg_zc, fd, const_cast<void *>(data),
                                  size, 0, flags, io_writable, task, result,
                                  const_cast<sockaddr *>(address), address_size, nullptr, nullptr,
                                  0, nullptr, nullptr, nullptr, 0, 0, -1, 0, false, false, true);
    }

    [[nodiscard]] bool submit_io_uring_sendmsg_zc_iov(int fd, const iovec *iov, int iov_count,
                                                      const sockaddr *address,
                                                      socklen_t address_size, std::uint32_t flags,
                                                      Task *task, IoResult *result) noexcept {
        if (!io_uring_sendmsg_zc_available_) {
            if (result != nullptr) {
                result->fd = fd;
                result->events = io_error;
                result->error = ENOSYS;
            }
            return false;
        }
        return submit_io_uring_op(
            detail::io_uring_op_sendmsg_zc, fd, nullptr, 0, 0, flags, io_writable, task, result,
            const_cast<sockaddr *>(address), address_size, nullptr, nullptr, 0, nullptr, nullptr,
            iov, static_cast<std::size_t>(iov_count), 0, -1, 0, false, false, true);
    }
#endif
#if !defined(_WIN32)
    [[nodiscard]] bool submit_io_uring_recvmsg_fixed_file_iov(int file_index, const iovec *iov,
                                                              int iov_count, std::uint32_t flags,
                                                              Task *task,
                                                              IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_RECVMSG, file_index, nullptr, 0, 0, flags, io_readable,
                                  task, result, nullptr, 0, nullptr, nullptr, 0, nullptr, nullptr,
                                  iov, static_cast<std::size_t>(iov_count), 0, -1, 0, true);
#else
        static_cast<void>(file_index);
        static_cast<void>(iov);
        static_cast<void>(iov_count);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_recvmsg_iov(int fd, const iovec *iov, int iov_count,
                                                   sockaddr *address, socklen_t *address_size,
                                                   std::uint32_t flags, Task *task,
                                                   IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_RECVMSG, fd, nullptr, 0, 0, flags, io_readable, task,
                                  result, address, address_size == nullptr ? 0 : *address_size,
                                  address_size, nullptr, 0, nullptr, nullptr, iov,
                                  static_cast<std::size_t>(iov_count));
#else
        static_cast<void>(fd);
        static_cast<void>(iov);
        static_cast<void>(iov_count);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_recvmsg(int fd, void *data, std::size_t size,
                                               sockaddr *address, socklen_t *address_size,
                                               std::uint32_t flags, Task *task,
                                               IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_RECVMSG, fd, data, size, 0, flags, io_readable, task,
                                  result, address, address_size == nullptr ? 0 : *address_size,
                                  address_size);
#else
        static_cast<void>(fd);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

#endif
#if !defined(_WIN32)
    [[nodiscard]] bool submit_io_uring_sendmsg_fixed_file_iov(int file_index, const iovec *iov,
                                                              int iov_count, std::uint32_t flags,
                                                              Task *task,
                                                              IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_SENDMSG, file_index, nullptr, 0, 0, flags, io_writable,
                                  task, result, nullptr, 0, nullptr, nullptr, 0, nullptr, nullptr,
                                  iov, static_cast<std::size_t>(iov_count), 0, -1, 0, true);
#else
        static_cast<void>(file_index);
        static_cast<void>(iov);
        static_cast<void>(iov_count);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_sendmsg_iov(int fd, const iovec *iov, int iov_count,
                                                   const sockaddr *address, socklen_t address_size,
                                                   std::uint32_t flags, Task *task,
                                                   IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_SENDMSG, fd, nullptr, 0, 0, flags, io_writable, task,
                                  result, const_cast<sockaddr *>(address), address_size, nullptr,
                                  nullptr, 0, nullptr, nullptr, iov,
                                  static_cast<std::size_t>(iov_count));
#else
        static_cast<void>(fd);
        static_cast<void>(iov);
        static_cast<void>(iov_count);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_sendmsg(int fd, const void *data, std::size_t size,
                                               const sockaddr *address, socklen_t address_size,
                                               std::uint32_t flags, Task *task,
                                               IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_SENDMSG, fd, const_cast<void *>(data), size, 0, flags,
                                  io_writable, task, result, const_cast<sockaddr *>(address),
                                  address_size, nullptr);
#else
        static_cast<void>(fd);
        static_cast<void>(data);
        static_cast<void>(size);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

#endif
#if !defined(_WIN32)
    [[nodiscard]] bool submit_io_uring_accept(int fd, sockaddr *address, socklen_t *address_size,
                                              int flags, Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_ACCEPT, fd, nullptr, 0, 0,
                                  static_cast<std::uint32_t>(flags), io_readable, task, result,
                                  nullptr, 0, nullptr, nullptr, 0, address, address_size);
#else
        static_cast<void>(fd);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_accept_direct(int fd, sockaddr *address,
                                                     socklen_t *address_size, int flags,
                                                     int file_index, Task *task,
                                                     IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_ACCEPT, fd, nullptr, 0, 0,
                                  static_cast<std::uint32_t>(flags), io_readable, task, result,
                                  nullptr, 0, nullptr, nullptr, 0, address, address_size, nullptr,
                                  0, 0, -1, 0, false, false, false, 0, false, file_index);
#else
        static_cast<void>(fd);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        static_cast<void>(file_index);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    [[nodiscard]] bool submit_io_uring_accept_multishot(int fd, sockaddr *address,
                                                        socklen_t *address_size, int flags,
                                                        Task *task, IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_ACCEPT, fd, nullptr, 0, 0,
                                  static_cast<std::uint32_t>(flags), io_readable, task, result,
                                  nullptr, 0, nullptr, nullptr, 0, address, address_size, nullptr,
                                  0, 0, -1, 0, false, true);
#else
        static_cast<void>(fd);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
#endif
#if !defined(_WIN32)
    [[nodiscard]] bool submit_io_uring_connect(int fd, const sockaddr *address,
                                               socklen_t address_size, Task *task,
                                               IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_op(IORING_OP_CONNECT, fd, nullptr, 0, 0, 0, io_writable, task,
                                  result, nullptr, 0, nullptr, address, address_size, nullptr,
                                  nullptr);
#else
        static_cast<void>(fd);
        static_cast<void>(address);
        static_cast<void>(address_size);
        static_cast<void>(task);
        if (result != nullptr) {
            result->error = ENOSYS;
        }
        return false;
#endif
    }
#endif
    [[nodiscard]] bool submit_io_uring_socket(int domain, int type, int protocol,
                                              std::uint32_t flags, Task *task,
                                              IoResult *result) noexcept {
#if defined(__linux__)
        return submit_io_uring_socket_impl(domain, type, protocol, flags, task, result);
#else
        static_cast<void>(domain);
        static_cast<void>(type);
        static_cast<void>(protocol);
        static_cast<void>(flags);
        static_cast<void>(task);
        if (result != nullptr) {
            result->fd = -1;
            result->events = io_error;
            result->error = ENOSYS;
        }
        return false;
#endif
    }

    void mark_ready(std::uint16_t source) noexcept;
    void notify_external_ready() noexcept;
    [[nodiscard]] bool try_push_local(Task *task) noexcept;
    [[nodiscard]] Task *try_pop_local() noexcept;
    void execute(Task *task) noexcept;

private:
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    struct IoWaitRegistration {
        int fd{-1};
        std::uint32_t events{0};
        Task *task{nullptr};
        IoResult *result{nullptr};
#if defined(__linux__)
        IoUringOperation *poll_operation{nullptr};
#endif
    };

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

#if defined(__linux__)
    struct IoUringOperation {
        Task *task{nullptr};
        IoResult *result{nullptr};
        IoUringOperation *prev{nullptr};
        IoUringOperation *next{nullptr};
        detail::IoUringMessage *msg{nullptr};
        union {
            detail::IoUringSocketAddress *socket_address;
            __kernel_timespec timeout;
        };
        IoWaitRegistration *wait_registration{nullptr};
        std::uint32_t complete_events{0};
        int direct_file_index{-1};
        std::uint8_t opcode{0};
        bool cancel_requested{false};
        bool multishot{false};
        bool poll_wait{false};
        bool zero_copy_send{false};
        bool zero_copy_primary_done{false};
        bool zero_copy_notification_done{false};
    };

    enum class IoUringPollSubmitResult : std::uint8_t {
        Submitted,
        Fallback,
        Failed,
        BackendClosed,
    };
#endif
    [[nodiscard]] bool io_thread() const noexcept {
        return native_io_thread()
#if defined(__linux__)
               || io_uring_thread()
#endif
            ;
    }

    [[nodiscard]] bool native_io_thread() const noexcept {
#if AF_DETAIL_HAS_EPOLL
        return kind_ == ThreadKind::Io || kind_ == ThreadKind::Epoll ||
               kind_ == ThreadKind::IoUring;
#elif AF_DETAIL_HAS_KQUEUE
        return kind_ == ThreadKind::Io || kind_ == ThreadKind::Kqueue;
#else
        return false;
#endif
    }

    [[nodiscard]] bool io_uring_thread() const noexcept {
        return kind_ == ThreadKind::IoUring;
    }

#if defined(__linux__)
    [[nodiscard]] bool provided_buffer_group_registered(std::uint16_t buffer_group) const noexcept;

    [[nodiscard]] IoUringPollSubmitResult
    try_submit_io_uring_poll_wait(int fd, std::uint32_t events, Task *task, IoResult *result,
                                  IoWaitRegistration *registration) noexcept {
        if (!io_uring_thread() || io_uring_fd_ < 0 || !io_uring_poll_add_available_) {
            return IoUringPollSubmitResult::Fallback;
        }

        const std::uint32_t native_events = native_poll_events(events);
        if (native_events == 0U) {
            result->fd = fd;
            result->events = io_error;
            result->error = EINVAL;
            return IoUringPollSubmitResult::Failed;
        }

        IoUringOperation *operation = nullptr;
        try {
            operation = io_uring_op_pool_.create();
        } catch (...) {
            result->fd = fd;
            result->events = io_error;
            result->error = ENOMEM;
            return IoUringPollSubmitResult::Failed;
        }

        int reserve_error = 0;
        io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
        if (sqe == nullptr) {
            io_uring_op_pool_.destroy(operation);
            result->fd = fd;
            result->events = io_error;
            result->error = reserve_error == 0 ? EBUSY : reserve_error;
            if (io_uring_fd_ < 0) {
                return IoUringPollSubmitResult::BackendClosed;
            }
            return IoUringPollSubmitResult::Fallback;
        }

        operation->task = task;
        operation->result = result;
        operation->prev = nullptr;
        operation->next = nullptr;
        operation->msg = nullptr;
        operation->socket_address = nullptr;
        operation->wait_registration = registration;
        operation->complete_events = 0;
        operation->direct_file_index = -1;
        operation->opcode = IORING_OP_POLL_ADD;
        operation->cancel_requested = false;
        operation->multishot = false;
        operation->poll_wait = true;
        operation->zero_copy_send = false;
        operation->zero_copy_primary_done = false;
        operation->zero_copy_notification_done = false;
        registration->poll_operation = operation;

        track_io_uring_operation(operation);

        *sqe = io_uring_sqe{};
        sqe->opcode = IORING_OP_POLL_ADD;
        sqe->fd = fd;
        sqe->user_data = reinterpret_cast<std::uint64_t>(operation);
        sqe->poll32_events = native_events;

        result->fd = fd;
        result->events = 0;
        result->error = 0;
        result->result = 0;
        result->completion_token = nullptr;

        if (io_uring_pending_submissions_ >= io_uring_submit_batch_threshold) {
            const int submit_error = flush_io_uring_submissions();
            if (submit_error == 0) {
                return IoUringPollSubmitResult::Submitted;
            }
            result->events = io_error;
            result->error = submit_error;
            result->result = -submit_error;
            fail_io_uring_backend(submit_error, operation);
            return IoUringPollSubmitResult::BackendClosed;
        }

        return IoUringPollSubmitResult::Submitted;
    }

#endif
#if defined(__linux__)
    [[nodiscard]] bool submit_io_uring_buffer_op(std::uint8_t opcode, int fd, void *data,
                                                 std::size_t size, std::uint64_t offset,
                                                 std::uint32_t op_flags,
                                                 std::uint32_t complete_events, Task *task,
                                                 IoResult *result) noexcept;
#endif
#if defined(__linux__)
    template <typename FillSqe>
    [[nodiscard]] bool submit_io_uring_fast_sqe(std::uint8_t opcode, int result_fd,
                                                std::uint32_t complete_events, Task *task,
                                                IoResult *result, FillSqe &&fill_sqe) noexcept;
#endif
#if defined(__linux__)
    [[nodiscard]] bool submit_io_uring_socket_impl(int domain, int type, int protocol,
                                                   std::uint32_t flags, Task *task,
                                                   IoResult *result) noexcept;
#endif
#if defined(__linux__)
    [[nodiscard]] bool
    submit_io_uring_fixed_file_rw(std::uint8_t opcode, int file_index, void *data, std::size_t size,
                                  std::uint64_t offset, std::uint32_t complete_events, Task *task,
                                  IoResult *result, std::uint16_t fixed_buffer_index = 0,
                                  bool fixed_buffer = false) noexcept;
#endif
#if defined(__linux__)
    struct IoUringGenericSubmitArgs {
        std::uint8_t opcode;
        int fd;
        void *data;
        std::size_t size;
        std::uint64_t offset;
        std::uint32_t op_flags;
        std::uint32_t complete_events;
        Task *task;
        IoResult *result;
        sockaddr *message_name;
        socklen_t message_name_len;
        socklen_t *message_name_len_out;
        const sockaddr *socket_address;
        socklen_t socket_address_size;
        sockaddr *socket_address_out;
        socklen_t *socket_address_size_out;
        const iovec *message_iov;
        std::size_t message_iov_count;
        std::uint64_t extra;
        std::int32_t extra_fd;
        std::uint16_t fixed_buffer_index;
        bool fixed_file;
        bool multishot;
        bool zero_copy_send;
        std::uint16_t provided_buffer_group;
        bool buffer_select;
        int direct_file_index;
    };

    struct IoUringGenericSubmitKind {
        bool openat_op;
        bool statx_op;
        bool renameat_op;
        bool unlinkat_op;
        bool path_fd_op;
        bool close_op;
        bool shutdown_op;
        bool fallocate_op;
        bool splice_op;
        bool fixed_buffer_op;
        bool message_op;
        bool accept_op;
        bool connect_op;
        bool address_op;
        bool message_iov_op;
        bool accept_address_op;
        bool needs_socket_address;
        bool data_optional_op;
    };

    static void set_io_uring_generic_submit_error(IoResult *result, int fd, int error) noexcept {
        if (result == nullptr) {
            return;
        }
        result->fd = fd;
        result->events = io_error;
        result->error = error;
    }

    [[nodiscard]] static IoUringGenericSubmitKind
    classify_io_uring_generic_submit(const IoUringGenericSubmitArgs &args) noexcept {
        const bool openat_op = args.opcode == IORING_OP_OPENAT;
        const bool statx_op = args.opcode == IORING_OP_STATX;
        const bool renameat_op = args.opcode == IORING_OP_RENAMEAT;
        const bool unlinkat_op = args.opcode == IORING_OP_UNLINKAT;
        const bool close_op = args.opcode == IORING_OP_CLOSE;
        const bool shutdown_op = args.opcode == IORING_OP_SHUTDOWN;
        const bool fallocate_op = args.opcode == IORING_OP_FALLOCATE;
        const bool splice_op = args.opcode == IORING_OP_SPLICE;
        const bool fixed_buffer_op =
            args.opcode == IORING_OP_READ_FIXED || args.opcode == IORING_OP_WRITE_FIXED;
        const bool message_op = args.opcode == IORING_OP_RECVMSG ||
                                args.opcode == IORING_OP_SENDMSG ||
                                args.opcode == detail::io_uring_op_sendmsg_zc;
        const bool accept_op = args.opcode == IORING_OP_ACCEPT;
        const bool connect_op = args.opcode == IORING_OP_CONNECT;
        const bool message_iov_op = message_op && args.message_iov != nullptr;
        const bool accept_address_op = accept_op && args.socket_address_out != nullptr &&
                                       args.socket_address_size_out != nullptr;
        return IoUringGenericSubmitKind{openat_op,
                                        statx_op,
                                        renameat_op,
                                        unlinkat_op,
                                        openat_op || statx_op || renameat_op || unlinkat_op,
                                        close_op,
                                        shutdown_op,
                                        fallocate_op,
                                        splice_op,
                                        fixed_buffer_op,
                                        message_op,
                                        accept_op,
                                        connect_op,
                                        accept_op || connect_op,
                                        message_iov_op,
                                        accept_address_op,
                                        connect_op || accept_address_op,
                                        args.opcode == IORING_OP_FSYNC || close_op || shutdown_op ||
                                            fallocate_op || splice_op};
    }
#endif
#if defined(__linux__)
    [[nodiscard]] bool
    validate_io_uring_generic_submit(const IoUringGenericSubmitArgs &args,
                                     const IoUringGenericSubmitKind &kind) const noexcept;

    [[nodiscard]] bool
    validate_io_uring_generic_fixed_resources(const IoUringGenericSubmitArgs &args,
                                              const IoUringGenericSubmitKind &kind) const noexcept;
#endif
#if defined(__linux__)
    [[nodiscard]] IoUringOperation *
    create_io_uring_generic_submit_operation(const IoUringGenericSubmitArgs &args,
                                             const IoUringGenericSubmitKind &kind) noexcept {
        IoUringOperation *operation = nullptr;
        try {
            operation = io_uring_op_pool_.create();
        } catch (...) {
            set_io_uring_generic_submit_error(args.result, args.fd, ENOMEM);
            return nullptr;
        }

        initialize_io_uring_generic_submit_operation(args, operation);

        if (kind.message_op && !attach_io_uring_generic_submit_message(args, kind, operation)) {
            return nullptr;
        }
        if (kind.needs_socket_address &&
            !attach_io_uring_generic_submit_socket_address(args, kind, operation)) {
            return nullptr;
        }
        return operation;
    }

    static void initialize_io_uring_generic_submit_operation(const IoUringGenericSubmitArgs &args,
                                                             IoUringOperation *operation) noexcept {
        operation->task = args.task;
        operation->result = args.result;
        operation->complete_events = args.complete_events;
        operation->direct_file_index = args.direct_file_index;
        operation->opcode = args.opcode;
        operation->cancel_requested = false;
        operation->multishot = args.multishot;
        operation->poll_wait = false;
        operation->zero_copy_send = args.zero_copy_send;
        operation->zero_copy_primary_done = false;
        operation->zero_copy_notification_done = false;
        operation->msg = nullptr;
        operation->socket_address = nullptr;
        operation->wait_registration = nullptr;
    }

    [[nodiscard]] bool attach_io_uring_generic_submit_message(const IoUringGenericSubmitArgs &args,
                                                              const IoUringGenericSubmitKind &kind,
                                                              IoUringOperation *operation) noexcept;

    [[nodiscard]] bool
    attach_io_uring_generic_submit_socket_address(const IoUringGenericSubmitArgs &args,
                                                  const IoUringGenericSubmitKind &kind,
                                                  IoUringOperation *operation) noexcept;
#endif
#if defined(__linux__)
    static void fill_io_uring_generic_submit_sqe(io_uring_sqe &sqe,
                                                 const IoUringGenericSubmitArgs &args,
                                                 const IoUringGenericSubmitKind &kind,
                                                 IoUringOperation *operation) noexcept;

    static void initialize_io_uring_generic_submit_sqe(io_uring_sqe &sqe,
                                                       const IoUringGenericSubmitArgs &args,
                                                       IoUringOperation *operation) noexcept;
#endif
#if defined(__linux__)
    static void fill_io_uring_generic_fallocate_sqe(io_uring_sqe &sqe,
                                                    const IoUringGenericSubmitArgs &args) noexcept;

    static void fill_io_uring_generic_splice_sqe(io_uring_sqe &sqe,
                                                 const IoUringGenericSubmitArgs &args) noexcept;

    static void fill_io_uring_generic_openat_sqe(io_uring_sqe &sqe,
                                                 const IoUringGenericSubmitArgs &args) noexcept;

    static void fill_io_uring_generic_statx_sqe(io_uring_sqe &sqe,
                                                const IoUringGenericSubmitArgs &args) noexcept;

    static void fill_io_uring_generic_renameat_sqe(io_uring_sqe &sqe,
                                                   const IoUringGenericSubmitArgs &args) noexcept;

    static void fill_io_uring_generic_unlinkat_sqe(io_uring_sqe &sqe,
                                                   const IoUringGenericSubmitArgs &args) noexcept;
#endif
#if defined(__linux__)
    static void fill_io_uring_generic_message_sqe(io_uring_sqe &sqe,
                                                  const IoUringGenericSubmitArgs &args,
                                                  IoUringOperation *operation) noexcept;

    static void fill_io_uring_generic_accept_sqe(io_uring_sqe &sqe,
                                                 const IoUringGenericSubmitArgs &args,
                                                 IoUringOperation *operation) noexcept;

    static void fill_io_uring_generic_connect_sqe(io_uring_sqe &sqe,
                                                  IoUringOperation *operation) noexcept;

    static void
    fill_io_uring_generic_socket_data_sqe(io_uring_sqe &sqe,
                                          const IoUringGenericSubmitArgs &args) noexcept;
#endif
#if defined(__linux__)
    static void
    fill_io_uring_generic_fixed_buffer_sqe(io_uring_sqe &sqe,
                                           const IoUringGenericSubmitArgs &args) noexcept;

    static void fill_io_uring_generic_buffer_sqe(io_uring_sqe &sqe,
                                                 const IoUringGenericSubmitArgs &args) noexcept;
#endif
#if defined(__linux__)
    [[nodiscard]] bool submit_io_uring_op(
        std::uint8_t opcode, int fd, void *data, std::size_t size, std::uint64_t offset,
        std::uint32_t op_flags, std::uint32_t complete_events, Task *task, IoResult *result,
        sockaddr *message_name = nullptr, socklen_t message_name_len = 0,
        socklen_t *message_name_len_out = nullptr, const sockaddr *socket_address = nullptr,
        socklen_t socket_address_size = 0, sockaddr *socket_address_out = nullptr,
        socklen_t *socket_address_size_out = nullptr, const iovec *message_iov = nullptr,
        std::size_t message_iov_count = 0, std::uint64_t extra = 0, std::int32_t extra_fd = -1,
        std::uint16_t fixed_buffer_index = 0, bool fixed_file = false, bool multishot = false,
        bool zero_copy_send = false, std::uint16_t provided_buffer_group = 0,
        bool buffer_select = false, int direct_file_index = -1) noexcept;
#endif

#if AF_DETAIL_HAS_EPOLL
    [[nodiscard]] bool native_io_backend_available() const noexcept;
    [[nodiscard]] bool notify_native_io_backend() noexcept;
    [[nodiscard]] bool init_native_io_backend() noexcept;
    void close_native_io_backend() noexcept;
    void clear_io_waits() noexcept;
    [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept;
    [[nodiscard]] bool register_native_io_wait(int fd, std::uint32_t events, Task *task,
                                               IoResult *result, bool prefer_rearm) noexcept;
    [[nodiscard]] bool cancel_native_io_wait(IoOpState &state) noexcept;
#elif AF_DETAIL_HAS_KQUEUE
#if AF_DETAIL_HAS_KQUEUE
    static constexpr uintptr_t kqueue_wake_ident = 1;

    [[nodiscard]] bool native_io_backend_available() const noexcept {
        return io_kqueue_fd_ >= 0;
    }

    [[nodiscard]] bool notify_native_io_backend() noexcept {
        if (io_kqueue_fd_ < 0) {
            return false;
        }
        bool expected = false;
        if (!io_wake_pending_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                      std::memory_order_acquire)) {
            return true;
        }

        struct kevent event{};
        EV_SET(&event, kqueue_wake_ident, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
        if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
            io_wake_pending_.store(false, std::memory_order_release);
            return false;
        }
        return true;
    }

    [[nodiscard]] bool init_native_io_backend() noexcept {
        if (!native_io_thread()) {
            return false;
        }
        if (io_kqueue_fd_ >= 0) {
            return true;
        }
        reserve_native_io_wait_storage();

        io_kqueue_fd_ = ::kqueue();
        if (io_kqueue_fd_ < 0) {
            return false;
        }

        struct kevent event{};
        EV_SET(&event, kqueue_wake_ident, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
        if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
            close_native_io_backend();
            return false;
        }
        return true;
    }

    void close_native_io_backend() noexcept {
        clear_io_waits();
        clear_kqueue_timeouts();
        if (io_kqueue_fd_ >= 0) {
            ::close(io_kqueue_fd_);
            io_kqueue_fd_ = -1;
        }
        io_wake_pending_.store(false, std::memory_order_relaxed);
    }
    [[nodiscard]] static intptr_t kqueue_timeout_data(std::chrono::nanoseconds timeout) noexcept {
#if defined(NOTE_NSECONDS)
        return clamp_kqueue_timer_value(timeout.count());
#elif defined(NOTE_USECONDS)
        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
            timeout + std::chrono::nanoseconds{999});
        return clamp_kqueue_timer_value(us.count());
#else
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            timeout + std::chrono::nanoseconds{999999});
        return clamp_kqueue_timer_value(ms.count() == 0 ? 1 : ms.count());
#endif
    }

    [[nodiscard]] static std::uint32_t kqueue_timeout_unit_flags() noexcept {
#if defined(NOTE_NSECONDS)
        return NOTE_NSECONDS;
#elif defined(NOTE_USECONDS)
        return NOTE_USECONDS;
#else
        return 0;
#endif
    }

    [[nodiscard]] static intptr_t clamp_kqueue_timer_value(std::int64_t value) noexcept {
        constexpr auto max_value = static_cast<std::uint64_t>(std::numeric_limits<intptr_t>::max());
        if (value <= 0) {
            return 1;
        }
        const auto unsigned_value = static_cast<std::uint64_t>(value);
        if (unsigned_value > max_value) {
            return std::numeric_limits<intptr_t>::max();
        }
        return static_cast<intptr_t>(value);
    }
    void clear_kqueue_timeouts() noexcept {
        KqueueTimeoutRegistration *registration = io_kqueue_timeouts_;
        while (registration != nullptr) {
            KqueueTimeoutRegistration *next = registration->next;
            if (registration->result != nullptr &&
                registration->result->completion_token == registration) {
                registration->result->completion_token = nullptr;
            }
            io_kqueue_timeout_pool_.destroy(registration);
            registration = next;
        }
        io_kqueue_timeouts_ = nullptr;
        io_kqueue_timeout_count_ = 0;
    }

    void track_kqueue_timeout(KqueueTimeoutRegistration *registration) noexcept {
        registration->prev = nullptr;
        registration->next = io_kqueue_timeouts_;
        if (io_kqueue_timeouts_ != nullptr) {
            io_kqueue_timeouts_->prev = registration;
        }
        io_kqueue_timeouts_ = registration;
        ++io_kqueue_timeout_count_;
    }

    void untrack_kqueue_timeout(KqueueTimeoutRegistration *registration) noexcept {
        if (registration->prev != nullptr) {
            registration->prev->next = registration->next;
        } else if (io_kqueue_timeouts_ == registration) {
            io_kqueue_timeouts_ = registration->next;
        }
        if (registration->next != nullptr) {
            registration->next->prev = registration->prev;
        }
        registration->prev = nullptr;
        registration->next = nullptr;
        if (io_kqueue_timeout_count_ != 0U) {
            --io_kqueue_timeout_count_;
        }
    }

    [[nodiscard]] uintptr_t next_kqueue_timeout_ident() noexcept {
        uintptr_t ident = io_kqueue_next_timeout_ident_++;
        if (ident <= kqueue_wake_ident) {
            ident = kqueue_wake_ident + 1U;
            io_kqueue_next_timeout_ident_ = ident + 1U;
        }
        return ident;
    }
    [[nodiscard]] bool submit_kqueue_timeout(std::chrono::nanoseconds timeout, Task *task,
                                             IoResult *result) noexcept {
        AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
                  "kqueue timeout submit must run on its IO thread");
        if (result != nullptr) {
            result->completion_token = nullptr;
        }
        if (RuntimeT::current_thread_index_ != index_ || task == nullptr || result == nullptr ||
            timeout.count() <= 0) {
            if (result != nullptr) {
                result->fd = -1;
                result->events = io_error;
                result->error = EINVAL;
                result->result = -EINVAL;
            }
            return false;
        }
        if (io_kqueue_fd_ < 0) {
            result->fd = -1;
            result->events = io_error;
            result->error = ENOSYS;
            result->result = -ENOSYS;
            return false;
        }

        KqueueTimeoutRegistration *registration = nullptr;
        try {
            registration = io_kqueue_timeout_pool_.create();
        } catch (...) {
            result->fd = -1;
            result->events = io_error;
            result->error = ENOMEM;
            result->result = -ENOMEM;
            return false;
        }

        registration->task = task;
        registration->result = result;
        registration->prev = nullptr;
        registration->next = nullptr;
        registration->ident = next_kqueue_timeout_ident();

        struct kevent event{};
        EV_SET(&event, registration->ident, EVFILT_TIMER, EV_ADD | EV_ONESHOT,
               kqueue_timeout_unit_flags(), kqueue_timeout_data(timeout), registration);
        if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0) {
            const int error = errno == 0 ? EIO : errno;
            io_kqueue_timeout_pool_.destroy(registration);
            result->fd = -1;
            result->events = io_error;
            result->error = error;
            result->result = -error;
            return false;
        }

        track_kqueue_timeout(registration);
        result->fd = -1;
        result->events = 0;
        result->error = 0;
        result->result = 0;
        result->completion_token = registration;
        return true;
    }
    [[nodiscard]] bool cancel_kqueue_timeout(IoOpState &state) noexcept {
        if (io_kqueue_fd_ < 0) {
            state.wait.events = io_error;
            state.wait.error = ENOSYS;
            state.wait.result = -ENOSYS;
            return false;
        }

        auto *registration = static_cast<KqueueTimeoutRegistration *>(state.wait.completion_token);
        if (registration == nullptr || registration->result != &state.wait) {
            state.wait.events = io_error;
            state.wait.error = ENOENT;
            state.wait.result = -ENOENT;
            return false;
        }

        struct kevent event{};
        EV_SET(&event, registration->ident, EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
        if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0 && errno != ENOENT) {
            const int error = errno == 0 ? EIO : errno;
            state.wait.events = io_error;
            state.wait.error = error;
            state.wait.result = -error;
            return false;
        }

        untrack_kqueue_timeout(registration);
        state.wait.fd = -1;
        state.wait.events = io_error;
        state.wait.error = ECANCELED;
        state.wait.result = -ECANCELED;
        state.wait.completion_token = nullptr;
        if (registration->task != running_task_) {
            enqueue_pending_blocking(index_, registration->task);
        }
        io_kqueue_timeout_pool_.destroy(registration);
        return true;
    }

    [[nodiscard]] bool complete_kqueue_timeout(KqueueTimeoutRegistration *registration,
                                               const struct kevent &event) noexcept {
        if (registration == nullptr || registration->result == nullptr) {
            return false;
        }

        IoResult *result = registration->result;
        if (result->completion_token != registration) {
            return false;
        }

        untrack_kqueue_timeout(registration);
        result->fd = -1;
        result->events = io_error;
        result->error = io_error_from_kqueue(event);
        if (result->error == 0) {
            result->error = ETIMEDOUT;
        }
        result->result = -result->error;
        result->completion_token = nullptr;
        enqueue_pending_blocking(index_, registration->task);
        io_kqueue_timeout_pool_.destroy(registration);
        return true;
    }
    [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept {
        if (io_kqueue_fd_ < 0) {
            return did_work;
        }
        if (timeout_ms == 0 && io_waits_.empty() && io_kqueue_timeout_count_ == 0U &&
            !io_wake_pending_.load(std::memory_order_acquire)) {
            return did_work;
        }

        timespec timeout{};
        timespec *timeout_ptr = nullptr;
        if (timeout_ms >= 0) {
            timeout.tv_sec = timeout_ms / 1000;
            timeout.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1000000L;
            timeout_ptr = &timeout;
        }

        std::array<struct kevent, 64> events;
        const int count = ::kevent(io_kqueue_fd_, nullptr, 0, events.data(),
                                   static_cast<int>(events.size()), timeout_ptr);
        if (count <= 0) {
            return did_work;
        }

        std::array<IoWaitRegistration *, 64> completed{};
        std::size_t completed_count = 0;
        for (int i = 0; i < count; ++i) {
            const struct kevent &event = events[static_cast<std::size_t>(i)];
            if (event.filter == EVFILT_USER) {
                io_wake_pending_.store(false, std::memory_order_release);
                did_work = true;
                continue;
            }
            if (event.filter == EVFILT_TIMER) {
                auto *timeout = static_cast<KqueueTimeoutRegistration *>(event.udata);
                if (complete_kqueue_timeout(timeout, event)) {
                    did_work = true;
                }
                continue;
            }

            auto *registration = static_cast<IoWaitRegistration *>(event.udata);
            if (registration == nullptr) {
                continue;
            }

            const int fd = registration->fd;
            auto it = io_waits_.find(fd);
            if (it == io_waits_.end() || it->second != registration) {
                continue;
            }

            remove_kqueue_filters(*registration);
            io_waits_.erase(it);

            registration->result->fd = fd;
            registration->result->events = io_events_from_kqueue(event);
            registration->result->error = io_error_from_kqueue(event);
            enqueue_pending_blocking(index_, registration->task);
            completed[completed_count++] = registration;
            did_work = true;
        }

        for (std::size_t i = 0; i < completed_count; ++i) {
            io_wait_pool_.destroy(completed[i]);
        }
        return did_work;
    }
    void clear_io_waits() noexcept {
        for (auto &entry : io_waits_) {
            io_wait_pool_.destroy(entry.second);
        }
        io_waits_.clear();
    }

    void reserve_native_io_wait_storage() noexcept {
        try {
            if constexpr (io_wait_reserve != 0U) {
                io_waits_.reserve(io_wait_reserve);
                io_wait_pool_.reserve_slots(io_wait_reserve);
                io_kqueue_timeout_pool_.reserve_slots(io_wait_reserve);
            }
        } catch (...) {
        }
    }
    [[nodiscard]] bool register_native_io_wait(int fd, std::uint32_t events, Task *task,
                                               IoResult *result, bool prefer_rearm) noexcept {
        static_cast<void>(prefer_rearm);
        const bool unsupported_events = (events & (io_readable | io_writable)) == 0U;
        if (io_kqueue_fd_ < 0 || fd < 0 || events == 0U || unsupported_events ||
            io_waits_.find(fd) != io_waits_.end()) {
            result->fd = fd;
            result->events = io_error;
            if (fd < 0) {
                result->error = EBADF;
            } else if (events == 0U || unsupported_events) {
                result->error = EINVAL;
            } else if (io_kqueue_fd_ < 0) {
                result->error = ENOSYS;
            } else {
                result->error = EALREADY;
            }
            return false;
        }

        IoWaitRegistration *registration = nullptr;
        try {
            registration = io_wait_pool_.create();
            auto [it, inserted] = io_waits_.emplace(fd, registration);
            static_cast<void>(it);
            if (!inserted) {
                io_wait_pool_.destroy(registration);
                result->fd = fd;
                result->events = io_error;
                result->error = EALREADY;
                return false;
            }
        } catch (...) {
            if (registration != nullptr) {
                io_wait_pool_.destroy(registration);
            }
            result->fd = fd;
            result->events = io_error;
            result->error = ENOMEM;
            return false;
        }

        registration->fd = fd;
        registration->events = events;
        registration->task = task;
        registration->result = result;

        std::array<struct kevent, 2> changes;
        int change_count = fill_kqueue_changes(fd, events, registration, changes);
        if (::kevent(io_kqueue_fd_, changes.data(), change_count, nullptr, 0, nullptr) != 0) {
            const int error = errno == 0 ? EIO : errno;
            io_waits_.erase(fd);
            io_wait_pool_.destroy(registration);
            result->fd = fd;
            result->events = io_error;
            result->error = error;
            return false;
        }

        *result = IoResult{fd, 0, 0};
        return true;
    }

    [[nodiscard]] bool cancel_native_io_wait(IoOpState &state) noexcept {
        if (io_kqueue_fd_ < 0) {
            state.wait.events = io_error;
            state.wait.error = ENOSYS;
            state.wait.result = -ENOSYS;
            return false;
        }

        const int fd = state.wait.fd;
        auto it = io_waits_.find(fd);
        if (fd < 0 || it == io_waits_.end() || it->second->result != &state.wait) {
            state.wait.events = io_error;
            state.wait.error = ENOENT;
            state.wait.result = -ENOENT;
            return false;
        }

        IoWaitRegistration *registration = it->second;
        remove_kqueue_filters(*registration);
        io_waits_.erase(it);

        state.wait.fd = fd;
        state.wait.events = io_error;
        state.wait.error = ECANCELED;
        state.wait.result = -ECANCELED;
        if (registration->task != running_task_) {
            enqueue_pending_blocking(index_, registration->task);
        }
        io_wait_pool_.destroy(registration);
        return true;
    }

    [[nodiscard]] static int fill_kqueue_changes(int fd, std::uint32_t events,
                                                 IoWaitRegistration *registration,
                                                 std::array<struct kevent, 2> &changes) noexcept {
        int count = 0;
        if ((events & io_readable) != 0U) {
            EV_SET(&changes[static_cast<std::size_t>(count++)], static_cast<uintptr_t>(fd),
                   EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0, registration);
        }
        if ((events & io_writable) != 0U) {
            EV_SET(&changes[static_cast<std::size_t>(count++)], static_cast<uintptr_t>(fd),
                   EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0, 0, registration);
        }
        return count;
    }

    void remove_kqueue_filters(const IoWaitRegistration &registration) noexcept {
        std::array<struct kevent, 2> changes;
        int count = 0;
        if ((registration.events & io_readable) != 0U) {
            EV_SET(&changes[static_cast<std::size_t>(count++)],
                   static_cast<uintptr_t>(registration.fd), EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        }
        if ((registration.events & io_writable) != 0U) {
            EV_SET(&changes[static_cast<std::size_t>(count++)],
                   static_cast<uintptr_t>(registration.fd), EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        }
        if (count != 0) {
            static_cast<void>(::kevent(io_kqueue_fd_, changes.data(), count, nullptr, 0, nullptr));
        }
    }
    [[nodiscard]] static std::uint32_t io_events_from_kqueue(const struct kevent &event) noexcept {
        if ((event.flags & EV_ERROR) != 0) {
            return io_error;
        }

        std::uint32_t result = 0;
        if (event.filter == EVFILT_READ) {
            result |= io_readable;
        } else if (event.filter == EVFILT_WRITE) {
            result |= io_writable;
        }
        if ((event.flags & EV_EOF) != 0) {
            result |= io_hangup;
        }
        return result;
    }

    [[nodiscard]] static int io_error_from_kqueue(const struct kevent &event) noexcept {
        if ((event.flags & EV_ERROR) == 0) {
            return 0;
        }
        return event.data == 0 ? EIO : static_cast<int>(event.data);
    }
#endif
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
                                               IoResult *result, bool prefer_rearm) noexcept {
        static_cast<void>(events);
        static_cast<void>(task);
        static_cast<void>(prefer_rearm);
        result->fd = fd;
        result->events = io_error;
        result->error = fd < 0 ? EBADF : ENOSYS;
        return false;
    }
#endif

    void notify_force() noexcept;
    void set_current_thread_name() noexcept;
    void run_loop() noexcept;
    void init_io_backend() noexcept;
    void close_io_backend() noexcept;
    [[nodiscard]] bool poll_io(int timeout_ms) noexcept;
#if defined(__linux__)
    void detect_io_uring_features() noexcept;
#endif
#if defined(__linux__)
    void close_io_uring_backend() noexcept;
#endif
#if defined(__linux__)
    template <typename T>
    [[nodiscard]] static T *ptr_at(std::byte *base, std::uint32_t offset) noexcept;
    [[nodiscard]] static unsigned io_uring_requested_setup_flags() noexcept;
    [[nodiscard]] bool map_io_uring_rings(const io_uring_params &params) noexcept;
    void bind_io_uring_ring_pointers(const io_uring_params &params) noexcept;
    [[nodiscard]] bool register_io_uring_wake_fd() noexcept;
#endif
#if defined(__linux__)
    void init_io_uring_backend() noexcept;
#endif
#if defined(__linux__)
    void reserve_io_backend_storage() noexcept;
#endif
#if defined(__linux__)
    [[nodiscard]] io_uring_sqe *reserve_io_uring_sqe(int &error) noexcept;
    [[nodiscard]] int flush_io_uring_submissions() noexcept;
    [[nodiscard]] bool flush_io_uring_submissions_or_fail() noexcept;
#endif
#if defined(__linux__)
    [[nodiscard]] bool poll_io_uring_completions() noexcept;
#endif
#if defined(__linux__)
    [[nodiscard]] bool complete_io_uring_operation(IoUringOperation *operation, int result,
                                                   std::uint32_t cqe_flags) noexcept;
#endif
#if defined(__linux__)
    void complete_io_uring_poll_wait(IoUringOperation *operation, int result) noexcept;
#endif
#if defined(__linux__)
    [[nodiscard]] static bool io_uring_result_is_fd(std::uint8_t opcode) noexcept;
    [[nodiscard]] static bool
    io_uring_operation_result_is_fd(const IoUringOperation *operation) noexcept;
    void clear_direct_io_uring_file_slot(const IoUringOperation *operation) noexcept;
    [[nodiscard]] int submit_io_uring_cancel(IoUringOperation *operation) noexcept;
#endif
#if defined(__linux__)
    void track_io_uring_operation(IoUringOperation *operation) noexcept;
    void untrack_io_uring_operation(IoUringOperation *operation) noexcept;
    void clear_io_uring_operations() noexcept;
    void fail_io_uring_backend(int error, IoUringOperation *running_operation) noexcept;
    void clear_or_fail_io_uring_operations(int error, IoUringOperation *running_operation) noexcept;
    void close_pending_io_uring_fd_result(IoUringOperation *operation) noexcept;
    static void clear_io_uring_result_token(IoUringOperation *operation) noexcept;
    void destroy_io_uring_operation(IoUringOperation *operation) noexcept;
#endif

#if defined(__linux__)
    void drain_io_wake() noexcept {
        std::uint64_t value = 0;
        while (::read(io_wake_fd_, &value, sizeof(value)) == sizeof(value)) {
        }
        io_wake_pending_.store(false, std::memory_order_release);
    }

    [[nodiscard]] static std::uint32_t native_poll_events(std::uint32_t events) noexcept {
        std::uint32_t result = POLLERR | POLLHUP;
        if ((events & io_readable) != 0U) {
            result |= POLLIN;
        }
        if ((events & io_writable) != 0U) {
            result |= POLLOUT;
        }
        return result;
    }

    [[nodiscard]] static std::uint32_t io_events_from_poll(std::uint32_t events) noexcept {
        std::uint32_t result = 0;
        if ((events & (POLLIN | POLLPRI)) != 0U) {
            result |= io_readable;
        }
        if ((events & POLLOUT) != 0U) {
            result |= io_writable;
        }
        if ((events & (POLLERR | POLLNVAL)) != 0U) {
            result |= io_error;
        }
        if ((events & POLLHUP) != 0U) {
            result |= io_hangup;
        }
#ifdef POLLRDHUP
        if ((events & POLLRDHUP) != 0U) {
            result |= io_hangup;
        }
#endif
        return result;
    }

    [[nodiscard]] static std::uint32_t io_events_from_native(std::uint32_t events) noexcept {
        std::uint32_t result = 0;
        if ((events & EPOLLIN) != 0U) {
            result |= io_readable;
        }
        if ((events & EPOLLOUT) != 0U) {
            result |= io_writable;
        }
        if ((events & EPOLLERR) != 0U) {
            result |= io_error;
        }
        if ((events & EPOLLHUP) != 0U) {
            result |= io_hangup;
        }
        return result;
    }
#endif

    void advance_ready_word_cursor_after(std::size_t word) noexcept;
    Task *pop_one() noexcept;
    void finish_done(Task *task) noexcept;
    void finish_pending(Task *task) noexcept;
    void finish_again(Task *task) noexcept;
    std::uint16_t index_;
    ThreadKind kind_{ThreadKind::Worker};
    std::uint16_t next_source_{0};
    std::uint16_t next_ready_word_{0};
    std::vector<Task *> local_queue_;
    std::size_t local_capacity_{0};
    std::size_t local_mask_{0};
    std::size_t local_head_{0};
    std::size_t local_tail_{0};
    std::size_t local_size_{0};
    ExternalQueue *external_queue_{nullptr};
    detail::ReadySourceSet<thread_count> ready_sources_;
    CacheLineAtomic<bool> external_ready_{false};
    CacheLineAtomic<std::uint32_t> wake_epoch_{0};
    CacheLineAtomic<bool> sleeping_{false};
    CacheLineAtomic<bool> stop_requested_{false};
    Task *running_task_{nullptr};
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
    absl::flat_hash_map<int, IoWaitRegistration *> io_waits_;
    IoObjectPool<IoWaitRegistration> io_wait_pool_;
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
#if defined(__linux__)
    int io_uring_fd_{-1};
    int io_uring_backend_error_{0};
    std::byte *io_uring_sq_ring_{nullptr};
    std::byte *io_uring_cq_ring_{nullptr};
    io_uring_sqe *io_uring_sqes_{nullptr};
    std::size_t io_uring_sq_ring_size_{0};
    std::size_t io_uring_cq_ring_size_{0};
    std::size_t io_uring_sqes_size_{0};
    std::uint32_t *io_uring_sq_head_{nullptr};
    std::uint32_t *io_uring_sq_tail_{nullptr};
    std::uint32_t *io_uring_sq_ring_mask_{nullptr};
    std::uint32_t *io_uring_sq_ring_entries_{nullptr};
    std::uint32_t *io_uring_sq_array_{nullptr};
    std::uint32_t *io_uring_cq_head_{nullptr};
    std::uint32_t *io_uring_cq_tail_{nullptr};
    std::uint32_t *io_uring_cq_ring_mask_{nullptr};
    io_uring_cqe *io_uring_cqes_{nullptr};
    std::uint32_t io_uring_sq_cached_head_{0};
    std::uint32_t io_uring_sq_cached_tail_{0};
    std::uint32_t io_uring_sq_ring_mask_value_{0};
    std::uint32_t io_uring_sq_ring_entries_value_{0};
    std::uint32_t io_uring_cq_ring_mask_value_{0};
    unsigned io_uring_pending_submissions_{0};
    bool io_uring_send_zc_available_{false};
    bool io_uring_sendmsg_zc_available_{false};
    bool io_uring_poll_add_available_{false};
    bool io_uring_socket_available_{false};
    bool io_uring_buffers_registered_{false};
    unsigned io_uring_registered_buffer_count_{0};
    std::vector<std::uint16_t> io_uring_provided_buffer_groups_;
    bool io_uring_files_registered_{false};
    unsigned io_uring_registered_file_count_{0};
    IoUringOperation *io_uring_operations_{nullptr};
    IoObjectPool<detail::IoUringMessage> io_uring_msg_pool_;
    IoObjectPool<detail::IoUringSocketAddress> io_uring_address_pool_;
    IoObjectPool<IoUringOperation> io_uring_op_pool_;
#endif
#if AF_DETAIL_HAS_EPOLL || AF_DETAIL_HAS_KQUEUE
    CacheLineAtomic<bool> io_wake_pending_{false};
#endif
    std::thread worker_;
};

} // namespace af::detail

#include "af/detail/runtime/runtime_executor_epoll_backend.hpp"
#include "af/detail/runtime/runtime_executor_io_backend.hpp"
#include "af/detail/runtime/runtime_executor_io_resources.hpp"
#include "af/detail/runtime/runtime_executor_io_submit_core.hpp"
#include "af/detail/runtime/runtime_executor_lifecycle.hpp"
#include "af/detail/runtime/runtime_executor_scheduler.hpp"
