#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
class alignas(hardware_cache_line_size) Executor {
  using Thread = typename TraitsT::Thread;
  using Task = BasicTask<RuntimeT>;
  using RuntimeStatus = detail::RuntimeStatus;
  template <typename T> using CacheLineAtomic = detail::CacheLineAtomic<T>;

  static constexpr std::uint16_t thread_count = RuntimeT::thread_count;
  static constexpr std::uint16_t invalid_thread_index =
      RuntimeT::invalid_thread_index;
  static constexpr std::size_t spsc_queue_capacity =
      RuntimeT::spsc_queue_capacity;
  static constexpr std::size_t io_wait_reserve = RuntimeT::io_wait_reserve;
  static constexpr unsigned io_uring_entries = RuntimeT::io_uring_entries;
  static constexpr unsigned io_uring_submit_batch_threshold =
      RuntimeT::io_uring_submit_batch_threshold;
  static constexpr unsigned io_uring_cq_entries = RuntimeT::io_uring_cq_entries;
  static constexpr unsigned io_uring_setup_flags =
      RuntimeT::io_uring_setup_flags;
  static constexpr bool io_uring_setup_sqpoll = RuntimeT::io_uring_setup_sqpoll;
  static constexpr unsigned io_uring_sqpoll_idle_ms =
      RuntimeT::io_uring_sqpoll_idle_ms;
  static constexpr int io_uring_sqpoll_cpu = RuntimeT::io_uring_sqpoll_cpu;
  static constexpr bool io_uring_setup_submit_all =
      RuntimeT::io_uring_setup_submit_all;
  static constexpr bool io_uring_setup_coop_taskrun =
      RuntimeT::io_uring_setup_coop_taskrun;
  static constexpr bool io_uring_setup_single_issuer =
      RuntimeT::io_uring_setup_single_issuer;
  static constexpr bool io_uring_setup_defer_taskrun =
      RuntimeT::io_uring_setup_defer_taskrun;
  static constexpr std::size_t io_uring_provided_buffer_group_reserve =
      RuntimeT::io_uring_provided_buffer_group_reserve;

  [[nodiscard]] static constexpr Thread
  thread_from_index(std::uint16_t index) noexcept {
    return RuntimeT::thread_from_index(index);
  }

  [[nodiscard]] static constexpr ThreadKind
  thread_kind(Thread thread) noexcept {
    return RuntimeT::thread_kind(thread);
  }

  [[nodiscard]] static auto &spsc_queue(std::uint16_t source,
                                        std::uint16_t target) noexcept {
    return RuntimeT::spsc_queue(source, target);
  }

  static void on_task_finished(Task *task) noexcept {
    RuntimeT::on_task_finished(task);
  }

  static void enqueue_pending_blocking(std::uint16_t index,
                                       Task *task) noexcept {
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
  explicit Executor(std::uint16_t index)
      : index_(index), kind_(thread_kind(thread_from_index(index))),
        local_queue_(detail::next_power_of_two(
            spsc_queue_capacity < 2 ? 2 : spsc_queue_capacity)) {}
  Executor(const Executor &) = delete;
  Executor &operator=(const Executor &) = delete;

  ~Executor() { close_io_backend(); }

  void start() {
    init_io_backend();
    worker_ = std::thread([this] { run_loop(); });
  }

  void request_stop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
    notify_force();
  }

  void join() {
    if (worker_.joinable()) {
      worker_.join();
    }
  }
  void notify() noexcept {
    wake_epoch_.fetch_add(1, std::memory_order_release);
    if (!io_thread() || !io_backend_available()) {
      wake_epoch_.notify_one();
      return;
    }

    if (!sleeping_.load(std::memory_order_acquire)) {
      return;
    }

    bool expected = true;
    if (sleeping_.compare_exchange_strong(expected, false,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
      if (notify_native_io_backend()) {
        return;
      }
      wake_epoch_.notify_one();
    }
  }

  [[nodiscard]] bool io_backend_available() const noexcept {
    return native_io_backend_available();
  }

  [[nodiscard]] bool io_uring_backend_available() const noexcept {
#if defined(__linux__)
    return io_uring_fd_ >= 0;
#else
    return false;
#endif
  }

  [[nodiscard]] int io_uring_backend_error() const noexcept {
#if defined(__linux__)
    return io_uring_fd_ >= 0
               ? 0
               : (io_uring_backend_error_ == 0 ? ENODEV
                                               : io_uring_backend_error_);
#else
    return ENOSYS;
#endif
  }

  [[nodiscard]] bool io_uring_poll_available() const noexcept {
#if defined(__linux__)
    return io_uring_fd_ >= 0 && io_uring_poll_add_available_;
#else
    return false;
#endif
  }

#if !defined(_WIN32)
  [[nodiscard]] bool register_io_uring_buffers(const iovec *buffers,
                                               unsigned buffer_count,
                                               int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring buffer registration must run on its IO thread");
    if (error != nullptr) {
      *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_ || buffers == nullptr ||
        buffer_count == 0U) {
      if (error != nullptr) {
        *error = EINVAL;
      }
      return false;
    }

#if defined(__linux__)
    if (io_uring_fd_ < 0) {
      if (error != nullptr) {
        *error = ENOSYS;
      }
      return false;
    }
    if (io_uring_buffers_registered_) {
      if (error != nullptr) {
        *error = EALREADY;
      }
      return false;
    }
    if (buffer_count >
        static_cast<unsigned>(std::numeric_limits<std::uint16_t>::max())) {
      if (error != nullptr) {
        *error = EINVAL;
      }
      return false;
    }

    if (detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_BUFFERS,
                                      buffers, buffer_count) != 0) {
      if (error != nullptr) {
        *error = errno == 0 ? EIO : errno;
      }
      return false;
    }

    io_uring_buffers_registered_ = true;
    io_uring_registered_buffer_count_ = buffer_count;
    return true;
#else
    if (error != nullptr) {
      *error = ENOSYS;
    }
    return false;
#endif
  }
  [[nodiscard]] bool unregister_io_uring_buffers(int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring buffer unregistration must run on its IO thread");
    if (error != nullptr) {
      *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_) {
      if (error != nullptr) {
        *error = EINVAL;
      }
      return false;
    }

#if defined(__linux__)
    if (io_uring_fd_ < 0) {
      if (error != nullptr) {
        *error = ENOSYS;
      }
      return false;
    }
    if (!io_uring_buffers_registered_) {
      if (error != nullptr) {
        *error = ENOENT;
      }
      return false;
    }
    if (io_uring_pending_submissions_ != 0U) {
      const int submit_error = flush_io_uring_submissions();
      if (submit_error != 0) {
        if (error != nullptr) {
          *error = submit_error;
        }
        fail_io_uring_backend(submit_error, nullptr);
        return false;
      }
    }
    if (io_uring_operations_ != nullptr) {
      if (error != nullptr) {
        *error = EBUSY;
      }
      return false;
    }

    if (detail::sys_io_uring_register(io_uring_fd_, IORING_UNREGISTER_BUFFERS,
                                      nullptr, 0) != 0) {
      if (error != nullptr) {
        *error = errno == 0 ? EIO : errno;
      }
      return false;
    }

    io_uring_buffers_registered_ = false;
    io_uring_registered_buffer_count_ = 0;
    return true;
#else
    if (error != nullptr) {
      *error = ENOSYS;
    }
    return false;
#endif
  }
#endif
  [[nodiscard]] bool
  register_io_uring_provided_buffer_ring(void *ring, unsigned ring_entries,
                                         std::uint16_t buffer_group,
                                         int *error) noexcept {
    AF_ASSERT(
        RuntimeT::current_thread_index_ == index_ &&
        "io_uring provided buffer ring registration must run on its IO thread");
    if (error != nullptr) {
      *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_ || ring == nullptr ||
        ring_entries == 0U || (ring_entries & (ring_entries - 1U)) != 0U) {
      if (error != nullptr) {
        *error = EINVAL;
      }
      return false;
    }

#if defined(__linux__)
    if (io_uring_fd_ < 0) {
      if (error != nullptr) {
        *error = ENOSYS;
      }
      return false;
    }
    if (provided_buffer_group_registered(buffer_group)) {
      if (error != nullptr) {
        *error = EALREADY;
      }
      return false;
    }

    detail::IoUringBufferRingRegistration registration{};
    registration.ring_addr = reinterpret_cast<std::uint64_t>(ring);
    registration.ring_entries = ring_entries;
    registration.bgid = buffer_group;
    if (detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_PBUF_RING,
                                      &registration, 1) != 0) {
      if (error != nullptr) {
        *error = errno == 0 ? EIO : errno;
      }
      return false;
    }

    try {
      io_uring_provided_buffer_groups_.push_back(buffer_group);
    } catch (...) {
      detail::IoUringBufferRingRegistration undo{};
      undo.bgid = buffer_group;
      static_cast<void>(detail::sys_io_uring_register(
          io_uring_fd_, IORING_UNREGISTER_PBUF_RING, &undo, 1));
      if (error != nullptr) {
        *error = ENOMEM;
      }
      return false;
    }
    return true;
#else
    if (error != nullptr) {
      *error = ENOSYS;
    }
    return false;
#endif
  }
  [[nodiscard]] bool
  unregister_io_uring_provided_buffer_ring(std::uint16_t buffer_group,
                                           int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring provided buffer ring unregistration must run on its IO "
              "thread");
    if (error != nullptr) {
      *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_) {
      if (error != nullptr) {
        *error = EINVAL;
      }
      return false;
    }

#if defined(__linux__)
    if (io_uring_fd_ < 0) {
      if (error != nullptr) {
        *error = ENOSYS;
      }
      return false;
    }
    auto it = std::find(io_uring_provided_buffer_groups_.begin(),
                        io_uring_provided_buffer_groups_.end(), buffer_group);
    if (it == io_uring_provided_buffer_groups_.end()) {
      if (error != nullptr) {
        *error = ENOENT;
      }
      return false;
    }
    if (io_uring_pending_submissions_ != 0U) {
      const int submit_error = flush_io_uring_submissions();
      if (submit_error != 0) {
        if (error != nullptr) {
          *error = submit_error;
        }
        fail_io_uring_backend(submit_error, nullptr);
        return false;
      }
    }
    if (io_uring_operations_ != nullptr) {
      if (error != nullptr) {
        *error = EBUSY;
      }
      return false;
    }

    detail::IoUringBufferRingRegistration registration{};
    registration.bgid = buffer_group;
    if (detail::sys_io_uring_register(io_uring_fd_, IORING_UNREGISTER_PBUF_RING,
                                      &registration, 1) != 0) {
      if (error != nullptr) {
        *error = errno == 0 ? EIO : errno;
      }
      return false;
    }

    io_uring_provided_buffer_groups_.erase(it);
    return true;
#else
    if (error != nullptr) {
      *error = ENOSYS;
    }
    return false;
#endif
  }
  [[nodiscard]] bool register_io_uring_files(const int *files,
                                             unsigned file_count,
                                             int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring file registration must run on its IO thread");
    if (error != nullptr) {
      *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_ || files == nullptr ||
        file_count == 0U) {
      if (error != nullptr) {
        *error = EINVAL;
      }
      return false;
    }

#if defined(__linux__)
    if (io_uring_fd_ < 0) {
      if (error != nullptr) {
        *error = ENOSYS;
      }
      return false;
    }
    if (io_uring_files_registered_) {
      if (error != nullptr) {
        *error = EALREADY;
      }
      return false;
    }
    if (file_count > static_cast<unsigned>(std::numeric_limits<int>::max())) {
      if (error != nullptr) {
        *error = EINVAL;
      }
      return false;
    }

    if (detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_FILES,
                                      files, file_count) != 0) {
      if (error != nullptr) {
        *error = errno == 0 ? EIO : errno;
      }
      return false;
    }

    io_uring_files_registered_ = true;
    io_uring_registered_file_count_ = file_count;
    return true;
#else
    if (error != nullptr) {
      *error = ENOSYS;
    }
    return false;
#endif
  }
  [[nodiscard]] bool unregister_io_uring_files(int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring file unregistration must run on its IO thread");
    if (error != nullptr) {
      *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_) {
      if (error != nullptr) {
        *error = EINVAL;
      }
      return false;
    }

#if defined(__linux__)
    if (io_uring_fd_ < 0) {
      if (error != nullptr) {
        *error = ENOSYS;
      }
      return false;
    }
    if (!io_uring_files_registered_) {
      if (error != nullptr) {
        *error = ENOENT;
      }
      return false;
    }
    if (io_uring_pending_submissions_ != 0U) {
      const int submit_error = flush_io_uring_submissions();
      if (submit_error != 0) {
        if (error != nullptr) {
          *error = submit_error;
        }
        fail_io_uring_backend(submit_error, nullptr);
        return false;
      }
    }
    if (io_uring_operations_ != nullptr) {
      if (error != nullptr) {
        *error = EBUSY;
      }
      return false;
    }

    if (detail::sys_io_uring_register(io_uring_fd_, IORING_UNREGISTER_FILES,
                                      nullptr, 0) != 0) {
      if (error != nullptr) {
        *error = errno == 0 ? EIO : errno;
      }
      return false;
    }

    io_uring_files_registered_ = false;
    io_uring_registered_file_count_ = 0;
    return true;
#else
    if (error != nullptr) {
      *error = ENOSYS;
    }
    return false;
#endif
  }
  [[nodiscard]] bool update_io_uring_files(unsigned offset, const int *files,
                                           unsigned file_count,
                                           int *error) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring file update must run on its IO thread");
    if (error != nullptr) {
      *error = 0;
    }
    if (RuntimeT::current_thread_index_ != index_ || files == nullptr ||
        file_count == 0U) {
      if (error != nullptr) {
        *error = EINVAL;
      }
      return false;
    }

#if defined(__linux__)
    if (io_uring_fd_ < 0) {
      if (error != nullptr) {
        *error = ENOSYS;
      }
      return false;
    }
    if (!io_uring_files_registered_) {
      if (error != nullptr) {
        *error = ENOENT;
      }
      return false;
    }
    if (offset > io_uring_registered_file_count_ ||
        file_count > io_uring_registered_file_count_ - offset) {
      if (error != nullptr) {
        *error = EINVAL;
      }
      return false;
    }
    if (io_uring_pending_submissions_ != 0U) {
      const int submit_error = flush_io_uring_submissions();
      if (submit_error != 0) {
        if (error != nullptr) {
          *error = submit_error;
        }
        fail_io_uring_backend(submit_error, nullptr);
        return false;
      }
    }
    if (io_uring_operations_ != nullptr) {
      if (error != nullptr) {
        *error = EBUSY;
      }
      return false;
    }

    io_uring_files_update update{};
    update.offset = offset;
    update.fds = reinterpret_cast<std::uint64_t>(files);
    const int updated = detail::sys_io_uring_register(
        io_uring_fd_, IORING_REGISTER_FILES_UPDATE, &update, file_count);
    if (updated < 0) {
      if (error != nullptr) {
        *error = errno == 0 ? EIO : errno;
      }
      return false;
    }
    if (static_cast<unsigned>(updated) != file_count) {
      if (error != nullptr) {
        *error = EIO;
      }
      return false;
    }
    return true;
#else
    if (error != nullptr) {
      *error = ENOSYS;
    }
    return false;
#endif
  }

  [[nodiscard]] bool register_io_wait(int fd, std::uint32_t events, Task *task,
                                      IoResult *result,
                                      bool prefer_rearm = false) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_wait must be called from its IO thread");
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr ||
        result == nullptr) {
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

    auto *operation =
        static_cast<IoUringOperation *>(state.wait.completion_token);
    if (operation == nullptr || operation->result != &state.wait ||
        operation->poll_wait) {
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
    return submit_io_uring_buffer_op(IORING_OP_READ, fd, data, size, offset, 0,
                                     io_readable, task, result);
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

  [[nodiscard]] bool submit_io_uring_write(int fd, const void *data,
                                           std::size_t size,
                                           std::uint64_t offset, Task *task,
                                           IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_buffer_op(IORING_OP_WRITE, fd,
                                     const_cast<void *>(data), size, offset, 0,
                                     io_writable, task, result);
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
  [[nodiscard]] bool submit_io_timeout(std::chrono::nanoseconds timeout,
                                       Task *task, IoResult *result) noexcept {
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

  [[nodiscard]] bool submit_io_uring_timeout(std::chrono::nanoseconds timeout,
                                             Task *task,
                                             IoResult *result) noexcept {
#if defined(__linux__)
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring timeout submit must be called from its IO thread");
    if (result != nullptr) {
      result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr ||
        result == nullptr || timeout.count() <= 0) {
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

    const auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(timeout);
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
  [[nodiscard]] bool
  submit_io_uring_read_fixed_file(int file_index, void *data, std::size_t size,
                                  std::uint64_t offset, Task *task,
                                  IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fixed_file_rw(IORING_OP_READ, file_index, data, size,
                                         offset, io_readable, task, result);
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

  [[nodiscard]] bool
  submit_io_uring_write_fixed_file(int file_index, const void *data,
                                   std::size_t size, std::uint64_t offset,
                                   Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fixed_file_rw(IORING_OP_WRITE, file_index,
                                         const_cast<void *>(data), size, offset,
                                         io_writable, task, result);
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
  [[nodiscard]] bool
  submit_io_uring_readv_fixed_file(int file_index, const iovec *iov,
                                   int iov_count, std::uint64_t offset,
                                   Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fixed_file_rw(
        IORING_OP_READV, file_index, const_cast<iovec *>(iov),
        static_cast<std::size_t>(iov_count), offset, io_readable, task, result);
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

  [[nodiscard]] bool
  submit_io_uring_writev_fixed_file(int file_index, const iovec *iov,
                                    int iov_count, std::uint64_t offset,
                                    Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fixed_file_rw(
        IORING_OP_WRITEV, file_index, const_cast<iovec *>(iov),
        static_cast<std::size_t>(iov_count), offset, io_writable, task, result);
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
  [[nodiscard]] bool submit_io_uring_read_fixed_file(
      int file_index, void *data, std::size_t size, std::uint64_t offset,
      std::uint16_t buffer_index, Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fixed_file_rw(IORING_OP_READ_FIXED, file_index, data,
                                         size, offset, io_readable, task,
                                         result, buffer_index, true);
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

  [[nodiscard]] bool submit_io_uring_write_fixed_file(
      int file_index, const void *data, std::size_t size, std::uint64_t offset,
      std::uint16_t buffer_index, Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fixed_file_rw(
        IORING_OP_WRITE_FIXED, file_index, const_cast<void *>(data), size,
        offset, io_writable, task, result, buffer_index, true);
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
  [[nodiscard]] bool
  submit_io_uring_read_fixed(int fd, void *data, std::size_t size,
                             std::uint64_t offset, std::uint16_t buffer_index,
                             Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_READ_FIXED, fd, data, size, offset, 0,
                              io_readable, task, result, nullptr, 0, nullptr,
                              nullptr, 0, nullptr, nullptr, nullptr, 0, 0, -1,
                              buffer_index, false);
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

  [[nodiscard]] bool
  submit_io_uring_write_fixed(int fd, const void *data, std::size_t size,
                              std::uint64_t offset, std::uint16_t buffer_index,
                              Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(
        IORING_OP_WRITE_FIXED, fd, const_cast<void *>(data), size, offset, 0,
        io_writable, task, result, nullptr, 0, nullptr, nullptr, 0, nullptr,
        nullptr, nullptr, 0, 0, -1, buffer_index, false);
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
  [[nodiscard]] bool submit_io_uring_readv(int fd, const iovec *iov,
                                           int iov_count, std::uint64_t offset,
                                           Task *task,
                                           IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_READV, fd, const_cast<iovec *>(iov),
                              static_cast<std::size_t>(iov_count), offset, 0,
                              io_readable, task, result);
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

  [[nodiscard]] bool submit_io_uring_writev(int fd, const iovec *iov,
                                            int iov_count, std::uint64_t offset,
                                            Task *task,
                                            IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_WRITEV, fd, const_cast<iovec *>(iov),
                              static_cast<std::size_t>(iov_count), offset, 0,
                              io_writable, task, result);
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
  [[nodiscard]] bool submit_io_uring_fsync(int fd, std::uint32_t flags,
                                           Task *task,
                                           IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_FSYNC, fd, nullptr, 0, 0, flags,
                              io_writable, task, result);
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

  [[nodiscard]] bool
  submit_io_uring_fsync_fixed_file(int file_index, std::uint32_t flags,
                                   Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_FSYNC, file_index, nullptr, 0, 0, flags,
                              io_writable, task, result, nullptr, 0, nullptr,
                              nullptr, 0, nullptr, nullptr, nullptr, 0, 0, -1,
                              0, true);
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

  [[nodiscard]] bool submit_io_uring_openat(int dir_fd, const char *path,
                                            int flags, std::uint32_t mode,
                                            Task *task,
                                            IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(
        IORING_OP_OPENAT, dir_fd, const_cast<char *>(path), mode, 0,
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

  [[nodiscard]] bool submit_io_uring_openat_direct(int dir_fd, const char *path,
                                                   int flags,
                                                   std::uint32_t mode,
                                                   int file_index, Task *task,
                                                   IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(
        IORING_OP_OPENAT, dir_fd, const_cast<char *>(path), mode, 0,
        static_cast<std::uint32_t>(flags), io_readable, task, result, nullptr,
        0, nullptr, nullptr, 0, nullptr, nullptr, nullptr, 0, 0, -1, 0, false,
        false, false, 0, false, file_index);
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
                                             const struct open_how *how,
                                             Task *task,
                                             IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fast_sqe(
        detail::io_uring_op_openat2, dir_fd, io_readable, task, result,
        [&](io_uring_sqe &sqe) noexcept {
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
  [[nodiscard]] bool submit_io_uring_close(int fd, Task *task,
                                           IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_CLOSE, fd, nullptr, 0, 0, 0,
                              io_writable, task, result);
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
    return submit_io_uring_op(IORING_OP_SHUTDOWN, fd, nullptr,
                              static_cast<std::size_t>(how), 0, 0, io_writable,
                              task, result);
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
  [[nodiscard]] bool submit_io_uring_statx(int dir_fd, const char *path,
                                           int flags, std::uint32_t mask,
                                           struct statx *output, Task *task,
                                           IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fast_sqe(
        IORING_OP_STATX, dir_fd, io_readable, task, result,
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
  [[nodiscard]] bool submit_io_uring_fallocate(int fd, int mode,
                                               std::uint64_t offset,
                                               std::uint64_t length, Task *task,
                                               IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fast_sqe(IORING_OP_FALLOCATE, fd, io_writable, task,
                                    result, [&](io_uring_sqe &sqe) noexcept {
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

  [[nodiscard]] bool submit_io_uring_ftruncate(int fd, std::uint64_t length,
                                               Task *task,
                                               IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fast_sqe(detail::io_uring_op_ftruncate, fd,
                                    io_writable, task, result,
                                    [&](io_uring_sqe &sqe) noexcept {
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
  [[nodiscard]] bool
  submit_io_uring_renameat(int old_dir_fd, const char *old_path, int new_dir_fd,
                           const char *new_path, std::uint32_t flags,
                           Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fast_sqe(
        IORING_OP_RENAMEAT, old_dir_fd, io_writable, task, result,
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

  [[nodiscard]] bool submit_io_uring_unlinkat(int dir_fd, const char *path,
                                              int flags, Task *task,
                                              IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fast_sqe(
        IORING_OP_UNLINKAT, dir_fd, io_writable, task, result,
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

  [[nodiscard]] bool submit_io_uring_mkdirat(int dir_fd, const char *path,
                                             std::uint32_t mode, Task *task,
                                             IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fast_sqe(
        detail::io_uring_op_mkdirat, dir_fd, io_writable, task, result,
        [&](io_uring_sqe &sqe) noexcept {
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

  [[nodiscard]] bool submit_io_uring_symlinkat(const char *target,
                                               int new_dir_fd,
                                               const char *link_path,
                                               Task *task,
                                               IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fast_sqe(
        detail::io_uring_op_symlinkat, new_dir_fd, io_writable, task, result,
        [&](io_uring_sqe &sqe) noexcept {
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

  [[nodiscard]] bool
  submit_io_uring_linkat(int old_dir_fd, const char *old_path, int new_dir_fd,
                         const char *new_path, std::uint32_t flags, Task *task,
                         IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_fast_sqe(
        detail::io_uring_op_linkat, old_dir_fd, io_writable, task, result,
        [&](io_uring_sqe &sqe) noexcept {
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
  [[nodiscard]] bool submit_io_uring_splice(int in_fd, std::int64_t off_in,
                                            int out_fd, std::int64_t off_out,
                                            std::size_t count,
                                            unsigned int flags, Task *task,
                                            IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_SPLICE, out_fd, nullptr, count,
                              static_cast<std::uint64_t>(off_out), flags,
                              io_writable, task, result, nullptr, 0, nullptr,
                              nullptr, 0, nullptr, nullptr, nullptr, 0,
                              static_cast<std::uint64_t>(off_in), in_fd);
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
    return submit_io_uring_buffer_op(IORING_OP_RECV, fd, data, size, 0, flags,
                                     io_readable, task, result);
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

  [[nodiscard]] bool
  submit_io_uring_recv_fixed_file(int file_index, void *data, std::size_t size,
                                  std::uint32_t flags, Task *task,
                                  IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_RECV, file_index, data, size, 0, flags,
                              io_readable, task, result, nullptr, 0, nullptr,
                              nullptr, 0, nullptr, nullptr, nullptr, 0, 0, -1,
                              0, true);
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
  [[nodiscard]] bool submit_io_uring_recv_multishot(int fd,
                                                    std::uint16_t buffer_group,
                                                    std::uint32_t flags,
                                                    Task *task,
                                                    IoResult *result) noexcept {
    if (!provided_buffer_group_registered(buffer_group)) {
      if (result != nullptr) {
        result->fd = fd;
        result->events = io_error;
        result->error = ENOBUFS;
      }
      return false;
    }
    return submit_io_uring_op(IORING_OP_RECV, fd, nullptr, 0, 0, flags,
                              io_readable, task, result, nullptr, 0, nullptr,
                              nullptr, 0, nullptr, nullptr, nullptr, 0, 0, -1,
                              0, false, true, false, buffer_group, true);
  }

  [[nodiscard]] bool submit_io_uring_recvmsg_multishot(
      int fd, std::uint16_t buffer_group, socklen_t name_capacity,
      std::size_t control_capacity, std::uint32_t flags, Task *task,
      IoResult *result) noexcept {
    if (!provided_buffer_group_registered(buffer_group)) {
      if (result != nullptr) {
        result->fd = fd;
        result->events = io_error;
        result->error = ENOBUFS;
      }
      return false;
    }
    return submit_io_uring_op(
        IORING_OP_RECVMSG, fd, nullptr, control_capacity, 0, flags, io_readable,
        task, result, nullptr, name_capacity, nullptr, nullptr, 0, nullptr,
        nullptr, nullptr, 0, 0, -1, 0, false, true, false, buffer_group, true);
  }
#endif
  [[nodiscard]] bool submit_io_uring_send(int fd, const void *data,
                                          std::size_t size, std::uint32_t flags,
                                          Task *task,
                                          IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_buffer_op(IORING_OP_SEND, fd,
                                     const_cast<void *>(data), size, 0, flags,
                                     io_writable, task, result);
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

  [[nodiscard]] bool
  submit_io_uring_send_fixed_file(int file_index, const void *data,
                                  std::size_t size, std::uint32_t flags,
                                  Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(
        IORING_OP_SEND, file_index, const_cast<void *>(data), size, 0, flags,
        io_writable, task, result, nullptr, 0, nullptr, nullptr, 0, nullptr,
        nullptr, nullptr, 0, 0, -1, 0, true);
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
  [[nodiscard]] bool submit_io_uring_send_zc(int fd, const void *data,
                                             std::size_t size,
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
    return submit_io_uring_op(
        detail::io_uring_op_send_zc, fd, const_cast<void *>(data), size, 0,
        flags, io_writable, task, result, nullptr, 0, nullptr, nullptr, 0,
        nullptr, nullptr, nullptr, 0, 0, -1, 0, false, false, true);
  }

  [[nodiscard]] bool submit_io_uring_sendmsg_zc(int fd, const void *data,
                                                std::size_t size,
                                                const sockaddr *address,
                                                socklen_t address_size,
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
    return submit_io_uring_op(
        detail::io_uring_op_sendmsg_zc, fd, const_cast<void *>(data), size, 0,
        flags, io_writable, task, result, const_cast<sockaddr *>(address),
        address_size, nullptr, nullptr, 0, nullptr, nullptr, nullptr, 0, 0, -1,
        0, false, false, true);
  }

  [[nodiscard]] bool
  submit_io_uring_sendmsg_zc_iov(int fd, const iovec *iov, int iov_count,
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
        detail::io_uring_op_sendmsg_zc, fd, nullptr, 0, 0, flags, io_writable,
        task, result, const_cast<sockaddr *>(address), address_size, nullptr,
        nullptr, 0, nullptr, nullptr, iov, static_cast<std::size_t>(iov_count),
        0, -1, 0, false, false, true);
  }
#endif
#if !defined(_WIN32)
  [[nodiscard]] bool submit_io_uring_recvmsg_fixed_file_iov(
      int file_index, const iovec *iov, int iov_count, std::uint32_t flags,
      Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(
        IORING_OP_RECVMSG, file_index, nullptr, 0, 0, flags, io_readable, task,
        result, nullptr, 0, nullptr, nullptr, 0, nullptr, nullptr, iov,
        static_cast<std::size_t>(iov_count), 0, -1, 0, true);
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

  [[nodiscard]] bool
  submit_io_uring_recvmsg_iov(int fd, const iovec *iov, int iov_count,
                              sockaddr *address, socklen_t *address_size,
                              std::uint32_t flags, Task *task,
                              IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(
        IORING_OP_RECVMSG, fd, nullptr, 0, 0, flags, io_readable, task, result,
        address, address_size == nullptr ? 0 : *address_size, address_size,
        nullptr, 0, nullptr, nullptr, iov, static_cast<std::size_t>(iov_count));
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

  [[nodiscard]] bool submit_io_uring_recvmsg(int fd, void *data,
                                             std::size_t size,
                                             sockaddr *address,
                                             socklen_t *address_size,
                                             std::uint32_t flags, Task *task,
                                             IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(
        IORING_OP_RECVMSG, fd, data, size, 0, flags, io_readable, task, result,
        address, address_size == nullptr ? 0 : *address_size, address_size);
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
  [[nodiscard]] bool submit_io_uring_sendmsg_fixed_file_iov(
      int file_index, const iovec *iov, int iov_count, std::uint32_t flags,
      Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(
        IORING_OP_SENDMSG, file_index, nullptr, 0, 0, flags, io_writable, task,
        result, nullptr, 0, nullptr, nullptr, 0, nullptr, nullptr, iov,
        static_cast<std::size_t>(iov_count), 0, -1, 0, true);
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

  [[nodiscard]] bool
  submit_io_uring_sendmsg_iov(int fd, const iovec *iov, int iov_count,
                              const sockaddr *address, socklen_t address_size,
                              std::uint32_t flags, Task *task,
                              IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(
        IORING_OP_SENDMSG, fd, nullptr, 0, 0, flags, io_writable, task, result,
        const_cast<sockaddr *>(address), address_size, nullptr, nullptr, 0,
        nullptr, nullptr, iov, static_cast<std::size_t>(iov_count));
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

  [[nodiscard]] bool submit_io_uring_sendmsg(int fd, const void *data,
                                             std::size_t size,
                                             const sockaddr *address,
                                             socklen_t address_size,
                                             std::uint32_t flags, Task *task,
                                             IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_SENDMSG, fd, const_cast<void *>(data),
                              size, 0, flags, io_writable, task, result,
                              const_cast<sockaddr *>(address), address_size,
                              nullptr);
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
  [[nodiscard]] bool submit_io_uring_accept(int fd, sockaddr *address,
                                            socklen_t *address_size, int flags,
                                            Task *task,
                                            IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_ACCEPT, fd, nullptr, 0, 0,
                              static_cast<std::uint32_t>(flags), io_readable,
                              task, result, nullptr, 0, nullptr, nullptr, 0,
                              address, address_size);
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
                                                   socklen_t *address_size,
                                                   int flags, int file_index,
                                                   Task *task,
                                                   IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(IORING_OP_ACCEPT, fd, nullptr, 0, 0,
                              static_cast<std::uint32_t>(flags), io_readable,
                              task, result, nullptr, 0, nullptr, nullptr, 0,
                              address, address_size, nullptr, 0, 0, -1, 0,
                              false, false, false, 0, false, file_index);
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

  [[nodiscard]] bool
  submit_io_uring_accept_multishot(int fd, sockaddr *address,
                                   socklen_t *address_size, int flags,
                                   Task *task, IoResult *result) noexcept {
#if defined(__linux__)
    return submit_io_uring_op(
        IORING_OP_ACCEPT, fd, nullptr, 0, 0, static_cast<std::uint32_t>(flags),
        io_readable, task, result, nullptr, 0, nullptr, nullptr, 0, address,
        address_size, nullptr, 0, 0, -1, 0, false, true);
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
    return submit_io_uring_op(IORING_OP_CONNECT, fd, nullptr, 0, 0, 0,
                              io_writable, task, result, nullptr, 0, nullptr,
                              address, address_size, nullptr, nullptr);
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
    return submit_io_uring_socket_impl(domain, type, protocol, flags, task,
                                       result);
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

  void mark_ready(std::uint16_t source) noexcept {
    ready_sources_.mark(source);
  }

  void notify_external_ready() noexcept {
    if (!external_ready_.load(std::memory_order_acquire)) {
      external_ready_.store(true, std::memory_order_release);
      notify_force();
      return;
    }
    notify();
  }
  [[nodiscard]] bool try_push_local(Task *task) noexcept {
    if (local_size_ == local_queue_.size()) {
      return false;
    }

    local_queue_[local_tail_ & (local_queue_.size() - 1U)] = task;
    ++local_tail_;
    ++local_size_;
    return true;
  }

  [[nodiscard]] Task *try_pop_local() noexcept {
    if (local_size_ == 0) {
      return nullptr;
    }

    Task *task = local_queue_[local_head_ & (local_queue_.size() - 1U)];
    ++local_head_;
    --local_size_;
    return task;
  }
  void execute(Task *task) noexcept {
    TaskState expected = TaskState::Queued;
    if (!task->state_.compare_exchange_strong(expected, TaskState::Starting,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
      AF_ASSERT(false && "executor popped a task that was not queued");
      return;
    }
    task->prepare_running_epoch();
    task->state_.store(TaskState::Running, std::memory_order_release);

    TaskResult result = TaskResult::Done;
    Task *previous_running_task = running_task_;
    running_task_ = task;
    try {
      result = task->run();
    } catch (...) {
      AF_ASSERT(false && "task::run must not throw");
      result = TaskResult::Done;
    }
    running_task_ = previous_running_task;

    switch (result) {
    case TaskResult::Done:
      finish_done(task);
      break;
    case TaskResult::Pending:
      finish_pending(task);
      break;
    case TaskResult::Again:
      finish_again(task);
      break;
    case TaskResult::Failed:
      finish_done(task);
      break;
    case TaskResult::Cancelled:
      finish_done(task);
      break;
    }
  }

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
  [[nodiscard]] bool
  provided_buffer_group_registered(std::uint16_t buffer_group) const noexcept {
    return std::find(io_uring_provided_buffer_groups_.begin(),
                     io_uring_provided_buffer_groups_.end(),
                     buffer_group) != io_uring_provided_buffer_groups_.end();
  }

  [[nodiscard]] IoUringPollSubmitResult
  try_submit_io_uring_poll_wait(int fd, std::uint32_t events, Task *task,
                                IoResult *result,
                                IoWaitRegistration *registration) noexcept {
    if (!io_uring_thread() || io_uring_fd_ < 0 ||
        !io_uring_poll_add_available_) {
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
  [[nodiscard]] bool submit_io_uring_buffer_op(
      std::uint8_t opcode, int fd, void *data, std::size_t size,
      std::uint64_t offset, std::uint32_t op_flags,
      std::uint32_t complete_events, Task *task, IoResult *result) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring submit must be called from its IO thread");
    if (result != nullptr) {
      result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr ||
        result == nullptr) {
      if (result != nullptr) {
        result->fd = fd;
        result->events = io_error;
        result->error = EINVAL;
      }
      return false;
    }
    if (io_uring_fd_ < 0 || fd < 0) {
      result->fd = fd;
      result->events = io_error;
      result->error = io_uring_fd_ < 0 ? ENOSYS : EBADF;
      return false;
    }
    if (data == nullptr) {
      result->fd = fd;
      result->events = io_error;
      result->error = EINVAL;
      return false;
    }
    if (!detail::io_uring_sqe_len_fits(size)) {
      result->fd = fd;
      result->events = io_error;
      result->error = EINVAL;
      return false;
    }

    IoUringOperation *operation = nullptr;
    try {
      operation = io_uring_op_pool_.create();
    } catch (...) {
      result->fd = fd;
      result->events = io_error;
      result->error = ENOMEM;
      return false;
    }

    operation->task = task;
    operation->result = result;
    operation->complete_events = complete_events;
    operation->direct_file_index = -1;
    operation->opcode = opcode;
    operation->cancel_requested = false;
    operation->multishot = false;
    operation->poll_wait = false;
    operation->zero_copy_send = false;
    operation->zero_copy_primary_done = false;
    operation->zero_copy_notification_done = false;
    operation->msg = nullptr;
    operation->socket_address = nullptr;
    operation->wait_registration = nullptr;

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
      io_uring_op_pool_.destroy(operation);
      result->fd = fd;
      result->events = io_error;
      result->error = reserve_error == 0 ? EBUSY : reserve_error;
      return false;
    }

    track_io_uring_operation(operation);

    detail::fill_buffer_sqe(
        *sqe,
        detail::IoUringBufferSqe{opcode, fd, data, size, offset, op_flags},
        reinterpret_cast<std::uint64_t>(operation));

    result->fd = fd;
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
  }
#endif
#if defined(__linux__)
  template <typename FillSqe>
  [[nodiscard]] bool
  submit_io_uring_fast_sqe(std::uint8_t opcode, int result_fd,
                           std::uint32_t complete_events, Task *task,
                           IoResult *result, FillSqe &&fill_sqe) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring submit must be called from its IO thread");
    if (result != nullptr) {
      result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr ||
        result == nullptr) {
      if (result != nullptr) {
        result->fd = result_fd;
        result->events = io_error;
        result->error = EINVAL;
      }
      return false;
    }
    if (io_uring_fd_ < 0) {
      result->fd = result_fd;
      result->events = io_error;
      result->error = ENOSYS;
      return false;
    }

    IoUringOperation *operation = nullptr;
    try {
      operation = io_uring_op_pool_.create();
    } catch (...) {
      result->fd = result_fd;
      result->events = io_error;
      result->error = ENOMEM;
      return false;
    }

    operation->task = task;
    operation->result = result;
    operation->complete_events = complete_events;
    operation->direct_file_index = -1;
    operation->opcode = opcode;
    operation->cancel_requested = false;
    operation->multishot = false;
    operation->poll_wait = false;
    operation->zero_copy_send = false;
    operation->zero_copy_primary_done = false;
    operation->zero_copy_notification_done = false;
    operation->msg = nullptr;
    operation->socket_address = nullptr;
    operation->wait_registration = nullptr;

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
      io_uring_op_pool_.destroy(operation);
      result->fd = result_fd;
      result->events = io_error;
      result->error = reserve_error == 0 ? EBUSY : reserve_error;
      return false;
    }

    track_io_uring_operation(operation);

    *sqe = io_uring_sqe{};
    sqe->opcode = opcode;
    sqe->user_data = reinterpret_cast<std::uint64_t>(operation);
    fill_sqe(*sqe);

    result->fd = result_fd;
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
  }
#endif
#if defined(__linux__)
  [[nodiscard]] bool submit_io_uring_socket_impl(int domain, int type,
                                                 int protocol,
                                                 std::uint32_t flags,
                                                 Task *task,
                                                 IoResult *result) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring socket submit must be called from its IO thread");
    if (result != nullptr) {
      result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr ||
        result == nullptr) {
      if (result != nullptr) {
        result->fd = -1;
        result->events = io_error;
        result->error = EINVAL;
      }
      return false;
    }
    if (io_uring_fd_ < 0 || !io_uring_socket_available_) {
      result->fd = -1;
      result->events = io_error;
      result->error = ENOSYS;
      return false;
    }

    IoUringOperation *operation = nullptr;
    try {
      operation = io_uring_op_pool_.create();
    } catch (...) {
      result->fd = -1;
      result->events = io_error;
      result->error = ENOMEM;
      return false;
    }

    operation->task = task;
    operation->result = result;
    operation->complete_events = io_readable;
    operation->direct_file_index = -1;
    operation->opcode = detail::io_uring_op_socket;
    operation->cancel_requested = false;
    operation->multishot = false;
    operation->poll_wait = false;
    operation->zero_copy_send = false;
    operation->zero_copy_primary_done = false;
    operation->zero_copy_notification_done = false;
    operation->msg = nullptr;
    operation->socket_address = nullptr;
    operation->wait_registration = nullptr;

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
      io_uring_op_pool_.destroy(operation);
      result->fd = -1;
      result->events = io_error;
      result->error = reserve_error == 0 ? EBUSY : reserve_error;
      return false;
    }

    track_io_uring_operation(operation);

    *sqe = io_uring_sqe{};
    sqe->opcode = detail::io_uring_op_socket;
    sqe->fd = domain;
    sqe->off = static_cast<std::uint64_t>(type);
    sqe->len = static_cast<unsigned>(protocol);
    sqe->rw_flags = flags;
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
  }
#endif
#if defined(__linux__)
  [[nodiscard]] bool submit_io_uring_fixed_file_rw(
      std::uint8_t opcode, int file_index, void *data, std::size_t size,
      std::uint64_t offset, std::uint32_t complete_events, Task *task,
      IoResult *result, std::uint16_t fixed_buffer_index = 0,
      bool fixed_buffer = false) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring submit must be called from its IO thread");
    if (result != nullptr) {
      result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr ||
        result == nullptr) {
      if (result != nullptr) {
        result->fd = file_index;
        result->events = io_error;
        result->error = EINVAL;
      }
      return false;
    }
    if (io_uring_fd_ < 0 || file_index < 0) {
      result->fd = file_index;
      result->events = io_error;
      result->error = io_uring_fd_ < 0 ? ENOSYS : EBADF;
      return false;
    }
    if (data == nullptr) {
      result->fd = file_index;
      result->events = io_error;
      result->error = EINVAL;
      return false;
    }
    if (!io_uring_files_registered_) {
      result->fd = file_index;
      result->events = io_error;
      result->error = ENXIO;
      return false;
    }
    if (static_cast<unsigned>(file_index) >= io_uring_registered_file_count_) {
      result->fd = file_index;
      result->events = io_error;
      result->error = EINVAL;
      return false;
    }
    if (fixed_buffer) {
      if (!io_uring_buffers_registered_) {
        result->fd = file_index;
        result->events = io_error;
        result->error = ENOBUFS;
        return false;
      }
      if (fixed_buffer_index >= io_uring_registered_buffer_count_) {
        result->fd = file_index;
        result->events = io_error;
        result->error = EINVAL;
        return false;
      }
    }
    if (!detail::io_uring_sqe_len_fits(size)) {
      result->fd = file_index;
      result->events = io_error;
      result->error = EINVAL;
      return false;
    }

    IoUringOperation *operation = nullptr;
    try {
      operation = io_uring_op_pool_.create();
    } catch (...) {
      result->fd = file_index;
      result->events = io_error;
      result->error = ENOMEM;
      return false;
    }

    operation->task = task;
    operation->result = result;
    operation->complete_events = complete_events;
    operation->direct_file_index = -1;
    operation->opcode = opcode;
    operation->cancel_requested = false;
    operation->multishot = false;
    operation->poll_wait = false;
    operation->zero_copy_send = false;
    operation->zero_copy_primary_done = false;
    operation->zero_copy_notification_done = false;
    operation->msg = nullptr;
    operation->socket_address = nullptr;
    operation->wait_registration = nullptr;

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
      io_uring_op_pool_.destroy(operation);
      result->fd = file_index;
      result->events = io_error;
      result->error = reserve_error == 0 ? EBUSY : reserve_error;
      return false;
    }

    track_io_uring_operation(operation);

    detail::fill_fixed_file_rw_sqe(
        *sqe,
        detail::IoUringFixedFileRwSqe{opcode, file_index, data, size, offset,
                                      fixed_buffer_index, fixed_buffer},
        reinterpret_cast<std::uint64_t>(operation));

    result->fd = file_index;
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
  }
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

  static void set_io_uring_generic_submit_error(IoResult *result, int fd,
                                                int error) noexcept {
    if (result == nullptr) {
      return;
    }
    result->fd = fd;
    result->events = io_error;
    result->error = error;
  }

  [[nodiscard]] static IoUringGenericSubmitKind
  classify_io_uring_generic_submit(
      const IoUringGenericSubmitArgs &args) noexcept {
    const bool openat_op = args.opcode == IORING_OP_OPENAT;
    const bool statx_op = args.opcode == IORING_OP_STATX;
    const bool renameat_op = args.opcode == IORING_OP_RENAMEAT;
    const bool unlinkat_op = args.opcode == IORING_OP_UNLINKAT;
    const bool close_op = args.opcode == IORING_OP_CLOSE;
    const bool shutdown_op = args.opcode == IORING_OP_SHUTDOWN;
    const bool fallocate_op = args.opcode == IORING_OP_FALLOCATE;
    const bool splice_op = args.opcode == IORING_OP_SPLICE;
    const bool fixed_buffer_op = args.opcode == IORING_OP_READ_FIXED ||
                                 args.opcode == IORING_OP_WRITE_FIXED;
    const bool message_op = args.opcode == IORING_OP_RECVMSG ||
                            args.opcode == IORING_OP_SENDMSG ||
                            args.opcode == detail::io_uring_op_sendmsg_zc;
    const bool accept_op = args.opcode == IORING_OP_ACCEPT;
    const bool connect_op = args.opcode == IORING_OP_CONNECT;
    const bool message_iov_op = message_op && args.message_iov != nullptr;
    const bool accept_address_op = accept_op &&
                                   args.socket_address_out != nullptr &&
                                   args.socket_address_size_out != nullptr;
    return IoUringGenericSubmitKind{
        openat_op,
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
  [[nodiscard]] bool validate_io_uring_generic_submit(
      const IoUringGenericSubmitArgs &args,
      const IoUringGenericSubmitKind &kind) const noexcept {
    if (RuntimeT::current_thread_index_ != index_ || args.task == nullptr ||
        args.result == nullptr) {
      set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
      return false;
    }
    if (io_uring_fd_ < 0 || (!kind.path_fd_op && args.fd < 0)) {
      set_io_uring_generic_submit_error(args.result, args.fd,
                                        io_uring_fd_ < 0 ? ENOSYS : EBADF);
      return false;
    }
    if (!kind.data_optional_op && !kind.address_op && args.data == nullptr &&
        !kind.message_iov_op && !args.buffer_select) {
      set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
      return false;
    }
    if (!validate_io_uring_generic_fixed_resources(args, kind)) {
      return false;
    }
    if (args.opcode != IORING_OP_FSYNC && !kind.message_op &&
        args.size >
            static_cast<std::size_t>(std::numeric_limits<unsigned>::max())) {
      set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
      return false;
    }
    if (kind.message_iov_op &&
        (args.message_iov_count == 0U ||
         args.message_iov_count > static_cast<std::size_t>(IOV_MAX))) {
      set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
      return false;
    }
    if (kind.connect_op &&
        (args.socket_address == nullptr || args.socket_address_size == 0U ||
         args.socket_address_size >
             static_cast<socklen_t>(sizeof(sockaddr_storage)))) {
      set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
      return false;
    }
    if (kind.accept_op && ((args.socket_address_out == nullptr) !=
                           (args.socket_address_size_out == nullptr))) {
      set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
      return false;
    }
    if (args.buffer_select &&
        ((args.opcode != IORING_OP_RECV && args.opcode != IORING_OP_RECVMSG) ||
         !provided_buffer_group_registered(args.provided_buffer_group))) {
      const int error =
          (args.opcode == IORING_OP_RECV || args.opcode == IORING_OP_RECVMSG)
              ? ENOBUFS
              : EINVAL;
      set_io_uring_generic_submit_error(args.result, args.fd, error);
      return false;
    }
    return true;
  }

  [[nodiscard]] bool validate_io_uring_generic_fixed_resources(
      const IoUringGenericSubmitArgs &args,
      const IoUringGenericSubmitKind &kind) const noexcept {
    if (args.fixed_file) {
      if (kind.path_fd_op) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
      }
      if (!io_uring_files_registered_) {
        set_io_uring_generic_submit_error(args.result, args.fd, ENXIO);
        return false;
      }
      if (static_cast<unsigned>(args.fd) >= io_uring_registered_file_count_) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
      }
    }
    if (args.direct_file_index >= 0) {
      if (!(kind.openat_op || kind.accept_op)) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
      }
      if (!io_uring_files_registered_) {
        set_io_uring_generic_submit_error(args.result, args.fd, ENXIO);
        return false;
      }
      if (static_cast<unsigned>(args.direct_file_index) >=
          io_uring_registered_file_count_) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
      }
    }
    if (kind.fixed_buffer_op) {
      if (!io_uring_buffers_registered_) {
        set_io_uring_generic_submit_error(args.result, args.fd, ENOBUFS);
        return false;
      }
      if (args.fixed_buffer_index >= io_uring_registered_buffer_count_) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
      }
    }
    return true;
  }
#endif
#if defined(__linux__)
  [[nodiscard]] IoUringOperation *create_io_uring_generic_submit_operation(
      const IoUringGenericSubmitArgs &args,
      const IoUringGenericSubmitKind &kind) noexcept {
    IoUringOperation *operation = nullptr;
    try {
      operation = io_uring_op_pool_.create();
    } catch (...) {
      set_io_uring_generic_submit_error(args.result, args.fd, ENOMEM);
      return nullptr;
    }

    initialize_io_uring_generic_submit_operation(args, operation);

    if (kind.message_op &&
        !attach_io_uring_generic_submit_message(args, kind, operation)) {
      return nullptr;
    }
    if (kind.needs_socket_address &&
        !attach_io_uring_generic_submit_socket_address(args, kind, operation)) {
      return nullptr;
    }
    return operation;
  }

  static void initialize_io_uring_generic_submit_operation(
      const IoUringGenericSubmitArgs &args,
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

  [[nodiscard]] bool
  attach_io_uring_generic_submit_message(const IoUringGenericSubmitArgs &args,
                                         const IoUringGenericSubmitKind &kind,
                                         IoUringOperation *operation) noexcept {
    try {
      operation->msg = io_uring_msg_pool_.create();
    } catch (...) {
      io_uring_op_pool_.destroy(operation);
      set_io_uring_generic_submit_error(args.result, args.fd, ENOMEM);
      return false;
    }

    operation->msg->header = msghdr{};
    operation->msg->header.msg_name = args.message_name;
    operation->msg->header.msg_namelen = args.message_name_len;
    if (args.opcode == IORING_OP_RECVMSG && args.multishot &&
        args.buffer_select) {
      operation->msg->header.msg_controllen = args.size;
    } else if (kind.message_iov_op) {
      operation->msg->header.msg_iov = const_cast<iovec *>(args.message_iov);
      operation->msg->header.msg_iovlen = args.message_iov_count;
    } else {
      operation->msg->iov = iovec{args.data, args.size};
      operation->msg->header.msg_iov = &operation->msg->iov;
      operation->msg->header.msg_iovlen = 1;
    }
    operation->msg->address_size = args.message_name_len_out;
    return true;
  }

  [[nodiscard]] bool attach_io_uring_generic_submit_socket_address(
      const IoUringGenericSubmitArgs &args,
      const IoUringGenericSubmitKind &kind,
      IoUringOperation *operation) noexcept {
    try {
      operation->socket_address = io_uring_address_pool_.create();
    } catch (...) {
      destroy_io_uring_operation(operation);
      set_io_uring_generic_submit_error(args.result, args.fd, ENOMEM);
      return false;
    }

    operation->socket_address->storage = sockaddr_storage{};
    operation->socket_address->output = nullptr;
    operation->socket_address->output_size = nullptr;
    operation->socket_address->output_capacity = 0;
    if (kind.connect_op) {
      std::memcpy(&operation->socket_address->storage, args.socket_address,
                  args.socket_address_size);
      operation->socket_address->size = args.socket_address_size;
    } else {
      operation->socket_address->size =
          sizeof(operation->socket_address->storage);
      operation->socket_address->output = args.socket_address_out;
      operation->socket_address->output_size = args.socket_address_size_out;
      operation->socket_address->output_capacity =
          args.socket_address_size_out == nullptr
              ? 0
              : *args.socket_address_size_out;
    }
    return true;
  }
#endif
#if defined(__linux__)
  static void
  fill_io_uring_generic_submit_sqe(io_uring_sqe &sqe,
                                   const IoUringGenericSubmitArgs &args,
                                   const IoUringGenericSubmitKind &kind,
                                   IoUringOperation *operation) noexcept {
    initialize_io_uring_generic_submit_sqe(sqe, args, operation);

    if (args.opcode == IORING_OP_FSYNC) {
      sqe.fsync_flags = args.op_flags;
    } else if (kind.close_op) {
      // fd is already filled.
    } else if (kind.shutdown_op) {
      sqe.len = static_cast<unsigned>(args.size);
    } else if (kind.fallocate_op) {
      fill_io_uring_generic_fallocate_sqe(sqe, args);
    } else if (kind.splice_op) {
      fill_io_uring_generic_splice_sqe(sqe, args);
    } else if (kind.openat_op) {
      fill_io_uring_generic_openat_sqe(sqe, args);
    } else if (kind.statx_op) {
      fill_io_uring_generic_statx_sqe(sqe, args);
    } else if (kind.renameat_op) {
      fill_io_uring_generic_renameat_sqe(sqe, args);
    } else if (kind.unlinkat_op) {
      fill_io_uring_generic_unlinkat_sqe(sqe, args);
    } else if (kind.message_op) {
      fill_io_uring_generic_message_sqe(sqe, args, operation);
    } else if (kind.accept_op) {
      fill_io_uring_generic_accept_sqe(sqe, args, operation);
    } else if (kind.connect_op) {
      fill_io_uring_generic_connect_sqe(sqe, operation);
    } else if (kind.fixed_buffer_op) {
      fill_io_uring_generic_fixed_buffer_sqe(sqe, args);
    } else if (args.opcode == IORING_OP_RECV || args.opcode == IORING_OP_SEND ||
               args.opcode == detail::io_uring_op_send_zc) {
      fill_io_uring_generic_socket_data_sqe(sqe, args);
    } else {
      fill_io_uring_generic_buffer_sqe(sqe, args);
    }
  }

  static void
  initialize_io_uring_generic_submit_sqe(io_uring_sqe &sqe,
                                         const IoUringGenericSubmitArgs &args,
                                         IoUringOperation *operation) noexcept {
    sqe = io_uring_sqe{};
    sqe.opcode = args.opcode;
    sqe.fd = args.fd;
    sqe.user_data = reinterpret_cast<std::uint64_t>(operation);
    if (args.fixed_file) {
      sqe.flags |= IOSQE_FIXED_FILE;
    }
    if (args.buffer_select) {
      sqe.flags |= IOSQE_BUFFER_SELECT;
      sqe.buf_index = args.provided_buffer_group;
    }
    if (args.direct_file_index >= 0) {
      sqe.file_index = static_cast<std::uint32_t>(args.direct_file_index) + 1U;
    }
  }
#endif
#if defined(__linux__)
  static void fill_io_uring_generic_fallocate_sqe(
      io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = args.extra;
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
  }

  static void fill_io_uring_generic_splice_sqe(
      io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = args.extra;
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
    sqe.splice_fd_in = args.extra_fd;
    sqe.splice_flags = args.op_flags;
  }

  static void fill_io_uring_generic_openat_sqe(
      io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.open_flags = args.op_flags;
  }

  static void fill_io_uring_generic_statx_sqe(
      io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
    sqe.statx_flags = args.op_flags;
  }

  static void fill_io_uring_generic_renameat_sqe(
      io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
    sqe.rename_flags = args.op_flags;
  }

  static void fill_io_uring_generic_unlinkat_sqe(
      io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.unlink_flags = args.op_flags;
  }
#endif
#if defined(__linux__)
  static void
  fill_io_uring_generic_message_sqe(io_uring_sqe &sqe,
                                    const IoUringGenericSubmitArgs &args,
                                    IoUringOperation *operation) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(&operation->msg->header);
    sqe.len = 1U;
    sqe.msg_flags = args.op_flags;
    if (args.opcode == IORING_OP_RECVMSG && args.multishot) {
      sqe.ioprio |= IORING_RECV_MULTISHOT;
    }
  }

  static void
  fill_io_uring_generic_accept_sqe(io_uring_sqe &sqe,
                                   const IoUringGenericSubmitArgs &args,
                                   IoUringOperation *operation) noexcept {
    if (operation->socket_address != nullptr) {
      sqe.addr =
          reinterpret_cast<std::uint64_t>(&operation->socket_address->storage);
      sqe.addr2 =
          reinterpret_cast<std::uint64_t>(&operation->socket_address->size);
    }
    sqe.accept_flags = args.op_flags;
    if (args.multishot) {
      sqe.ioprio |= IORING_ACCEPT_MULTISHOT;
    }
  }

  static void
  fill_io_uring_generic_connect_sqe(io_uring_sqe &sqe,
                                    IoUringOperation *operation) noexcept {
    sqe.addr =
        reinterpret_cast<std::uint64_t>(&operation->socket_address->storage);
    sqe.off = operation->socket_address->size;
  }

  static void fill_io_uring_generic_socket_data_sqe(
      io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.msg_flags = args.op_flags;
    if (args.opcode == IORING_OP_RECV && args.multishot) {
      sqe.ioprio |= IORING_RECV_MULTISHOT;
    }
  }
#endif
#if defined(__linux__)
  static void fill_io_uring_generic_fixed_buffer_sqe(
      io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
    sqe.buf_index = args.fixed_buffer_index;
  }

  static void fill_io_uring_generic_buffer_sqe(
      io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
  }
#endif
#if defined(__linux__)
  [[nodiscard]] bool submit_io_uring_op(
      std::uint8_t opcode, int fd, void *data, std::size_t size,
      std::uint64_t offset, std::uint32_t op_flags,
      std::uint32_t complete_events, Task *task, IoResult *result,
      sockaddr *message_name = nullptr, socklen_t message_name_len = 0,
      socklen_t *message_name_len_out = nullptr,
      const sockaddr *socket_address = nullptr,
      socklen_t socket_address_size = 0, sockaddr *socket_address_out = nullptr,
      socklen_t *socket_address_size_out = nullptr,
      const iovec *message_iov = nullptr, std::size_t message_iov_count = 0,
      std::uint64_t extra = 0, std::int32_t extra_fd = -1,
      std::uint16_t fixed_buffer_index = 0, bool fixed_file = false,
      bool multishot = false, bool zero_copy_send = false,
      std::uint16_t provided_buffer_group = 0, bool buffer_select = false,
      int direct_file_index = -1) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring submit must be called from its IO thread");
    if (result != nullptr) {
      result->completion_token = nullptr;
    }

    const IoUringGenericSubmitArgs args{opcode,
                                        fd,
                                        data,
                                        size,
                                        offset,
                                        op_flags,
                                        complete_events,
                                        task,
                                        result,
                                        message_name,
                                        message_name_len,
                                        message_name_len_out,
                                        socket_address,
                                        socket_address_size,
                                        socket_address_out,
                                        socket_address_size_out,
                                        message_iov,
                                        message_iov_count,
                                        extra,
                                        extra_fd,
                                        fixed_buffer_index,
                                        fixed_file,
                                        multishot,
                                        zero_copy_send,
                                        provided_buffer_group,
                                        buffer_select,
                                        direct_file_index};
    const IoUringGenericSubmitKind kind =
        classify_io_uring_generic_submit(args);
    if (!validate_io_uring_generic_submit(args, kind)) {
      return false;
    }

    IoUringOperation *operation =
        create_io_uring_generic_submit_operation(args, kind);
    if (operation == nullptr) {
      return false;
    }

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
      destroy_io_uring_operation(operation);
      set_io_uring_generic_submit_error(
          result, fd, reserve_error == 0 ? EBUSY : reserve_error);
      return false;
    }

    track_io_uring_operation(operation);
    fill_io_uring_generic_submit_sqe(*sqe, args, kind, operation);

    result->fd = fd;
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
  }
#endif

#if AF_DETAIL_HAS_EPOLL
#if AF_DETAIL_HAS_EPOLL
  [[nodiscard]] bool native_io_backend_available() const noexcept {
    return io_epoll_fd_ >= 0;
  }

  [[nodiscard]] bool notify_native_io_backend() noexcept {
    if (io_epoll_fd_ < 0) {
      return false;
    }
    bool expected = false;
    if (io_wake_pending_.compare_exchange_strong(expected, true,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire)) {
      const std::uint64_t value = 1;
      const auto written = ::write(io_wake_fd_, &value, sizeof(value));
      static_cast<void>(written);
    }
    return true;
  }

  [[nodiscard]] bool init_native_io_backend() noexcept {
    if (!native_io_thread()) {
      return false;
    }
    if (io_epoll_fd_ >= 0) {
      return true;
    }
    reserve_io_backend_storage();

    io_epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (io_epoll_fd_ < 0) {
      return false;
    }

    io_wake_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (io_wake_fd_ < 0) {
      close_native_io_backend();
      return false;
    }

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.ptr = nullptr;
    if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, io_wake_fd_, &event) != 0) {
      close_native_io_backend();
      return false;
    }
    return true;
  }

  void close_native_io_backend() noexcept {
    clear_io_waits();
    if (io_wake_fd_ >= 0) {
      ::close(io_wake_fd_);
      io_wake_fd_ = -1;
    }
    if (io_epoll_fd_ >= 0) {
      ::close(io_epoll_fd_);
      io_epoll_fd_ = -1;
    }
    io_wake_pending_.store(false, std::memory_order_relaxed);
  }
  void clear_io_waits() noexcept {
    for (auto &entry : io_waits_) {
      io_wait_pool_.destroy(entry.second);
    }
    io_waits_.clear();
  }
  [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept {
    if (io_epoll_fd_ < 0) {
      return did_work;
    }
    if (timeout_ms == 0 && io_waits_.empty()) {
      return did_work;
    }

    std::array<epoll_event, 64> events;
    const int count = ::epoll_wait(io_epoll_fd_, events.data(),
                                   static_cast<int>(events.size()), timeout_ms);
    if (count <= 0) {
      return did_work;
    }

    for (int i = 0; i < count; ++i) {
      auto *registration = static_cast<IoWaitRegistration *>(
          events[static_cast<std::size_t>(i)].data.ptr);
      if (registration == nullptr) {
        drain_io_wake();
        if (poll_io_uring_completions()) {
          did_work = true;
        }
        did_work = true;
        continue;
      }

      const int fd = registration->fd;
      io_waits_.erase(fd);
      static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));

      registration->result->fd = fd;
      registration->result->events =
          io_events_from_native(events[static_cast<std::size_t>(i)].events);
      registration->result->error = 0;
      enqueue_pending_blocking(index_, registration->task);
      io_wait_pool_.destroy(registration);
      did_work = true;
    }
    return did_work;
  }
  [[nodiscard]] bool register_native_io_wait(int fd, std::uint32_t events,
                                             Task *task, IoResult *result,
                                             bool prefer_rearm) noexcept {
    static_cast<void>(prefer_rearm);
    if (io_epoll_fd_ < 0 || fd < 0 || events == 0U ||
        io_waits_.find(fd) != io_waits_.end()) {
      result->fd = fd;
      result->events = io_error;
      if (fd < 0) {
        result->error = EBADF;
      } else if (events == 0U) {
        result->error = EINVAL;
      } else if (io_epoll_fd_ < 0) {
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
    registration->poll_operation = nullptr;

    const IoUringPollSubmitResult poll_result =
        try_submit_io_uring_poll_wait(fd, events, task, result, registration);
    if (poll_result == IoUringPollSubmitResult::Submitted) {
      *result = IoResult{fd, 0, 0};
      return true;
    }
    if (poll_result == IoUringPollSubmitResult::Failed) {
      io_waits_.erase(fd);
      io_wait_pool_.destroy(registration);
      return false;
    }
    if (poll_result == IoUringPollSubmitResult::BackendClosed) {
      return false;
    }

    std::uint32_t native_events = EPOLLERR | EPOLLHUP | EPOLLONESHOT;
    if ((events & io_readable) != 0U) {
      native_events |= EPOLLIN;
    }
    if ((events & io_writable) != 0U) {
      native_events |= EPOLLOUT;
    }

    epoll_event event{};
    event.events = native_events;
    event.data.ptr = registration;

    if (::epoll_ctl(io_epoll_fd_, EPOLL_CTL_ADD, fd, &event) != 0) {
      const int first_error = errno;
      if (first_error != EEXIST ||
          ::epoll_ctl(io_epoll_fd_, EPOLL_CTL_MOD, fd, &event) != 0) {
        io_waits_.erase(fd);
        io_wait_pool_.destroy(registration);
        result->fd = fd;
        result->events = io_error;
        result->error = errno;
        return false;
      }
    }

    *result = IoResult{fd, 0, 0};
    return true;
  }
  [[nodiscard]] bool cancel_native_io_wait(IoOpState &state) noexcept {
    if (io_epoll_fd_ < 0) {
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
    if (registration->poll_operation != nullptr) {
      IoUringOperation *operation = registration->poll_operation;
      const int submit_error = submit_io_uring_cancel(operation);
      if (submit_error != 0) {
        state.wait.events = io_error;
        state.wait.error = submit_error;
        state.wait.result = -submit_error;
        return false;
      }

      io_waits_.erase(it);
      registration->poll_operation = nullptr;
      if (operation->wait_registration == registration) {
        operation->wait_registration = nullptr;
      }
      operation->cancel_requested = true;
      operation->task = nullptr;
      operation->result = nullptr;

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
    io_waits_.erase(it);
    static_cast<void>(::epoll_ctl(io_epoll_fd_, EPOLL_CTL_DEL, fd, nullptr));

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
#endif
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
    if (!io_wake_pending_.compare_exchange_strong(expected, true,
                                                  std::memory_order_acq_rel,
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
    EV_SET(&event, kqueue_wake_ident, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0,
           nullptr);
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
  [[nodiscard]] static intptr_t
  kqueue_timeout_data(std::chrono::nanoseconds timeout) noexcept {
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

  [[nodiscard]] static intptr_t
  clamp_kqueue_timer_value(std::int64_t value) noexcept {
    constexpr auto max_value =
        static_cast<std::uint64_t>(std::numeric_limits<intptr_t>::max());
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

  void
  untrack_kqueue_timeout(KqueueTimeoutRegistration *registration) noexcept {
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
  [[nodiscard]] bool submit_kqueue_timeout(std::chrono::nanoseconds timeout,
                                           Task *task,
                                           IoResult *result) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "kqueue timeout submit must run on its IO thread");
    if (result != nullptr) {
      result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr ||
        result == nullptr || timeout.count() <= 0) {
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
           kqueue_timeout_unit_flags(), kqueue_timeout_data(timeout),
           registration);
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

    auto *registration =
        static_cast<KqueueTimeoutRegistration *>(state.wait.completion_token);
    if (registration == nullptr || registration->result != &state.wait) {
      state.wait.events = io_error;
      state.wait.error = ENOENT;
      state.wait.result = -ENOENT;
      return false;
    }

    struct kevent event{};
    EV_SET(&event, registration->ident, EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
    if (::kevent(io_kqueue_fd_, &event, 1, nullptr, 0, nullptr) != 0 &&
        errno != ENOENT) {
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

  [[nodiscard]] bool
  complete_kqueue_timeout(KqueueTimeoutRegistration *registration,
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
    if (timeout_ms == 0 && io_waits_.empty() &&
        io_kqueue_timeout_count_ == 0U &&
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
      }
    } catch (...) {
    }
  }
  [[nodiscard]] bool register_native_io_wait(int fd, std::uint32_t events,
                                             Task *task, IoResult *result,
                                             bool prefer_rearm) noexcept {
    static_cast<void>(prefer_rearm);
    const bool unsupported_events =
        (events & (io_readable | io_writable)) == 0U;
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
    if (::kevent(io_kqueue_fd_, changes.data(), change_count, nullptr, 0,
                 nullptr) != 0) {
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

  [[nodiscard]] static int
  fill_kqueue_changes(int fd, std::uint32_t events,
                      IoWaitRegistration *registration,
                      std::array<struct kevent, 2> &changes) noexcept {
    int count = 0;
    if ((events & io_readable) != 0U) {
      EV_SET(&changes[static_cast<std::size_t>(count++)],
             static_cast<uintptr_t>(fd), EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0,
             registration);
    }
    if ((events & io_writable) != 0U) {
      EV_SET(&changes[static_cast<std::size_t>(count++)],
             static_cast<uintptr_t>(fd), EVFILT_WRITE, EV_ADD | EV_ONESHOT, 0,
             0, registration);
    }
    return count;
  }

  void remove_kqueue_filters(const IoWaitRegistration &registration) noexcept {
    std::array<struct kevent, 2> changes;
    int count = 0;
    if ((registration.events & io_readable) != 0U) {
      EV_SET(&changes[static_cast<std::size_t>(count++)],
             static_cast<uintptr_t>(registration.fd), EVFILT_READ, EV_DELETE, 0,
             0, nullptr);
    }
    if ((registration.events & io_writable) != 0U) {
      EV_SET(&changes[static_cast<std::size_t>(count++)],
             static_cast<uintptr_t>(registration.fd), EVFILT_WRITE, EV_DELETE,
             0, 0, nullptr);
    }
    if (count != 0) {
      static_cast<void>(
          ::kevent(io_kqueue_fd_, changes.data(), count, nullptr, 0, nullptr));
    }
  }
  [[nodiscard]] static std::uint32_t
  io_events_from_kqueue(const struct kevent &event) noexcept {
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

  [[nodiscard]] static int
  io_error_from_kqueue(const struct kevent &event) noexcept {
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

  [[nodiscard]] bool notify_native_io_backend() noexcept { return false; }

  [[nodiscard]] bool init_native_io_backend() noexcept { return false; }

  void close_native_io_backend() noexcept {}

  [[nodiscard]] bool poll_native_io(int timeout_ms, bool did_work) noexcept {
    static_cast<void>(timeout_ms);
    return did_work;
  }

  [[nodiscard]] bool register_native_io_wait(int fd, std::uint32_t events,
                                             Task *task, IoResult *result,
                                             bool prefer_rearm) noexcept {
    static_cast<void>(events);
    static_cast<void>(task);
    static_cast<void>(prefer_rearm);
    result->fd = fd;
    result->events = io_error;
    result->error = fd < 0 ? EBADF : ENOSYS;
    return false;
  }
#endif

  void notify_force() noexcept {
    wake_epoch_.fetch_add(1, std::memory_order_release);
    if (notify_native_io_backend()) {
      return;
    }
    wake_epoch_.notify_one();
  }

  void run_loop() noexcept {
    RuntimeT::current_thread_index_ = index_;

    for (;;) {
      bool did_work = false;
      while (Task *task = pop_one()) {
        did_work = true;
        execute(task);
      }

#if defined(__linux__)
      if (flush_io_uring_submissions_or_fail()) {
        did_work = true;
      }
#endif

      if (stop_requested_.load(std::memory_order_acquire)) {
        if (!did_work) {
          break;
        }
        continue;
      }

      if (poll_io(0)) {
        continue;
      }

      const std::uint32_t observed =
          wake_epoch_.load(std::memory_order_acquire);
      sleeping_.store(true, std::memory_order_release);
      if (stop_requested_.load(std::memory_order_acquire)) {
        sleeping_.store(false, std::memory_order_relaxed);
        continue;
      }

      if (Task *task = pop_one()) {
        sleeping_.store(false, std::memory_order_relaxed);
        execute(task);
      } else {
        if (wake_epoch_.load(std::memory_order_acquire) != observed) {
          sleeping_.store(false, std::memory_order_relaxed);
          continue;
        }
        if (io_thread() && io_backend_available()) {
          static_cast<void>(poll_io(-1));
        } else {
          wake_epoch_.wait(observed, std::memory_order_acquire);
        }
        sleeping_.store(false, std::memory_order_relaxed);
      }
    }

    RuntimeT::current_thread_index_ = invalid_thread_index;
  }

  void init_io_backend() noexcept {
    if (!io_thread() || native_io_backend_available()) {
      return;
    }
    if (!init_native_io_backend()) {
#if defined(__linux__)
      if (io_uring_thread()) {
        io_uring_backend_error_ = ENODEV;
      }
#endif
      return;
    }
#if defined(__linux__)
    if (io_uring_thread()) {
      init_io_uring_backend();
    }
#endif
  }

  void close_io_backend() noexcept {
#if defined(__linux__)
    close_io_uring_backend();
#endif
    close_native_io_backend();
  }

  [[nodiscard]] bool poll_io(int timeout_ms) noexcept {
#if defined(__linux__)
    bool did_work = poll_io_uring_completions();
#else
    bool did_work = false;
#endif
    return poll_native_io(timeout_ms, did_work);
  }

#if defined(__linux__)
  void detect_io_uring_features() noexcept {
    io_uring_send_zc_available_ = false;
    io_uring_sendmsg_zc_available_ = false;
    io_uring_poll_add_available_ = false;
    io_uring_socket_available_ = false;

    constexpr unsigned probe_count = 64;
    std::array<std::byte,
               sizeof(io_uring_probe) + probe_count * sizeof(io_uring_probe_op)>
        storage{};
    auto *probe = reinterpret_cast<io_uring_probe *>(storage.data());
    if (detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_PROBE,
                                      probe, probe_count) != 0) {
      return;
    }

    const auto *ops = reinterpret_cast<const io_uring_probe_op *>(
        storage.data() + sizeof(io_uring_probe));
    const unsigned op_count = std::min<unsigned>(probe->ops_len, probe_count);
    for (unsigned i = 0; i < op_count; ++i) {
      if (ops[i].op == detail::io_uring_op_send_zc &&
          (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
        io_uring_send_zc_available_ = true;
      } else if (ops[i].op == detail::io_uring_op_sendmsg_zc &&
                 (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
        io_uring_sendmsg_zc_available_ = true;
      } else if (ops[i].op == IORING_OP_POLL_ADD &&
                 (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
        io_uring_poll_add_available_ = true;
      } else if (ops[i].op == detail::io_uring_op_socket &&
                 (ops[i].flags & IO_URING_OP_SUPPORTED) != 0U) {
        io_uring_socket_available_ = true;
      }
    }
  }
#endif
#if defined(__linux__)
  void close_io_uring_backend() noexcept {
    clear_io_uring_operations();
    if (io_uring_fd_ >= 0 && io_uring_files_registered_) {
      static_cast<void>(detail::sys_io_uring_register(
          io_uring_fd_, IORING_UNREGISTER_FILES, nullptr, 0));
    }
    if (io_uring_fd_ >= 0 && io_uring_buffers_registered_) {
      static_cast<void>(detail::sys_io_uring_register(
          io_uring_fd_, IORING_UNREGISTER_BUFFERS, nullptr, 0));
    }
    if (io_uring_sqes_ != nullptr && io_uring_sqes_ != MAP_FAILED) {
      ::munmap(io_uring_sqes_, io_uring_sqes_size_);
    }
    if (io_uring_sq_ring_ != nullptr && io_uring_sq_ring_ != MAP_FAILED) {
      ::munmap(io_uring_sq_ring_, io_uring_sq_ring_size_);
    }
    if (io_uring_cq_ring_ != nullptr && io_uring_cq_ring_ != MAP_FAILED &&
        io_uring_cq_ring_ != io_uring_sq_ring_) {
      ::munmap(io_uring_cq_ring_, io_uring_cq_ring_size_);
    }
    if (io_uring_fd_ >= 0) {
      ::close(io_uring_fd_);
    }

    io_uring_fd_ = -1;
    io_uring_backend_error_ = 0;
    io_uring_sq_ring_ = nullptr;
    io_uring_cq_ring_ = nullptr;
    io_uring_sqes_ = nullptr;
    io_uring_sq_ring_size_ = 0;
    io_uring_cq_ring_size_ = 0;
    io_uring_sqes_size_ = 0;
    io_uring_sq_head_ = nullptr;
    io_uring_sq_tail_ = nullptr;
    io_uring_sq_ring_mask_ = nullptr;
    io_uring_sq_ring_entries_ = nullptr;
    io_uring_sq_array_ = nullptr;
    io_uring_cq_head_ = nullptr;
    io_uring_cq_tail_ = nullptr;
    io_uring_cq_ring_mask_ = nullptr;
    io_uring_cqes_ = nullptr;
    io_uring_pending_submissions_ = 0;
    io_uring_send_zc_available_ = false;
    io_uring_sendmsg_zc_available_ = false;
    io_uring_poll_add_available_ = false;
    io_uring_socket_available_ = false;
    io_uring_buffers_registered_ = false;
    io_uring_registered_buffer_count_ = 0;
    io_uring_provided_buffer_groups_.clear();
    io_uring_files_registered_ = false;
    io_uring_registered_file_count_ = 0;
  }
#endif
#if defined(__linux__)
  template <typename T>
  [[nodiscard]] static T *ptr_at(std::byte *base,
                                 std::uint32_t offset) noexcept {
    return reinterpret_cast<T *>(base + offset);
  }

  [[nodiscard]] static unsigned io_uring_requested_setup_flags() noexcept {
    unsigned requested_setup_flags = io_uring_setup_flags;
    if constexpr (io_uring_setup_sqpoll || io_uring_sqpoll_cpu >= 0) {
      requested_setup_flags |= IORING_SETUP_SQPOLL;
    }
    if constexpr (io_uring_setup_submit_all) {
      requested_setup_flags |= IORING_SETUP_SUBMIT_ALL;
    }
    if constexpr (io_uring_setup_coop_taskrun) {
      requested_setup_flags |= IORING_SETUP_COOP_TASKRUN;
    }
    if constexpr (io_uring_setup_single_issuer ||
                  io_uring_setup_defer_taskrun) {
      requested_setup_flags |= IORING_SETUP_SINGLE_ISSUER;
    }
    if constexpr (io_uring_setup_defer_taskrun) {
      requested_setup_flags |= IORING_SETUP_DEFER_TASKRUN;
    }
    return requested_setup_flags;
  }

  [[nodiscard]] bool
  map_io_uring_rings(const io_uring_params &params) noexcept {
    const std::size_t sq_ring_size =
        params.sq_off.array +
        static_cast<std::size_t>(params.sq_entries) * sizeof(std::uint32_t);
    const std::size_t cq_ring_size =
        params.cq_off.cqes +
        static_cast<std::size_t>(params.cq_entries) * sizeof(io_uring_cqe);

    if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0U) {
      io_uring_sq_ring_size_ = std::max(sq_ring_size, cq_ring_size);
      io_uring_cq_ring_size_ = io_uring_sq_ring_size_;
      io_uring_sq_ring_ = static_cast<std::byte *>(
          ::mmap(nullptr, io_uring_sq_ring_size_, PROT_READ | PROT_WRITE,
                 MAP_SHARED | MAP_POPULATE, io_uring_fd_, IORING_OFF_SQ_RING));
      io_uring_cq_ring_ = io_uring_sq_ring_;
    } else {
      io_uring_sq_ring_size_ = sq_ring_size;
      io_uring_cq_ring_size_ = cq_ring_size;
      io_uring_sq_ring_ = static_cast<std::byte *>(
          ::mmap(nullptr, io_uring_sq_ring_size_, PROT_READ | PROT_WRITE,
                 MAP_SHARED | MAP_POPULATE, io_uring_fd_, IORING_OFF_SQ_RING));
      io_uring_cq_ring_ = static_cast<std::byte *>(
          ::mmap(nullptr, io_uring_cq_ring_size_, PROT_READ | PROT_WRITE,
                 MAP_SHARED | MAP_POPULATE, io_uring_fd_, IORING_OFF_CQ_RING));
    }

    io_uring_sqes_size_ =
        static_cast<std::size_t>(params.sq_entries) * sizeof(io_uring_sqe);
    io_uring_sqes_ = static_cast<io_uring_sqe *>(
        ::mmap(nullptr, io_uring_sqes_size_, PROT_READ | PROT_WRITE,
               MAP_SHARED | MAP_POPULATE, io_uring_fd_, IORING_OFF_SQES));

    return io_uring_sq_ring_ != MAP_FAILED && io_uring_cq_ring_ != MAP_FAILED &&
           io_uring_sqes_ != MAP_FAILED;
  }

  void bind_io_uring_ring_pointers(const io_uring_params &params) noexcept {
    io_uring_sq_head_ =
        ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.head);
    io_uring_sq_tail_ =
        ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.tail);
    io_uring_sq_ring_mask_ =
        ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.ring_mask);
    io_uring_sq_ring_entries_ =
        ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.ring_entries);
    io_uring_sq_array_ =
        ptr_at<std::uint32_t>(io_uring_sq_ring_, params.sq_off.array);
    io_uring_cq_head_ =
        ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.head);
    io_uring_cq_tail_ =
        ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.tail);
    io_uring_cq_ring_mask_ =
        ptr_at<std::uint32_t>(io_uring_cq_ring_, params.cq_off.ring_mask);
    io_uring_cqes_ =
        ptr_at<io_uring_cqe>(io_uring_cq_ring_, params.cq_off.cqes);
  }

  [[nodiscard]] bool register_io_uring_wake_fd() noexcept {
    return detail::sys_io_uring_register(io_uring_fd_, IORING_REGISTER_EVENTFD,
                                         &io_wake_fd_, 1) == 0;
  }
#endif
#if defined(__linux__)
  void init_io_uring_backend() noexcept {
    if (io_uring_fd_ >= 0) {
      return;
    }
    if (io_wake_fd_ < 0) {
      io_uring_backend_error_ = ENODEV;
      return;
    }

    io_uring_backend_error_ = 0;
    io_uring_params params{};
    detail::configure_io_uring_params(
        params, detail::IoUringSetupRequest{
                    io_uring_requested_setup_flags(), io_uring_cq_entries,
                    io_uring_sqpoll_idle_ms, io_uring_sqpoll_cpu});
    io_uring_fd_ = detail::sys_io_uring_setup(io_uring_entries, &params);
    if (io_uring_fd_ < 0) {
      io_uring_backend_error_ = errno == 0 ? EIO : errno;
      return;
    }

    if (!map_io_uring_rings(params)) {
      const int map_error = errno == 0 ? EIO : errno;
      close_io_uring_backend();
      io_uring_backend_error_ = map_error;
      return;
    }

    bind_io_uring_ring_pointers(params);
    detect_io_uring_features();

    if (!register_io_uring_wake_fd()) {
      const int register_error = errno == 0 ? EIO : errno;
      close_io_uring_backend();
      io_uring_backend_error_ = register_error;
    }
  }
#endif
#if defined(__linux__)
  void reserve_io_backend_storage() noexcept {
    try {
      if constexpr (io_wait_reserve != 0U) {
        io_waits_.reserve(io_wait_reserve);
      }
      if constexpr (io_uring_provided_buffer_group_reserve != 0U) {
        io_uring_provided_buffer_groups_.reserve(
            io_uring_provided_buffer_group_reserve);
      }
    } catch (...) {
    }
  }
#endif
#if defined(__linux__)
  [[nodiscard]] io_uring_sqe *reserve_io_uring_sqe(int &error) noexcept {
    error = 0;
    if (io_uring_fd_ < 0 || io_uring_sq_tail_ == nullptr ||
        io_uring_sq_head_ == nullptr) {
      error = ENOSYS;
      return nullptr;
    }

    std::uint32_t head = __atomic_load_n(io_uring_sq_head_, __ATOMIC_ACQUIRE);
    std::uint32_t tail = __atomic_load_n(io_uring_sq_tail_, __ATOMIC_RELAXED);
    if (tail - head >= *io_uring_sq_ring_entries_ &&
        io_uring_pending_submissions_ != 0U) {
      const int submit_error = flush_io_uring_submissions();
      if (submit_error != 0) {
        error = submit_error;
        fail_io_uring_backend(submit_error, nullptr);
        return nullptr;
      }
      head = __atomic_load_n(io_uring_sq_head_, __ATOMIC_ACQUIRE);
      tail = __atomic_load_n(io_uring_sq_tail_, __ATOMIC_RELAXED);
    }
    if (tail - head >= *io_uring_sq_ring_entries_) {
      error = EBUSY;
      return nullptr;
    }

    const std::uint32_t index = tail & *io_uring_sq_ring_mask_;
    io_uring_sq_array_[index] = index;
    __atomic_store_n(io_uring_sq_tail_, tail + 1U, __ATOMIC_RELEASE);
    ++io_uring_pending_submissions_;
    return &io_uring_sqes_[index];
  }

  [[nodiscard]] int flush_io_uring_submissions() noexcept {
    if (io_uring_pending_submissions_ == 0U) {
      return 0;
    }

    unsigned remaining = io_uring_pending_submissions_;
    while (remaining != 0U) {
      const int submitted =
          detail::sys_io_uring_enter(io_uring_fd_, remaining, 0, 0);
      if (submitted > 0) {
        const auto submitted_count = static_cast<unsigned>(submitted);
        if (submitted_count > remaining) {
          return EIO;
        }
        remaining -= submitted_count;
        continue;
      }
      if (submitted == 0) {
        return EIO;
      }
      if (errno == EINTR) {
        continue;
      }
      return errno == 0 ? EIO : errno;
    }

    io_uring_pending_submissions_ = 0;
    return 0;
  }

  [[nodiscard]] bool flush_io_uring_submissions_or_fail() noexcept {
    const int submit_error = flush_io_uring_submissions();
    if (submit_error == 0) {
      return false;
    }
    fail_io_uring_backend(submit_error, nullptr);
    return true;
  }
#endif
#if defined(__linux__)
  [[nodiscard]] bool poll_io_uring_completions() noexcept {
    if (io_uring_fd_ < 0 || io_uring_cq_head_ == nullptr ||
        io_uring_cq_tail_ == nullptr) {
      return false;
    }

    bool did_work = false;
    std::uint32_t head = __atomic_load_n(io_uring_cq_head_, __ATOMIC_ACQUIRE);
    const std::uint32_t tail =
        __atomic_load_n(io_uring_cq_tail_, __ATOMIC_ACQUIRE);
    while (head != tail) {
      io_uring_cqe &cqe = io_uring_cqes_[head & *io_uring_cq_ring_mask_];
      auto *operation = reinterpret_cast<IoUringOperation *>(cqe.user_data);
      if (operation != nullptr) {
        const bool yield_to_task =
            complete_io_uring_operation(operation, cqe.res, cqe.flags);
        did_work = true;
        ++head;
        if (yield_to_task) {
          break;
        }
        continue;
      }
      ++head;
    }
    __atomic_store_n(io_uring_cq_head_, head, __ATOMIC_RELEASE);
    return did_work;
  }
#endif
#if defined(__linux__)
  [[nodiscard]] bool
  complete_io_uring_operation(IoUringOperation *operation, int result,
                              std::uint32_t cqe_flags) noexcept {
    if (operation->poll_wait) {
      complete_io_uring_poll_wait(operation, result);
      return false;
    }

    if (operation->zero_copy_send && (cqe_flags & IORING_CQE_F_NOTIF) != 0U) {
      operation->zero_copy_notification_done = true;
      if (operation->zero_copy_primary_done) {
        untrack_io_uring_operation(operation);
        destroy_io_uring_operation(operation);
      }
      return false;
    }

    const bool cqe_has_more = operation->multishot && result >= 0 &&
                              (cqe_flags & IORING_CQE_F_MORE) != 0U;
    const bool more = cqe_has_more && !operation->cancel_requested;
    const bool cancel_draining_more =
        cqe_has_more && operation->cancel_requested;
    const bool zero_copy_waits_for_notification =
        operation->zero_copy_send && (cqe_flags & IORING_CQE_F_MORE) != 0U;
    if (!more && !cancel_draining_more && !zero_copy_waits_for_notification) {
      untrack_io_uring_operation(operation);
    }
    operation->result->result = result;
    if (operation->cancel_requested) {
      if (result >= 0) {
        if (io_uring_operation_result_is_fd(operation)) {
          ::close(result);
        } else {
          clear_direct_io_uring_file_slot(operation);
        }
      }
      if (cancel_draining_more) {
        return false;
      }
      operation->result->events = io_error;
      operation->result->error = ECANCELED;
      operation->result->result = -ECANCELED;
    } else if (result < 0) {
      operation->result->events = io_error;
      operation->result->error = -result;
    } else {
      std::uint32_t events = operation->complete_events | (more ? io_more : 0U);
      if ((cqe_flags & IORING_CQE_F_BUFFER) != 0U) {
        events |= io_buffer_selected |
                  ((cqe_flags >> io_buffer_id_shift) << io_buffer_id_shift);
      }
      operation->result->events = events;
      operation->result->error = 0;
      if (operation->msg != nullptr &&
          operation->msg->address_size != nullptr) {
        *operation->msg->address_size = operation->msg->header.msg_namelen;
      }
      if (operation->opcode != IORING_OP_TIMEOUT &&
          operation->socket_address != nullptr &&
          operation->socket_address->output_size != nullptr) {
        const socklen_t actual_size = operation->socket_address->size;
        if (operation->socket_address->output != nullptr &&
            operation->socket_address->output_capacity != 0U) {
          const auto copy_size = static_cast<std::size_t>(std::min(
              actual_size, operation->socket_address->output_capacity));
          std::memcpy(operation->socket_address->output,
                      &operation->socket_address->storage, copy_size);
        }
        *operation->socket_address->output_size = actual_size;
      }
    }
    enqueue_pending_blocking(index_, operation->task);
    if (more) {
      return true;
    }
    if (zero_copy_waits_for_notification) {
      operation->zero_copy_primary_done = true;
      clear_io_uring_result_token(operation);
      operation->task = nullptr;
      operation->result = nullptr;
      if (operation->zero_copy_notification_done) {
        untrack_io_uring_operation(operation);
        destroy_io_uring_operation(operation);
      }
      return true;
    }
    destroy_io_uring_operation(operation);
    return false;
  }
#endif
#if defined(__linux__)
  void complete_io_uring_poll_wait(IoUringOperation *operation,
                                   int result) noexcept {
    IoWaitRegistration *registration = operation->wait_registration;
    if (registration == nullptr || operation->task == nullptr ||
        operation->result == nullptr) {
      untrack_io_uring_operation(operation);
      destroy_io_uring_operation(operation);
      return;
    }

    const int fd = registration->fd;
    auto it = io_waits_.find(fd);
    if (it != io_waits_.end() && it->second == registration) {
      io_waits_.erase(it);
    }

    registration->result->fd = fd;
    registration->result->result = result;
    if (operation->cancel_requested) {
      registration->result->events = io_error;
      registration->result->error = ECANCELED;
      registration->result->result = -ECANCELED;
    } else if (result < 0) {
      registration->result->events = io_error;
      registration->result->error = -result;
    } else {
      registration->result->events =
          io_events_from_poll(static_cast<std::uint32_t>(result));
      registration->result->error = 0;
    }

    enqueue_pending_blocking(index_, registration->task);
    registration->poll_operation = nullptr;
    operation->wait_registration = nullptr;
    untrack_io_uring_operation(operation);
    destroy_io_uring_operation(operation);
    io_wait_pool_.destroy(registration);
  }
#endif
#if defined(__linux__)
  [[nodiscard]] static bool
  io_uring_result_is_fd(std::uint8_t opcode) noexcept {
    return opcode == IORING_OP_OPENAT || opcode == IORING_OP_ACCEPT ||
           opcode == detail::io_uring_op_openat2 ||
           opcode == detail::io_uring_op_socket;
  }

  [[nodiscard]] static bool
  io_uring_operation_result_is_fd(const IoUringOperation *operation) noexcept {
    return operation != nullptr && operation->direct_file_index < 0 &&
           io_uring_result_is_fd(operation->opcode);
  }

  void
  clear_direct_io_uring_file_slot(const IoUringOperation *operation) noexcept {
    if (operation == nullptr || operation->direct_file_index < 0 ||
        !io_uring_result_is_fd(operation->opcode) || io_uring_fd_ < 0 ||
        !io_uring_files_registered_) {
      return;
    }
    const int invalid_fd = -1;
    io_uring_files_update update{};
    update.offset = static_cast<unsigned>(operation->direct_file_index);
    update.fds = reinterpret_cast<std::uint64_t>(&invalid_fd);
    static_cast<void>(detail::sys_io_uring_register(
        io_uring_fd_, IORING_REGISTER_FILES_UPDATE, &update, 1));
  }

  [[nodiscard]] int
  submit_io_uring_cancel(IoUringOperation *operation) noexcept {
    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
      return reserve_error == 0 ? EBUSY : reserve_error;
    }

    *sqe = io_uring_sqe{};
    sqe->opcode = IORING_OP_ASYNC_CANCEL;
    sqe->fd = -1;
    sqe->addr = reinterpret_cast<std::uint64_t>(operation);
    sqe->cancel_flags = 0;
    sqe->user_data = 0;

    const int submit_error = flush_io_uring_submissions();
    if (submit_error != 0) {
      fail_io_uring_backend(submit_error, nullptr);
    }
    return submit_error;
  }
#endif
#if defined(__linux__)
  void track_io_uring_operation(IoUringOperation *operation) noexcept {
    operation->prev = nullptr;
    operation->next = io_uring_operations_;
    if (io_uring_operations_ != nullptr) {
      io_uring_operations_->prev = operation;
    }
    io_uring_operations_ = operation;
  }

  void untrack_io_uring_operation(IoUringOperation *operation) noexcept {
    if (operation->prev != nullptr) {
      operation->prev->next = operation->next;
    } else if (io_uring_operations_ == operation) {
      io_uring_operations_ = operation->next;
    }
    if (operation->next != nullptr) {
      operation->next->prev = operation->prev;
    }
    operation->prev = nullptr;
    operation->next = nullptr;
  }

  void clear_io_uring_operations() noexcept {
    IoUringOperation *operation = io_uring_operations_;
    io_uring_operations_ = nullptr;
    while (operation != nullptr) {
      IoUringOperation *next = operation->next;
      operation->prev = nullptr;
      operation->next = nullptr;
      close_pending_io_uring_fd_result(operation);
      destroy_io_uring_operation(operation);
      operation = next;
    }
  }

  void fail_io_uring_backend(int error,
                             IoUringOperation *running_operation) noexcept {
    const int backend_error = error == 0 ? EIO : error;
    clear_or_fail_io_uring_operations(error, running_operation);
    close_io_uring_backend();
    io_uring_backend_error_ = backend_error;
  }

  void clear_or_fail_io_uring_operations(
      int error, IoUringOperation *running_operation) noexcept {
    IoUringOperation *operation = io_uring_operations_;
    io_uring_operations_ = nullptr;
    while (operation != nullptr) {
      IoUringOperation *next = operation->next;
      operation->prev = nullptr;
      operation->next = nullptr;
      if (operation == running_operation) {
        close_pending_io_uring_fd_result(operation);
        destroy_io_uring_operation(operation);
        operation = next;
        continue;
      }

      close_pending_io_uring_fd_result(operation);
      if (operation->task == nullptr || operation->result == nullptr) {
        destroy_io_uring_operation(operation);
        operation = next;
        continue;
      }
      operation->result->events = io_error;
      operation->result->error =
          operation->cancel_requested ? ECANCELED : error;
      operation->result->result =
          operation->cancel_requested ? -ECANCELED : -error;
      enqueue_pending_blocking(index_, operation->task);
      destroy_io_uring_operation(operation);
      operation = next;
    }
  }

  void close_pending_io_uring_fd_result(IoUringOperation *operation) noexcept {
    if (operation == nullptr || operation->result == nullptr ||
        !io_uring_operation_result_is_fd(operation) ||
        operation->result->error != 0 ||
        (operation->result->events & operation->complete_events) == 0U ||
        operation->result->result < 0) {
      return;
    }
    ::close(static_cast<int>(operation->result->result));
    operation->result->events = io_error;
    operation->result->error = ECANCELED;
    operation->result->result = -ECANCELED;
  }

  static void
  clear_io_uring_result_token(IoUringOperation *operation) noexcept {
    if (operation != nullptr && operation->result != nullptr &&
        operation->result->completion_token == operation) {
      operation->result->completion_token = nullptr;
    }
  }

  void destroy_io_uring_operation(IoUringOperation *operation) noexcept {
    clear_io_uring_result_token(operation);
    if (operation->msg != nullptr) {
      io_uring_msg_pool_.destroy(operation->msg);
      operation->msg = nullptr;
    }
    if (operation->opcode != IORING_OP_TIMEOUT &&
        operation->socket_address != nullptr) {
      io_uring_address_pool_.destroy(operation->socket_address);
      operation->socket_address = nullptr;
    }
    io_uring_op_pool_.destroy(operation);
  }
#endif

#if defined(__linux__)
  void drain_io_wake() noexcept {
    std::uint64_t value = 0;
    while (::read(io_wake_fd_, &value, sizeof(value)) == sizeof(value)) {
    }
    io_wake_pending_.store(false, std::memory_order_release);
  }

  [[nodiscard]] static std::uint32_t
  native_poll_events(std::uint32_t events) noexcept {
    std::uint32_t result = POLLERR | POLLHUP;
    if ((events & io_readable) != 0U) {
      result |= POLLIN;
    }
    if ((events & io_writable) != 0U) {
      result |= POLLOUT;
    }
    return result;
  }

  [[nodiscard]] static std::uint32_t
  io_events_from_poll(std::uint32_t events) noexcept {
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

  [[nodiscard]] static std::uint32_t
  io_events_from_native(std::uint32_t events) noexcept {
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

  void advance_ready_word_cursor_after(std::size_t word) noexcept {
    if constexpr (decltype(ready_sources_)::word_count > 1U) {
      std::size_t next = word + 1U;
      if (next == decltype(ready_sources_)::word_count) {
        next = 0;
      }
      next_ready_word_ = static_cast<std::uint16_t>(next);
    } else {
      static_cast<void>(word);
    }
  }

  Task *pop_one() noexcept {
    if (Task *task = try_pop_local()) {
      return task;
    }

    for (std::size_t checked_word = 0;
         checked_word < decltype(ready_sources_)::word_count; ++checked_word) {
      std::size_t word = checked_word;
      if constexpr (decltype(ready_sources_)::word_count > 1U) {
        word += next_ready_word_;
        if (word >= decltype(ready_sources_)::word_count) {
          word -= decltype(ready_sources_)::word_count;
        }
      }
      std::uint64_t mask = ready_sources_.load_word(word);
      while (mask != 0U) {
        const std::uint16_t source = static_cast<std::uint16_t>(
            decltype(ready_sources_)::word_base(word) + std::countr_zero(mask));
        const std::uint64_t bit = 1ULL << (source & 63U);
        mask &= ~bit;
        if (source == index_) {
          continue;
        }
        if (Task *task = spsc_queue(source, index_).try_pop()) {
          advance_ready_word_cursor_after(word);
          next_source_ =
              static_cast<std::uint16_t>((source + 1U) % thread_count);
          return task;
        }
        ready_sources_.clear(source);
        if (Task *task = spsc_queue(source, index_).try_pop()) {
          advance_ready_word_cursor_after(word);
          next_source_ =
              static_cast<std::uint16_t>((source + 1U) % thread_count);
          mark_ready(source);
          return task;
        }
      }
    }

    for (std::uint16_t checked = 0; checked < thread_count; ++checked) {
      const std::uint16_t source =
          static_cast<std::uint16_t>((next_source_ + checked) % thread_count);
      if (source == index_) {
        continue;
      }
      if (Task *task = spsc_queue(source, index_).try_pop()) {
        next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
        mark_ready(source);
        return task;
      }
    }

    if (external_ready_.load(std::memory_order_acquire)) {
      if (Task *task = RuntimeT::external_queues_[index_]->try_pop()) {
        return task;
      }

      external_ready_.store(false, std::memory_order_release);
      if (Task *task = RuntimeT::external_queues_[index_]->try_pop()) {
        external_ready_.store(true, std::memory_order_release);
        return task;
      }
    }

    return RuntimeT::external_queues_[index_]->try_pop();
  }
  void finish_done(Task *task) noexcept {
    task->state_.store(TaskState::Done, std::memory_order_release);
    static_cast<void>(task->take_requested_thread());
    on_task_finished(task);
    task->release_lifetime_ref();
  }

  void finish_pending(Task *task) noexcept {
    task->state_.store(TaskState::Pending, std::memory_order_release);
    const std::uint16_t requested = task->take_requested_thread();
    if (requested != invalid_thread_index) {
      // A running-task wake request is converted into a real queue entry only
      // if the task is still Pending; a concurrent Pending->Queued wake wins
      // otherwise.
      enqueue_pending_blocking(requested, task);
    }
  }

  void finish_again(Task *task) noexcept {
    task->state_.store(TaskState::Queued, std::memory_order_release);
    static_cast<void>(task->take_requested_thread());
    enqueue_ready_blocking_from_runtime_thread(index_, index_, task);
  }

  std::uint16_t index_;
  ThreadKind kind_{ThreadKind::Worker};
  std::uint16_t next_source_{0};
  std::uint16_t next_ready_word_{0};
  std::vector<Task *> local_queue_;
  std::size_t local_head_{0};
  std::size_t local_tail_{0};
  std::size_t local_size_{0};
  detail::ReadySourceSet<thread_count> ready_sources_;
  CacheLineAtomic<bool> external_ready_{false};
  CacheLineAtomic<std::uint32_t> wake_epoch_{0};
  CacheLineAtomic<bool> sleeping_{false};
  CacheLineAtomic<bool> stop_requested_{false};
  Task *running_task_{nullptr};
#if AF_DETAIL_HAS_NATIVE_IO_WAIT
  absl::flat_hash_map<int, IoWaitRegistration *> io_waits_;
  detail::ObjectPool<IoWaitRegistration> io_wait_pool_;
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
  detail::ObjectPool<KqueueTimeoutRegistration> io_kqueue_timeout_pool_;
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
  detail::ObjectPool<detail::IoUringMessage> io_uring_msg_pool_;
  detail::ObjectPool<detail::IoUringSocketAddress> io_uring_address_pool_;
  detail::ObjectPool<IoUringOperation> io_uring_op_pool_;
#endif
#if AF_DETAIL_HAS_EPOLL || AF_DETAIL_HAS_KQUEUE
  CacheLineAtomic<bool> io_wake_pending_{false};
#endif
  std::thread worker_;
};

} // namespace af::detail
