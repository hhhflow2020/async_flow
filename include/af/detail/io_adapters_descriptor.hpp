#pragma once

template <typename ThreadT> class IoDescriptor {
public:
  constexpr IoDescriptor() noexcept = default;
  constexpr IoDescriptor(ThreadT thread, int fd) noexcept
      : thread_(thread), fd_(fd) {}

  [[nodiscard]] constexpr ThreadT thread() const noexcept { return thread_; }

  [[nodiscard]] constexpr int fd() const noexcept { return fd_; }

  [[nodiscard]] constexpr bool valid() const noexcept { return fd_ >= 0; }

  constexpr void reset(ThreadT thread, int fd) noexcept {
    thread_ = thread;
    fd_ = fd;
  }

#if !defined(_WIN32)
  template <typename TaskT>
  [[nodiscard]] IoStatus setsockopt(TaskT &task, int level, int option,
                                    const void *value,
                                    socklen_t value_size) const noexcept {
    static_assert(
        std::is_same_v<typename TaskT::Thread, ThreadT>,
        "IoDescriptor thread type must match the task runtime thread type");
    return af::io_setsockopt(task, thread_, fd_, level, option, value,
                             value_size);
  }

  template <typename TaskT>
  [[nodiscard]] IoStatus getsockopt(TaskT &task, int level, int option,
                                    void *value,
                                    socklen_t *value_size) const noexcept {
    static_assert(
        std::is_same_v<typename TaskT::Thread, ThreadT>,
        "IoDescriptor thread type must match the task runtime thread type");
    return af::io_getsockopt(task, thread_, fd_, level, option, value,
                             value_size);
  }

  template <typename TaskT>
  [[nodiscard]] IoStatus getsockname(TaskT &task, sockaddr *address,
                                     socklen_t *address_size) const noexcept {
    static_assert(
        std::is_same_v<typename TaskT::Thread, ThreadT>,
        "IoDescriptor thread type must match the task runtime thread type");
    return af::io_getsockname(task, thread_, fd_, address, address_size);
  }

  template <typename TaskT>
  [[nodiscard]] IoStatus getpeername(TaskT &task, sockaddr *address,
                                     socklen_t *address_size) const noexcept {
    static_assert(
        std::is_same_v<typename TaskT::Thread, ThreadT>,
        "IoDescriptor thread type must match the task runtime thread type");
    return af::io_getpeername(task, thread_, fd_, address, address_size);
  }
#endif

protected:
  ThreadT thread_{};
  int fd_{-1};
};
