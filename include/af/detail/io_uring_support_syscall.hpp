#if !defined(AF_IO_URING_SUPPORT_DETAIL_INCLUDE)
#error                                                                         \
    "io_uring_support_syscall.hpp is internal to af/detail/io_uring_support.hpp"
#endif

namespace af::detail {

[[nodiscard]] inline int sys_io_uring_setup(unsigned entries,
                                            io_uring_params *params) noexcept {
  return static_cast<int>(::syscall(__NR_io_uring_setup, entries, params));
}

[[nodiscard]] inline int sys_io_uring_enter(int ring_fd, unsigned to_submit,
                                            unsigned min_complete,
                                            unsigned flags) noexcept {
  return static_cast<int>(::syscall(__NR_io_uring_enter, ring_fd, to_submit,
                                    min_complete, flags, nullptr, 0));
}

[[nodiscard]] inline int sys_io_uring_register(int ring_fd, unsigned opcode,
                                               const void *arg,
                                               unsigned nr_args) noexcept {
  return static_cast<int>(
      ::syscall(__NR_io_uring_register, ring_fd, opcode, arg, nr_args));
}

inline void
configure_io_uring_params(io_uring_params &params,
                          const IoUringSetupRequest &request) noexcept {
  params.flags = request.flags;
  if (request.cq_entries != 0U) {
    params.flags |= IORING_SETUP_CQSIZE;
    params.cq_entries = request.cq_entries;
  }
  if ((params.flags & IORING_SETUP_SQPOLL) != 0U) {
    params.sq_thread_idle = request.sqpoll_idle_ms;
    if (request.sqpoll_cpu >= 0) {
      params.flags |= IORING_SETUP_SQ_AFF;
      params.sq_thread_cpu = static_cast<unsigned>(request.sqpoll_cpu);
    }
  }
}

} // namespace af::detail
