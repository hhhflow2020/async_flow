#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_fd_lifecycle_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#define AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_executor_io_uring_open_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_fd_close_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_filesystem_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_splice_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE
