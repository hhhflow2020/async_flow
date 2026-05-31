#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_buffer_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#include "af/detail/runtime_executor_io_uring_buffer_op_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_fast_sqe_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_socket_create_core_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_fixed_file_rw_submit_fragment.hpp"
