#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_file_data_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#include "af/detail/runtime_executor_io_uring_file_basic_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_timeout_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_file_fixed_file_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_file_fixed_vectored_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_file_fixed_buffer_file_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_file_fixed_buffer_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_file_vectored_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_file_sync_submit_fragment.hpp"
