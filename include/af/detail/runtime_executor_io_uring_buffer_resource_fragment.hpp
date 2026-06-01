#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_buffer_resource_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if !defined(_WIN32)
#include "af/detail/runtime_executor_io_uring_buffer_register_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_buffer_unregister_fragment.hpp"
#endif
