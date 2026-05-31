#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_native_io_backend_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if AF_DETAIL_HAS_EPOLL
#include "af/detail/runtime_executor_epoll_backend_fragment.hpp"
#elif AF_DETAIL_HAS_KQUEUE
#include "af/detail/runtime_executor_kqueue_backend_fragment.hpp"
#else
#include "af/detail/runtime_executor_noop_io_backend_fragment.hpp"
#endif
