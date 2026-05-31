#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_epoll_backend_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if AF_DETAIL_HAS_EPOLL
#include "af/detail/runtime_executor_epoll_setup_fragment.hpp"
#include "af/detail/runtime_executor_epoll_storage_fragment.hpp"
#include "af/detail/runtime_executor_epoll_poll_fragment.hpp"
#include "af/detail/runtime_executor_epoll_wait_fragment.hpp"
#include "af/detail/runtime_executor_epoll_cancel_fragment.hpp"
#endif
