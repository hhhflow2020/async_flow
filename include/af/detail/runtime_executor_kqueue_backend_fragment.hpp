#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_kqueue_backend_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if AF_DETAIL_HAS_KQUEUE
#include "af/detail/runtime_executor_kqueue_setup_fragment.hpp"
#include "af/detail/runtime_executor_kqueue_timeout_fragment.hpp"
#include "af/detail/runtime_executor_kqueue_poll_fragment.hpp"
#include "af/detail/runtime_executor_kqueue_storage_fragment.hpp"
#include "af/detail/runtime_executor_kqueue_wait_fragment.hpp"
#include "af/detail/runtime_executor_kqueue_event_helpers_fragment.hpp"
#endif
