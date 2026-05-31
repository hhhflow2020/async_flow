#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_socket_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#include "af/detail/runtime_executor_io_uring_socket_recv_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_socket_multishot_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_socket_send_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_socket_zc_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_socket_msg_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_socket_accept_connect_submit_fragment.hpp"
#include "af/detail/runtime_executor_io_uring_socket_create_submit_fragment.hpp"
