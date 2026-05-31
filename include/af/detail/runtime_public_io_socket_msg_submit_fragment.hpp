#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_socket_msg_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_io_socket_msg_recv_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_io_socket_msg_send_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE
