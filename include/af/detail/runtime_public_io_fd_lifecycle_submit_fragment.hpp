#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_fd_lifecycle_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_io_fd_open_submit_fragment.hpp"
#include "af/detail/runtime_public_io_fd_close_submit_fragment.hpp"
#include "af/detail/runtime_public_io_filesystem_submit_fragment.hpp"
#include "af/detail/runtime_public_io_splice_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE
