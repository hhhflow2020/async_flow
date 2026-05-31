#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_public_io_file_data_submit_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_io_file_basic_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_io_file_fixed_file_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_io_file_fixed_buffer_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE

#define AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE 1
#include "af/detail/runtime_public_io_file_vectored_submit_fragment.hpp"
#undef AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE
