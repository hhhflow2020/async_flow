#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "io_rpc_length_prefixed_runtime.hpp"

#if defined(__linux__)

namespace io_rpc_length_prefixed_example {

#define IO_RPC_LENGTH_PREFIXED_SERVER_DETAIL_INCLUDE 1
#include "io_rpc_length_prefixed_process_task_decl.hpp"
#include "io_rpc_length_prefixed_server_task.hpp"
#include "io_rpc_length_prefixed_process_task_impl.hpp"
#undef IO_RPC_LENGTH_PREFIXED_SERVER_DETAIL_INCLUDE

} // namespace io_rpc_length_prefixed_example

#endif
