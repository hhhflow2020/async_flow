#pragma once

#include "af/io_common.hpp"

namespace af {

// clang-format off
#include "af/detail/io_socket_lifecycle.hpp"
#include "af/detail/io_socket_accept_connect.hpp"
#include "af/detail/io_socket_recv.hpp"
#include "af/detail/io_socket_send.hpp"
#include "af/detail/io_socket_transfer.hpp"
#include "af/detail/io_socket_vectored.hpp"
// clang-format on

} // namespace af
