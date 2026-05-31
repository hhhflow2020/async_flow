#pragma once

#include "af/io_common.hpp"

namespace af {

#define AF_IO_SOCKET_FRAGMENT_INCLUDE 1
#include "af/detail/io_socket_lifecycle_fragment.hpp"
#undef AF_IO_SOCKET_FRAGMENT_INCLUDE

#define AF_IO_SOCKET_FRAGMENT_INCLUDE 1
#include "af/detail/io_socket_accept_connect_fragment.hpp"
#undef AF_IO_SOCKET_FRAGMENT_INCLUDE

#define AF_IO_SOCKET_FRAGMENT_INCLUDE 1
#include "af/detail/io_socket_recv_fragment.hpp"
#undef AF_IO_SOCKET_FRAGMENT_INCLUDE

#define AF_IO_SOCKET_FRAGMENT_INCLUDE 1
#include "af/detail/io_socket_send_fragment.hpp"
#undef AF_IO_SOCKET_FRAGMENT_INCLUDE

#define AF_IO_SOCKET_FRAGMENT_INCLUDE 1
#include "af/detail/io_socket_transfer_fragment.hpp"
#undef AF_IO_SOCKET_FRAGMENT_INCLUDE

#define AF_IO_SOCKET_FRAGMENT_INCLUDE 1
#include "af/detail/io_socket_vectored_fragment.hpp"
#undef AF_IO_SOCKET_FRAGMENT_INCLUDE

} // namespace af
