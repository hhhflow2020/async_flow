#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_io_basic_socket_tasks.hpp is a runtime_io_test_support implementation detail"
#endif

#define AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_io_socket_read_write_tasks.hpp"
#undef AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE

#define AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_io_udp_datagram_tasks.hpp"
#undef AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE

#define AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_io_udp_vectored_tasks.hpp"
#undef AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE
