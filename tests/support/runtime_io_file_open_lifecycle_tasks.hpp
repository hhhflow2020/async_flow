#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_io_file_open_lifecycle_tasks.hpp is a runtime_io_file_tasks implementation detail"
#endif

#define AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_io_file_batch_write_tasks.hpp"
#undef AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE

#define AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_io_file_openat_tasks.hpp"
#undef AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE

#define AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE 1
#include "runtime_io_file_lifecycle_tasks.hpp"
#undef AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE
