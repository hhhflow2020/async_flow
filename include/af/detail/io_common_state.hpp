#if !defined(AF_IO_COMMON_DETAIL_INCLUDE)
#error "io_common_state.hpp is internal to af/io_common.hpp"
#endif

namespace detail {

// clang-format off
#include "af/detail/io_common_target.hpp"
#include "af/detail/io_common_wait_arm.hpp"
#include "af/detail/io_common_wait_state.hpp"
#include "af/detail/io_common_uring_status.hpp"
#include "af/detail/io_common_iovec.hpp"
// clang-format on

} // namespace detail
