#if !defined(AF_IO_COMMON_FRAGMENT_INCLUDE)
#error "io_common_detail_state_fragment.hpp is an io_common implementation fragment"
#endif

namespace detail {

#include "af/detail/io_common_target_fragment.hpp"
#include "af/detail/io_common_wait_arm_fragment.hpp"
#include "af/detail/io_common_wait_state_fragment.hpp"
#include "af/detail/io_common_uring_status_fragment.hpp"
#include "af/detail/io_common_iovec_fragment.hpp"

} // namespace detail
