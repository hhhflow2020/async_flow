#if !defined(AF_IO_URING_SUPPORT_DETAIL_INCLUDE)
#error                                                                         \
    "io_uring_support_opcode.hpp is internal to af/detail/io_uring_support.hpp"
#endif

namespace af::detail {

inline constexpr std::uint8_t io_uring_op_send_zc = 47U;
inline constexpr std::uint8_t io_uring_op_sendmsg_zc = 48U;
inline constexpr std::uint8_t io_uring_op_socket = 45U;
inline constexpr std::uint8_t io_uring_op_openat2 = 28U;
inline constexpr std::uint8_t io_uring_op_mkdirat = 37U;
inline constexpr std::uint8_t io_uring_op_symlinkat = 38U;
inline constexpr std::uint8_t io_uring_op_linkat = 39U;
inline constexpr std::uint8_t io_uring_op_ftruncate = 55U;

} // namespace af::detail
