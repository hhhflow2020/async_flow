#if !defined(AF_IO_URING_SUPPORT_DETAIL_INCLUDE)
#error "io_uring_support_sqe.hpp is internal to af/detail/io_uring_support.hpp"
#endif

namespace af::detail {

inline void fill_buffer_sqe(io_uring_sqe &sqe, const IoUringBufferSqe &request,
                            std::uint64_t user_data) noexcept {
  sqe = io_uring_sqe{};
  sqe.opcode = request.opcode;
  sqe.fd = request.fd;
  sqe.user_data = user_data;
  sqe.addr = reinterpret_cast<std::uint64_t>(request.data);
  sqe.len = static_cast<unsigned>(request.size);
  if (request.opcode == IORING_OP_RECV || request.opcode == IORING_OP_SEND) {
    sqe.msg_flags = request.op_flags;
  } else {
    sqe.off = request.offset;
  }
}

inline void fill_fixed_file_rw_sqe(io_uring_sqe &sqe,
                                   const IoUringFixedFileRwSqe &request,
                                   std::uint64_t user_data) noexcept {
  sqe = io_uring_sqe{};
  sqe.opcode = request.opcode;
  sqe.fd = request.file_index;
  sqe.flags |= IOSQE_FIXED_FILE;
  sqe.user_data = user_data;
  sqe.addr = reinterpret_cast<std::uint64_t>(request.data);
  sqe.len = static_cast<unsigned>(request.size);
  sqe.off = request.offset;
  if (request.fixed_buffer) {
    sqe.buf_index = request.fixed_buffer_index;
  }
}

} // namespace af::detail
