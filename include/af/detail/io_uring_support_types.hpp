#if !defined(AF_IO_URING_SUPPORT_DETAIL_INCLUDE)
#error                                                                         \
    "io_uring_support_types.hpp is internal to af/detail/io_uring_support.hpp"
#endif

namespace af::detail {

struct IoUringSetupRequest {
  unsigned flags{0};
  unsigned cq_entries{0};
  unsigned sqpoll_idle_ms{0};
  int sqpoll_cpu{-1};
};

struct IoUringMessage {
  iovec iov{};
  msghdr header{};
  socklen_t *address_size{nullptr};
};

struct IoUringSocketAddress {
  sockaddr_storage storage{};
  socklen_t size{0};
  sockaddr *output{nullptr};
  socklen_t *output_size{nullptr};
  socklen_t output_capacity{0};
};

struct IoUringBufferRingRegistration {
  std::uint64_t ring_addr{0};
  std::uint32_t ring_entries{0};
  std::uint16_t bgid{0};
  std::uint16_t pad{0};
  std::uint64_t reserved[3]{};
};

struct IoUringFixedFileRwSqe {
  std::uint8_t opcode{0};
  int file_index{-1};
  void *data{nullptr};
  std::size_t size{0};
  std::uint64_t offset{0};
  std::uint16_t fixed_buffer_index{0};
  bool fixed_buffer{false};
};

struct IoUringBufferSqe {
  std::uint8_t opcode{0};
  int fd{-1};
  void *data{nullptr};
  std::size_t size{0};
  std::uint64_t offset{0};
  std::uint32_t op_flags{0};
};

[[nodiscard]] inline constexpr bool
io_uring_sqe_len_fits(std::size_t size) noexcept {
  return size <= static_cast<std::size_t>(std::numeric_limits<unsigned>::max());
}

} // namespace af::detail
