#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#if defined(__linux__)
#include <linux/io_uring.h>

#ifndef IORING_CQE_F_MORE
#define IORING_CQE_F_MORE (1U << 1U)
#endif

#ifndef IORING_CQE_F_BUFFER
#define IORING_CQE_F_BUFFER (1U << 0U)
#endif

#ifndef IOSQE_BUFFER_SELECT
#define IOSQE_BUFFER_SELECT (1U << 5U)
#endif

#ifndef IORING_ACCEPT_MULTISHOT
#define IORING_ACCEPT_MULTISHOT (1U << 0U)
#endif

#ifndef IORING_RECV_MULTISHOT
#define IORING_RECV_MULTISHOT (1U << 1U)
#endif

#ifndef IORING_REGISTER_PBUF_RING
#define IORING_REGISTER_PBUF_RING 22U
#endif

#ifndef IORING_UNREGISTER_PBUF_RING
#define IORING_UNREGISTER_PBUF_RING 23U
#endif

#ifndef IORING_CQE_F_NOTIF
#define IORING_CQE_F_NOTIF (1U << 3U)
#endif

namespace af::detail {

struct IoUringFixedFileRwSqe {
    std::uint8_t opcode{0};
    int file_index{-1};
    void* data{nullptr};
    std::size_t size{0};
    std::uint64_t offset{0};
    std::uint16_t fixed_buffer_index{0};
    bool fixed_buffer{false};
};

struct IoUringBufferSqe {
    std::uint8_t opcode{0};
    int fd{-1};
    void* data{nullptr};
    std::size_t size{0};
    std::uint64_t offset{0};
    std::uint32_t op_flags{0};
};

[[nodiscard]] inline constexpr bool io_uring_sqe_len_fits(std::size_t size) noexcept {
    return size <= static_cast<std::size_t>(std::numeric_limits<unsigned>::max());
}

inline void fill_buffer_sqe(
    io_uring_sqe& sqe,
    const IoUringBufferSqe& request,
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

inline void fill_fixed_file_rw_sqe(
    io_uring_sqe& sqe,
    const IoUringFixedFileRwSqe& request,
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
#endif
