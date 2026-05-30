#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#if defined(__linux__)
#include <linux/io_uring.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

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

#ifndef IORING_SETUP_SQPOLL
#define IORING_SETUP_SQPOLL (1U << 1U)
#endif

#ifndef IORING_SETUP_SQ_AFF
#define IORING_SETUP_SQ_AFF (1U << 2U)
#endif

#ifndef IORING_SETUP_CQSIZE
#define IORING_SETUP_CQSIZE (1U << 3U)
#endif

#ifndef IORING_SETUP_SUBMIT_ALL
#define IORING_SETUP_SUBMIT_ALL (1U << 7U)
#endif

#ifndef IORING_SETUP_COOP_TASKRUN
#define IORING_SETUP_COOP_TASKRUN (1U << 8U)
#endif

#ifndef IORING_SETUP_SINGLE_ISSUER
#define IORING_SETUP_SINGLE_ISSUER (1U << 12U)
#endif

#ifndef IORING_SETUP_DEFER_TASKRUN
#define IORING_SETUP_DEFER_TASKRUN (1U << 13U)
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

struct IoUringSetupRequest {
    unsigned flags{0};
    unsigned cq_entries{0};
    unsigned sqpoll_idle_ms{0};
    int sqpoll_cpu{-1};
};

struct IoUringMessage {
    iovec iov{};
    msghdr header{};
    socklen_t* address_size{nullptr};
};

struct IoUringSocketAddress {
    sockaddr_storage storage{};
    socklen_t size{0};
    sockaddr* output{nullptr};
    socklen_t* output_size{nullptr};
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

[[nodiscard]] inline int sys_io_uring_setup(unsigned entries, io_uring_params* params) noexcept {
    return static_cast<int>(::syscall(__NR_io_uring_setup, entries, params));
}

[[nodiscard]] inline int sys_io_uring_enter(
    int ring_fd,
    unsigned to_submit,
    unsigned min_complete,
    unsigned flags) noexcept {
    return static_cast<int>(::syscall(
        __NR_io_uring_enter,
        ring_fd,
        to_submit,
        min_complete,
        flags,
        nullptr,
        0));
}

[[nodiscard]] inline int sys_io_uring_register(
    int ring_fd,
    unsigned opcode,
    const void* arg,
    unsigned nr_args) noexcept {
    return static_cast<int>(::syscall(__NR_io_uring_register, ring_fd, opcode, arg, nr_args));
}

inline void configure_io_uring_params(
    io_uring_params& params,
    const IoUringSetupRequest& request) noexcept {
    params.flags = request.flags;
    if (request.cq_entries != 0U) {
        params.flags |= IORING_SETUP_CQSIZE;
        params.cq_entries = request.cq_entries;
    }
    if ((params.flags & IORING_SETUP_SQPOLL) != 0U) {
        params.sq_thread_idle = request.sqpoll_idle_ms;
        if (request.sqpoll_cpu >= 0) {
            params.flags |= IORING_SETUP_SQ_AFF;
            params.sq_thread_cpu = static_cast<unsigned>(request.sqpoll_cpu);
        }
    }
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
