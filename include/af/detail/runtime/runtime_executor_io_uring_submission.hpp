#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_io_uring_submission.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] io_uring_sqe *Executor<RuntimeT, TraitsT>::reserve_io_uring_sqe(int &error) noexcept {
    error = 0;
    if (io_uring_fd_ < 0 || io_uring_sq_tail_ == nullptr || io_uring_sq_head_ == nullptr) {
        error = ENOSYS;
        return nullptr;
    }

    std::uint32_t head = io_uring_sq_cached_head_;
    std::uint32_t tail = io_uring_sq_cached_tail_;
    if (tail - head >= io_uring_sq_ring_entries_value_ && io_uring_pending_submissions_ != 0U) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error != 0) {
            error = submit_error;
            fail_io_uring_backend(submit_error, nullptr);
            return nullptr;
        }
        head = __atomic_load_n(io_uring_sq_head_, __ATOMIC_ACQUIRE);
        io_uring_sq_cached_head_ = head;
        tail = io_uring_sq_cached_tail_;
    }
    if (tail - head >= io_uring_sq_ring_entries_value_) {
        io_uring_sq_cached_head_ = __atomic_load_n(io_uring_sq_head_, __ATOMIC_ACQUIRE);
        head = io_uring_sq_cached_head_;
    }
    if (tail - head >= io_uring_sq_ring_entries_value_) {
        error = EBUSY;
        return nullptr;
    }

    const std::uint32_t index = tail & io_uring_sq_ring_mask_value_;
    io_uring_sq_array_[index] = index;
    io_uring_sq_cached_tail_ = tail + 1U;
    __atomic_store_n(io_uring_sq_tail_, io_uring_sq_cached_tail_, __ATOMIC_RELEASE);
    ++io_uring_pending_submissions_;
    return &io_uring_sqes_[index];
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] int Executor<RuntimeT, TraitsT>::flush_io_uring_submissions() noexcept {
    if (io_uring_pending_submissions_ == 0U) {
        return 0;
    }

    unsigned remaining = io_uring_pending_submissions_;
    while (remaining != 0U) {
        const int submitted = detail::sys_io_uring_enter(io_uring_fd_, remaining, 0, 0);
        if (submitted > 0) {
            const auto submitted_count = static_cast<unsigned>(submitted);
            if (submitted_count > remaining) {
                return EIO;
            }
            remaining -= submitted_count;
            continue;
        }
        if (submitted == 0) {
            return EIO;
        }
        if (errno == EINTR) {
            continue;
        }
        return errno == 0 ? EIO : errno;
    }

    io_uring_pending_submissions_ = 0;
    return 0;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::flush_io_uring_submissions_or_fail() noexcept {
    const int submit_error = flush_io_uring_submissions();
    if (submit_error == 0) {
        return false;
    }
    fail_io_uring_backend(submit_error, nullptr);
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] int
Executor<RuntimeT, TraitsT>::submit_io_uring_cancel(IoUringOperation *operation) noexcept {
    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
        return reserve_error == 0 ? EBUSY : reserve_error;
    }

    *sqe = io_uring_sqe{};
    sqe->opcode = IORING_OP_ASYNC_CANCEL;
    sqe->fd = -1;
    sqe->addr = reinterpret_cast<std::uint64_t>(operation);
    sqe->cancel_flags = 0;
    sqe->user_data = 0;

    const int submit_error = flush_io_uring_submissions();
    if (submit_error != 0) {
        fail_io_uring_backend(submit_error, nullptr);
    }
    return submit_error;
}
#endif

} // namespace af::detail
