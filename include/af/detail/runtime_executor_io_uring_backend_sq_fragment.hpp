#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_backend_sq_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if defined(__linux__)
        [[nodiscard]] io_uring_sqe* reserve_io_uring_sqe(int& error) noexcept {
            error = 0;
            if (io_uring_fd_ < 0 || io_uring_sq_tail_ == nullptr || io_uring_sq_head_ == nullptr) {
                error = ENOSYS;
                return nullptr;
            }

            std::uint32_t head = __atomic_load_n(io_uring_sq_head_, __ATOMIC_ACQUIRE);
            std::uint32_t tail = __atomic_load_n(io_uring_sq_tail_, __ATOMIC_RELAXED);
            if (tail - head >= *io_uring_sq_ring_entries_ && io_uring_pending_submissions_ != 0U) {
                const int submit_error = flush_io_uring_submissions();
                if (submit_error != 0) {
                    error = submit_error;
                    fail_io_uring_backend(submit_error, nullptr);
                    return nullptr;
                }
                head = __atomic_load_n(io_uring_sq_head_, __ATOMIC_ACQUIRE);
                tail = __atomic_load_n(io_uring_sq_tail_, __ATOMIC_RELAXED);
            }
            if (tail - head >= *io_uring_sq_ring_entries_) {
                error = EBUSY;
                return nullptr;
            }

            const std::uint32_t index = tail & *io_uring_sq_ring_mask_;
            io_uring_sq_array_[index] = index;
            __atomic_store_n(io_uring_sq_tail_, tail + 1U, __ATOMIC_RELEASE);
            ++io_uring_pending_submissions_;
            return &io_uring_sqes_[index];
        }

        [[nodiscard]] int flush_io_uring_submissions() noexcept {
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

        [[nodiscard]] bool flush_io_uring_submissions_or_fail() noexcept {
            const int submit_error = flush_io_uring_submissions();
            if (submit_error == 0) {
                return false;
            }
            fail_io_uring_backend(submit_error, nullptr);
            return true;
        }
#endif
