#pragma once

template <typename ThreadT> class IoFixedFile {
public:
    constexpr IoFixedFile() noexcept = default;
    constexpr IoFixedFile(ThreadT thread, int index) noexcept : thread_(thread), index_(index) {}

    [[nodiscard]] constexpr ThreadT thread() const noexcept {
        return thread_;
    }

    [[nodiscard]] constexpr int index() const noexcept {
        return index_;
    }

    [[nodiscard]] constexpr bool valid() const noexcept {
        return index_ >= 0;
    }

    constexpr void reset(ThreadT thread, int index) noexcept {
        thread_ = thread;
        index_ = index;
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus read_at(TaskT &task, void *data, std::size_t size, std::uint64_t offset,
                                   IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_read_fixed_file_at(task, thread_, index_, data, size, offset, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus readv_at(TaskT &task, const iovec *iov, int iov_count,
                                    std::uint64_t offset, IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_readv_fixed_file_at(task, thread_, index_, iov, iov_count, offset, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus read_fixed_at(TaskT &task, void *data, std::size_t size,
                                         std::uint64_t offset, std::uint16_t buffer_index,
                                         IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_read_fixed_file_at(task, thread_, index_, data, size, offset, buffer_index,
                                         state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus read_fixed_at(TaskT &task, IoFixedBuffer buffer, std::uint64_t offset,
                                         IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_read_fixed_file_at(task, thread_, index_, buffer, offset, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_some(TaskT &task, void *data, std::size_t size, IoOpState &state,
                                     std::uint32_t flags = 0) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_recv_fixed_file_some(task, thread_, index_, data, size, state, flags);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus recvv_some(TaskT &task, const iovec *iov, int iov_count,
                                      IoOpState &state, std::uint32_t flags = 0) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_recvv_fixed_file_some(task, thread_, index_, iov, iov_count, state, flags);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus write_at(TaskT &task, const void *data, std::size_t size,
                                    std::uint64_t offset, IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_write_fixed_file_at(task, thread_, index_, data, size, offset, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus writev_at(TaskT &task, const iovec *iov, int iov_count,
                                     std::uint64_t offset, IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_writev_fixed_file_at(task, thread_, index_, iov, iov_count, offset, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus write_fixed_at(TaskT &task, const void *data, std::size_t size,
                                          std::uint64_t offset, std::uint16_t buffer_index,
                                          IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_write_fixed_file_at(task, thread_, index_, data, size, offset, buffer_index,
                                          state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus write_fixed_at(TaskT &task, IoFixedBuffer buffer, std::uint64_t offset,
                                          IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_write_fixed_file_at(task, thread_, index_, buffer, offset, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus send_some(TaskT &task, const void *data, std::size_t size,
                                     IoOpState &state,
                                     std::uint32_t flags = static_cast<std::uint32_t>(
                                         detail::io_no_signal_flag())) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_send_fixed_file_some(task, thread_, index_, data, size, state, flags);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_some(TaskT &task, const iovec *iov, int iov_count,
                                      IoOpState &state,
                                      std::uint32_t flags = static_cast<std::uint32_t>(
                                          detail::io_no_signal_flag())) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_sendv_fixed_file_some(task, thread_, index_, iov, iov_count, state, flags);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus fsync(TaskT &task, IoOpState &state,
                                 std::uint32_t flags = 0) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoFixedFile thread type must match the task runtime thread type");
        return af::io_fsync_fixed_file(task, thread_, index_, state, flags);
    }

private:
    ThreadT thread_{};
    int index_{-1};
};
