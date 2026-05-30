#pragma once

#include "af/io_timeout.hpp"

namespace af {

template <typename ThreadT>
class IoDescriptor {
public:
    constexpr IoDescriptor() noexcept = default;
    constexpr IoDescriptor(ThreadT thread, int fd) noexcept : thread_(thread), fd_(fd) {}

    [[nodiscard]] constexpr ThreadT thread() const noexcept {
        return thread_;
    }

    [[nodiscard]] constexpr int fd() const noexcept {
        return fd_;
    }

    [[nodiscard]] constexpr bool valid() const noexcept {
        return fd_ >= 0;
    }

    constexpr void reset(ThreadT thread, int fd) noexcept {
        thread_ = thread;
        fd_ = fd;
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus setsockopt(
        TaskT& task,
        int level,
        int option,
        const void* value,
        socklen_t value_size) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDescriptor thread type must match the task runtime thread type");
        return af::io_setsockopt(task, thread_, fd_, level, option, value, value_size);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus getsockopt(
        TaskT& task,
        int level,
        int option,
        void* value,
        socklen_t* value_size) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDescriptor thread type must match the task runtime thread type");
        return af::io_getsockopt(task, thread_, fd_, level, option, value, value_size);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus getsockname(
        TaskT& task,
        sockaddr* address,
        socklen_t* address_size) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDescriptor thread type must match the task runtime thread type");
        return af::io_getsockname(task, thread_, fd_, address, address_size);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus getpeername(
        TaskT& task,
        sockaddr* address,
        socklen_t* address_size) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDescriptor thread type must match the task runtime thread type");
        return af::io_getpeername(task, thread_, fd_, address, address_size);
    }
#endif

protected:
    ThreadT thread_{};
    int fd_{-1};
};

template <typename ThreadT>
class IoFile : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus read_some(
        TaskT& task,
        void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_read_some(task, this->thread_, this->fd_, data, size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus write_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_write_some(task, this->thread_, this->fd_, data, size, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus readv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_readv_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus writev_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_writev_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus read_at(
        TaskT& task,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_read_at(task, this->thread_, this->fd_, data, size, offset, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus read_fixed_at(
        TaskT& task,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_read_fixed_at(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            offset,
            buffer_index,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus read_fixed_at(
        TaskT& task,
        IoFixedBuffer buffer,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_read_fixed_at(task, this->thread_, this->fd_, buffer, offset, state);
    }
#endif

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus readv_at(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_readv_at(task, this->thread_, this->fd_, iov, iov_count, offset, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus write_at(
        TaskT& task,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_write_at(task, this->thread_, this->fd_, data, size, offset, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus write_fixed_at(
        TaskT& task,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_write_fixed_at(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            offset,
            buffer_index,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus write_fixed_at(
        TaskT& task,
        IoFixedBuffer buffer,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_write_fixed_at(task, this->thread_, this->fd_, buffer, offset, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus writev_at(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_writev_at(task, this->thread_, this->fd_, iov, iov_count, offset, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus fsync(
        TaskT& task,
        IoOpState& state,
        std::uint32_t flags = 0) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFile thread type must match the task runtime thread type");
        return af::io_fsync(task, this->thread_, this->fd_, flags, state);
    }
};

template <typename ThreadT>
class IoFixedFile {
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
    [[nodiscard]] IoStatus read_at(
        TaskT& task,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_read_fixed_file_at(task, thread_, index_, data, size, offset, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus readv_at(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_readv_fixed_file_at(task, thread_, index_, iov, iov_count, offset, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_some(
        TaskT& task,
        void* data,
        std::size_t size,
        IoOpState& state,
        std::uint32_t flags = 0) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_recv_fixed_file_some(task, thread_, index_, data, size, state, flags);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus recvv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state,
        std::uint32_t flags = 0) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_recvv_fixed_file_some(task, thread_, index_, iov, iov_count, state, flags);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus read_fixed_at(
        TaskT& task,
        void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_read_fixed_file_at(
            task,
            thread_,
            index_,
            data,
            size,
            offset,
            buffer_index,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus read_fixed_at(
        TaskT& task,
        IoFixedBuffer buffer,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_read_fixed_file_at(task, thread_, index_, buffer, offset, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus write_at(
        TaskT& task,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_write_fixed_file_at(task, thread_, index_, data, size, offset, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus writev_at(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_writev_fixed_file_at(task, thread_, index_, iov, iov_count, offset, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus send_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state,
        std::uint32_t flags = static_cast<std::uint32_t>(detail::io_no_signal_flag())) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_send_fixed_file_some(task, thread_, index_, data, size, state, flags);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state,
        std::uint32_t flags = static_cast<std::uint32_t>(detail::io_no_signal_flag())) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_sendv_fixed_file_some(task, thread_, index_, iov, iov_count, state, flags);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus write_fixed_at(
        TaskT& task,
        const void* data,
        std::size_t size,
        std::uint64_t offset,
        std::uint16_t buffer_index,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_write_fixed_file_at(
            task,
            thread_,
            index_,
            data,
            size,
            offset,
            buffer_index,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus write_fixed_at(
        TaskT& task,
        IoFixedBuffer buffer,
        std::uint64_t offset,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_write_fixed_file_at(task, thread_, index_, buffer, offset, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus fsync(
        TaskT& task,
        IoOpState& state,
        std::uint32_t flags = 0) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoFixedFile thread type must match the task runtime thread type");
        return af::io_fsync_fixed_file(task, thread_, index_, state, flags);
    }

private:
    ThreadT thread_{};
    int index_{-1};
};

template <typename ThreadT>
class IoStream : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_some(
        TaskT& task,
        void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_recv_some(task, this->thread_, this->fd_, data, size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_multishot(
        TaskT& task,
        std::uint16_t buffer_group,
        std::uint16_t* buffer_id,
        IoOpState& state,
        std::uint32_t flags = 0) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_recv_multishot(
            task,
            this->thread_,
            this->fd_,
            buffer_group,
            buffer_id,
            state,
            flags);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus send_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_send_some(task, this->thread_, this->fd_, data, size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus send_zc_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_send_zc_some(task, this->thread_, this->fd_, data, size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendfile_some(
        TaskT& task,
        int file_fd,
        IoOffset* offset,
        std::size_t count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_sendfile_some(
            task,
            this->thread_,
            this->fd_,
            file_fd,
            offset,
            count,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus shutdown(
        TaskT& task,
        int how,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_shutdown(task, this->thread_, this->fd_, how, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus recvv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_recvv_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_sendv_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_zc_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_sendv_zc_some(task, this->thread_, this->fd_, iov, iov_count, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus connect(
        TaskT& task,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoStream thread type must match the task runtime thread type");
        return af::io_connect(task, this->thread_, this->fd_, address, address_size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus read_some(
        TaskT& task,
        void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        return recv_some(task, data, size, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus readv_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        return recvv_some(task, iov, iov_count, state);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus write_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        IoOpState& state) const noexcept {
        return send_some(task, data, size, state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus writev_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        return sendv_some(task, iov, iov_count, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus writev_zc_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        IoOpState& state) const noexcept {
        return sendv_zc_some(task, iov, iov_count, state);
    }
#endif
};

template <typename ThreadT>
class IoListener : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus bind(
        TaskT& task,
        const sockaddr* address,
        socklen_t address_size) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_bind(task, this->thread_, this->fd_, address, address_size);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus listen(TaskT& task, int backlog) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_listen(task, this->thread_, this->fd_, backlog);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus accept_some(
        TaskT& task,
        sockaddr* address,
        socklen_t* address_size,
        int* accepted_fd,
        IoOpState& state,
        int flags = detail::io_default_accept_flags()) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_accept_some(
            task,
            this->thread_,
            this->fd_,
            address,
            address_size,
            accepted_fd,
            state,
            flags);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus accept_direct(
        TaskT& task,
        sockaddr* address,
        socklen_t* address_size,
        int file_index,
        IoFixedFile<ThreadT>* accepted_file,
        IoOpState& state,
        int flags = detail::io_default_accept_flags()) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_accept_direct(
            task,
            this->thread_,
            this->fd_,
            address,
            address_size,
            flags,
            file_index,
            accepted_file,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus accept_multishot(
        TaskT& task,
        sockaddr* address,
        socklen_t* address_size,
        int* accepted_fd,
        IoOpState& state,
        int flags = detail::io_default_accept_flags()) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoListener thread type must match the task runtime thread type");
        return af::io_accept_multishot(
            task,
            this->thread_,
            this->fd_,
            address,
            address_size,
            accepted_fd,
            state,
            flags);
    }
};

template <typename ThreadT>
class IoDatagramSocket : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus bind(
        TaskT& task,
        const sockaddr* address,
        socklen_t address_size) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_bind(task, this->thread_, this->fd_, address, address_size);
    }
#endif

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_from_some(
        TaskT& task,
        void* data,
        std::size_t size,
        sockaddr* address,
        socklen_t* address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_recv_from_some(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            address,
            address_size,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_multishot(
        TaskT& task,
        std::uint16_t buffer_group,
        std::uint16_t* buffer_id,
        IoOpState& state,
        std::uint32_t flags = 0) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_recv_multishot(
            task,
            this->thread_,
            this->fd_,
            buffer_group,
            buffer_id,
            state,
            flags);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_from_multishot(
        TaskT& task,
        std::uint16_t buffer_group,
        socklen_t name_capacity,
        std::size_t control_capacity,
        std::uint16_t* buffer_id,
        IoOpState& state,
        std::uint32_t flags = 0) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_recvmsg_multishot(
            task,
            this->thread_,
            this->fd_,
            buffer_group,
            name_capacity,
            control_capacity,
            buffer_id,
            state,
            flags);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus send_to_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_send_to_some(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            address,
            address_size,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus send_zc_to_some(
        TaskT& task,
        const void* data,
        std::size_t size,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_send_zc_to_some(
            task,
            this->thread_,
            this->fd_,
            data,
            size,
            address,
            address_size,
            state);
    }

#if !defined(_WIN32)
    template <typename TaskT>
    [[nodiscard]] IoStatus recvv_from_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        sockaddr* address,
        socklen_t* address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_recvv_from_some(
            task,
            this->thread_,
            this->fd_,
            iov,
            iov_count,
            address,
            address_size,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_to_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_sendv_to_some(
            task,
            this->thread_,
            this->fd_,
            iov,
            iov_count,
            address,
            address_size,
            state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_zc_to_some(
        TaskT& task,
        const iovec* iov,
        int iov_count,
        const sockaddr* address,
        socklen_t address_size,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_sendv_zc_to_some(
            task,
            this->thread_,
            this->fd_,
            iov,
            iov_count,
            address,
            address_size,
            state);
    }
#endif
};

template <typename ThreadT>
using TcpStream = IoStream<ThreadT>;

template <typename ThreadT>
using TcpListener = IoListener<ThreadT>;

template <typename ThreadT>
using UdpSocket = IoDatagramSocket<ThreadT>;

template <typename ThreadT>
class IoEvent : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus wait(
        TaskT& task,
        std::uint64_t* value,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoEvent thread type must match the task runtime thread type");
        return af::io_wait_eventfd(task, this->thread_, this->fd_, value, state);
    }
};

template <typename ThreadT>
class IoTimer : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus wait(
        TaskT& task,
        std::uint64_t* expirations,
        IoOpState& state) const noexcept {
        static_assert(
            std::is_same_v<typename TaskT::Thread, ThreadT>,
            "IoTimer thread type must match the task runtime thread type");
        return af::io_wait_timerfd(task, this->thread_, this->fd_, expirations, state);
    }
};


} // namespace af
