#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_io_submit_core.hpp is internal"
#endif

namespace af::detail {

#if defined(__linux__)
template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::provided_buffer_group_registered(
    std::uint16_t buffer_group) const noexcept {
    return std::find(io_uring_provided_buffer_groups_.begin(),
                     io_uring_provided_buffer_groups_.end(),
                     buffer_group) != io_uring_provided_buffer_groups_.end();
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::submit_io_uring_buffer_op(
    std::uint8_t opcode, int fd, void *data, std::size_t size, std::uint64_t offset,
    std::uint32_t op_flags, std::uint32_t complete_events, Task *task, IoResult *result) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring submit must be called from its IO thread");
    if (result != nullptr) {
        result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr || result == nullptr) {
        if (result != nullptr) {
            detail::set_io_result_error(*result, fd, EINVAL);
        }
        return false;
    }
    if (io_uring_fd_ < 0 || fd < 0) {
        detail::set_io_result_error(*result, fd, io_uring_fd_ < 0 ? ENOSYS : EBADF);
        return false;
    }
    if (data == nullptr) {
        detail::set_io_result_error(*result, fd, EINVAL);
        return false;
    }
    if (!detail::io_uring_sqe_len_fits(size)) {
        detail::set_io_result_error(*result, fd, EINVAL);
        return false;
    }

    IoUringOperation *operation = nullptr;
    try {
        operation = io_uring_op_pool_.create();
    } catch (...) {
        detail::set_io_result_error(*result, fd, ENOMEM);
        return false;
    }

    operation->task = task;
    operation->result = result;
    operation->complete_events = complete_events;
    operation->poll_flags = 0;
    operation->direct_file_index = -1;
    operation->opcode = opcode;
    operation->cancel_requested = false;
    operation->multishot = false;
    operation->poll_wait = false;
    operation->net_poll = false;
    operation->zero_copy_send = false;
    operation->zero_copy_primary_done = false;
    operation->zero_copy_notification_done = false;
    operation->msg = nullptr;
    operation->socket_address = nullptr;
    operation->wait_registration = nullptr;
    operation->net_channel = nullptr;

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
        io_uring_op_pool_.destroy(operation);
        detail::set_io_result_error(*result, fd, reserve_error == 0 ? EBUSY : reserve_error);
        return false;
    }

    track_io_uring_operation(operation);

    detail::fill_buffer_sqe(*sqe,
                            detail::IoUringBufferSqe{opcode, fd, data, size, offset, op_flags},
                            reinterpret_cast<std::uint64_t>(operation));

    result->fd = fd;
    result->events = 0;
    result->error = 0;
    result->result = 0;
    result->completion_token = operation;

    if (io_uring_pending_submissions_ >= io_uring_submit_batch_threshold) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error == 0) {
            return true;
        }
        detail::set_io_result_error(*result, fd, submit_error);
        fail_io_uring_backend(submit_error, operation);
        return false;
    }

    return true;
}

template <typename RuntimeT, typename TraitsT>
template <typename FillSqe>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::submit_io_uring_fast_sqe(
    std::uint8_t opcode, int result_fd, std::uint32_t complete_events, Task *task, IoResult *result,
    FillSqe &&fill_sqe) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring submit must be called from its IO thread");
    if (result != nullptr) {
        result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr || result == nullptr) {
        if (result != nullptr) {
            detail::set_io_result_error(*result, result_fd, EINVAL);
        }
        return false;
    }
    if (io_uring_fd_ < 0) {
        detail::set_io_result_error(*result, result_fd, ENOSYS);
        return false;
    }

    IoUringOperation *operation = nullptr;
    try {
        operation = io_uring_op_pool_.create();
    } catch (...) {
        detail::set_io_result_error(*result, result_fd, ENOMEM);
        return false;
    }

    operation->task = task;
    operation->result = result;
    operation->complete_events = complete_events;
    operation->poll_flags = 0;
    operation->direct_file_index = -1;
    operation->opcode = opcode;
    operation->cancel_requested = false;
    operation->multishot = false;
    operation->poll_wait = false;
    operation->net_poll = false;
    operation->zero_copy_send = false;
    operation->zero_copy_primary_done = false;
    operation->zero_copy_notification_done = false;
    operation->msg = nullptr;
    operation->socket_address = nullptr;
    operation->wait_registration = nullptr;
    operation->net_channel = nullptr;

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
        io_uring_op_pool_.destroy(operation);
        detail::set_io_result_error(*result, result_fd, reserve_error == 0 ? EBUSY : reserve_error);
        return false;
    }

    track_io_uring_operation(operation);

    *sqe = io_uring_sqe{};
    sqe->opcode = opcode;
    sqe->user_data = reinterpret_cast<std::uint64_t>(operation);
    fill_sqe(*sqe);

    result->fd = result_fd;
    result->events = 0;
    result->error = 0;
    result->result = 0;
    result->completion_token = operation;

    if (io_uring_pending_submissions_ >= io_uring_submit_batch_threshold) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error == 0) {
            return true;
        }
        detail::set_io_result_error(*result, result_fd, submit_error);
        fail_io_uring_backend(submit_error, operation);
        return false;
    }

    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::submit_io_uring_socket_impl(int domain, int type, int protocol,
                                                         std::uint32_t flags, Task *task,
                                                         IoResult *result) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring socket submit must be called from its IO thread");
    if (result != nullptr) {
        result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr || result == nullptr) {
        if (result != nullptr) {
            detail::set_io_result_error(*result, -1, EINVAL);
        }
        return false;
    }
    if (io_uring_fd_ < 0 || !io_uring_socket_available_) {
        detail::set_io_result_error(*result, -1, ENOSYS);
        return false;
    }

    IoUringOperation *operation = nullptr;
    try {
        operation = io_uring_op_pool_.create();
    } catch (...) {
        detail::set_io_result_error(*result, -1, ENOMEM);
        return false;
    }

    operation->task = task;
    operation->result = result;
    operation->complete_events = io_readable;
    operation->poll_flags = 0;
    operation->direct_file_index = -1;
    operation->opcode = detail::io_uring_op_socket;
    operation->cancel_requested = false;
    operation->multishot = false;
    operation->poll_wait = false;
    operation->net_poll = false;
    operation->zero_copy_send = false;
    operation->zero_copy_primary_done = false;
    operation->zero_copy_notification_done = false;
    operation->msg = nullptr;
    operation->socket_address = nullptr;
    operation->wait_registration = nullptr;
    operation->net_channel = nullptr;

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
        io_uring_op_pool_.destroy(operation);
        detail::set_io_result_error(*result, -1, reserve_error == 0 ? EBUSY : reserve_error);
        return false;
    }

    track_io_uring_operation(operation);

    *sqe = io_uring_sqe{};
    sqe->opcode = detail::io_uring_op_socket;
    sqe->fd = domain;
    sqe->off = static_cast<std::uint64_t>(type);
    sqe->len = static_cast<unsigned>(protocol);
    sqe->rw_flags = flags;
    sqe->user_data = reinterpret_cast<std::uint64_t>(operation);

    result->fd = -1;
    result->events = 0;
    result->error = 0;
    result->result = 0;
    result->completion_token = operation;

    if (io_uring_pending_submissions_ >= io_uring_submit_batch_threshold) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error == 0) {
            return true;
        }
        detail::set_io_result_error(*result, -1, submit_error);
        fail_io_uring_backend(submit_error, operation);
        return false;
    }

    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::submit_io_uring_fixed_file_rw(
    std::uint8_t opcode, int file_index, void *data, std::size_t size, std::uint64_t offset,
    std::uint32_t complete_events, Task *task, IoResult *result, std::uint16_t fixed_buffer_index,
    bool fixed_buffer) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring submit must be called from its IO thread");
    if (result != nullptr) {
        result->completion_token = nullptr;
    }
    if (RuntimeT::current_thread_index_ != index_ || task == nullptr || result == nullptr) {
        if (result != nullptr) {
            detail::set_io_result_error(*result, file_index, EINVAL);
        }
        return false;
    }
    if (io_uring_fd_ < 0 || file_index < 0) {
        detail::set_io_result_error(*result, file_index, io_uring_fd_ < 0 ? ENOSYS : EBADF);
        return false;
    }
    if (data == nullptr) {
        detail::set_io_result_error(*result, file_index, EINVAL);
        return false;
    }
    if (!io_uring_files_registered_) {
        detail::set_io_result_error(*result, file_index, ENXIO);
        return false;
    }
    if (static_cast<unsigned>(file_index) >= io_uring_registered_file_count_) {
        detail::set_io_result_error(*result, file_index, EINVAL);
        return false;
    }
    if (fixed_buffer) {
        if (!io_uring_buffers_registered_) {
            detail::set_io_result_error(*result, file_index, ENOBUFS);
            return false;
        }
        if (fixed_buffer_index >= io_uring_registered_buffer_count_) {
            detail::set_io_result_error(*result, file_index, EINVAL);
            return false;
        }
    }
    if (!detail::io_uring_sqe_len_fits(size)) {
        detail::set_io_result_error(*result, file_index, EINVAL);
        return false;
    }

    IoUringOperation *operation = nullptr;
    try {
        operation = io_uring_op_pool_.create();
    } catch (...) {
        detail::set_io_result_error(*result, file_index, ENOMEM);
        return false;
    }

    operation->task = task;
    operation->result = result;
    operation->complete_events = complete_events;
    operation->poll_flags = 0;
    operation->direct_file_index = -1;
    operation->opcode = opcode;
    operation->cancel_requested = false;
    operation->multishot = false;
    operation->poll_wait = false;
    operation->net_poll = false;
    operation->zero_copy_send = false;
    operation->zero_copy_primary_done = false;
    operation->zero_copy_notification_done = false;
    operation->msg = nullptr;
    operation->socket_address = nullptr;
    operation->wait_registration = nullptr;
    operation->net_channel = nullptr;

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
        io_uring_op_pool_.destroy(operation);
        detail::set_io_result_error(*result, file_index,
                                    reserve_error == 0 ? EBUSY : reserve_error);
        return false;
    }

    track_io_uring_operation(operation);

    detail::fill_fixed_file_rw_sqe(*sqe,
                                   detail::IoUringFixedFileRwSqe{opcode, file_index, data, size,
                                                                 offset, fixed_buffer_index,
                                                                 fixed_buffer},
                                   reinterpret_cast<std::uint64_t>(operation));

    result->fd = file_index;
    result->events = 0;
    result->error = 0;
    result->result = 0;
    result->completion_token = operation;

    if (io_uring_pending_submissions_ >= io_uring_submit_batch_threshold) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error == 0) {
            return true;
        }
        detail::set_io_result_error(*result, file_index, submit_error);
        fail_io_uring_backend(submit_error, operation);
        return false;
    }

    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::validate_io_uring_generic_submit(
    const IoUringGenericSubmitArgs &args, const IoUringGenericSubmitKind &kind) const noexcept {
    if (RuntimeT::current_thread_index_ != index_ || args.task == nullptr ||
        args.result == nullptr) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
    }
    if (io_uring_fd_ < 0 || (!kind.path_fd_op && args.fd < 0)) {
        set_io_uring_generic_submit_error(args.result, args.fd, io_uring_fd_ < 0 ? ENOSYS : EBADF);
        return false;
    }
    if (!kind.data_optional_op && !kind.address_op && args.data == nullptr &&
        !kind.message_iov_op && !args.buffer_select) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
    }
    if (!validate_io_uring_generic_fixed_resources(args, kind)) {
        return false;
    }
    if (args.opcode != IORING_OP_FSYNC && !kind.message_op &&
        args.size > static_cast<std::size_t>(std::numeric_limits<unsigned>::max())) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
    }
    if (kind.message_iov_op && (args.message_iov_count == 0U ||
                                args.message_iov_count > static_cast<std::size_t>(IOV_MAX))) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
    }
    if (kind.connect_op &&
        (args.socket_address == nullptr || args.socket_address_size == 0U ||
         args.socket_address_size > static_cast<socklen_t>(sizeof(sockaddr_storage)))) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
    }
    if (kind.accept_op &&
        ((args.socket_address_out == nullptr) != (args.socket_address_size_out == nullptr))) {
        set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
        return false;
    }
    if (args.buffer_select &&
        ((args.opcode != IORING_OP_RECV && args.opcode != IORING_OP_RECVMSG) ||
         !provided_buffer_group_registered(args.provided_buffer_group))) {
        const int error =
            (args.opcode == IORING_OP_RECV || args.opcode == IORING_OP_RECVMSG) ? ENOBUFS : EINVAL;
        set_io_uring_generic_submit_error(args.result, args.fd, error);
        return false;
    }
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::validate_io_uring_generic_fixed_resources(
    const IoUringGenericSubmitArgs &args, const IoUringGenericSubmitKind &kind) const noexcept {
    if (args.fixed_file) {
        if (kind.path_fd_op) {
            set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
            return false;
        }
        if (!io_uring_files_registered_) {
            set_io_uring_generic_submit_error(args.result, args.fd, ENXIO);
            return false;
        }
        if (static_cast<unsigned>(args.fd) >= io_uring_registered_file_count_) {
            set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
            return false;
        }
    }
    if (args.direct_file_index >= 0) {
        if (!(kind.openat_op || kind.accept_op)) {
            set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
            return false;
        }
        if (!io_uring_files_registered_) {
            set_io_uring_generic_submit_error(args.result, args.fd, ENXIO);
            return false;
        }
        if (static_cast<unsigned>(args.direct_file_index) >= io_uring_registered_file_count_) {
            set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
            return false;
        }
    }
    if (kind.fixed_buffer_op) {
        if (!io_uring_buffers_registered_) {
            set_io_uring_generic_submit_error(args.result, args.fd, ENOBUFS);
            return false;
        }
        if (args.fixed_buffer_index >= io_uring_registered_buffer_count_) {
            set_io_uring_generic_submit_error(args.result, args.fd, EINVAL);
            return false;
        }
    }
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::attach_io_uring_generic_submit_message(
    const IoUringGenericSubmitArgs &args, const IoUringGenericSubmitKind &kind,
    IoUringOperation *operation) noexcept {
    try {
        operation->msg = io_uring_msg_pool_.create();
    } catch (...) {
        io_uring_op_pool_.destroy(operation);
        set_io_uring_generic_submit_error(args.result, args.fd, ENOMEM);
        return false;
    }

    operation->msg->header = msghdr{};
    operation->msg->header.msg_name = args.message_name;
    operation->msg->header.msg_namelen = args.message_name_len;
    if (args.opcode == IORING_OP_RECVMSG && args.multishot && args.buffer_select) {
        operation->msg->header.msg_controllen = args.size;
    } else if (kind.message_iov_op) {
        operation->msg->header.msg_iov = const_cast<iovec *>(args.message_iov);
        operation->msg->header.msg_iovlen = args.message_iov_count;
    } else {
        operation->msg->iov = iovec{args.data, args.size};
        operation->msg->header.msg_iov = &operation->msg->iov;
        operation->msg->header.msg_iovlen = 1;
    }
    operation->msg->address_size = args.message_name_len_out;
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::attach_io_uring_generic_submit_socket_address(
    const IoUringGenericSubmitArgs &args, const IoUringGenericSubmitKind &kind,
    IoUringOperation *operation) noexcept {
    try {
        operation->socket_address = io_uring_address_pool_.create();
    } catch (...) {
        destroy_io_uring_operation(operation);
        set_io_uring_generic_submit_error(args.result, args.fd, ENOMEM);
        return false;
    }

    operation->socket_address->storage = sockaddr_storage{};
    operation->socket_address->output = nullptr;
    operation->socket_address->output_size = nullptr;
    operation->socket_address->output_capacity = 0;
    if (kind.connect_op) {
        std::memcpy(&operation->socket_address->storage, args.socket_address,
                    args.socket_address_size);
        operation->socket_address->size = args.socket_address_size;
    } else {
        operation->socket_address->size = sizeof(operation->socket_address->storage);
        operation->socket_address->output = args.socket_address_out;
        operation->socket_address->output_size = args.socket_address_size_out;
        operation->socket_address->output_capacity =
            args.socket_address_size_out == nullptr ? 0 : *args.socket_address_size_out;
    }
    return true;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_submit_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args, const IoUringGenericSubmitKind &kind,
    IoUringOperation *operation) noexcept {
    initialize_io_uring_generic_submit_sqe(sqe, args, operation);

    if (args.opcode == IORING_OP_FSYNC) {
        sqe.fsync_flags = args.op_flags;
    } else if (kind.close_op) {
        // fd is already filled.
    } else if (kind.shutdown_op) {
        sqe.len = static_cast<unsigned>(args.size);
    } else if (kind.fallocate_op) {
        fill_io_uring_generic_fallocate_sqe(sqe, args);
    } else if (kind.splice_op) {
        fill_io_uring_generic_splice_sqe(sqe, args);
    } else if (kind.openat_op) {
        fill_io_uring_generic_openat_sqe(sqe, args);
    } else if (kind.statx_op) {
        fill_io_uring_generic_statx_sqe(sqe, args);
    } else if (kind.renameat_op) {
        fill_io_uring_generic_renameat_sqe(sqe, args);
    } else if (kind.unlinkat_op) {
        fill_io_uring_generic_unlinkat_sqe(sqe, args);
    } else if (kind.message_op) {
        fill_io_uring_generic_message_sqe(sqe, args, operation);
    } else if (kind.accept_op) {
        fill_io_uring_generic_accept_sqe(sqe, args, operation);
    } else if (kind.connect_op) {
        fill_io_uring_generic_connect_sqe(sqe, operation);
    } else if (kind.fixed_buffer_op) {
        fill_io_uring_generic_fixed_buffer_sqe(sqe, args);
    } else if (args.opcode == IORING_OP_RECV || args.opcode == IORING_OP_SEND ||
               args.opcode == detail::io_uring_op_send_zc) {
        fill_io_uring_generic_socket_data_sqe(sqe, args);
    } else {
        fill_io_uring_generic_buffer_sqe(sqe, args);
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::initialize_io_uring_generic_submit_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args, IoUringOperation *operation) noexcept {
    sqe = io_uring_sqe{};
    sqe.opcode = args.opcode;
    sqe.fd = args.fd;
    sqe.user_data = reinterpret_cast<std::uint64_t>(operation);
    if (args.fixed_file) {
        sqe.flags |= IOSQE_FIXED_FILE;
    }
    if (args.buffer_select) {
        sqe.flags |= IOSQE_BUFFER_SELECT;
        sqe.buf_index = args.provided_buffer_group;
    }
    if (args.direct_file_index >= 0) {
        sqe.file_index = static_cast<std::uint32_t>(args.direct_file_index) + 1U;
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_fallocate_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = args.extra;
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_splice_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = args.extra;
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
    sqe.splice_fd_in = args.extra_fd;
    sqe.splice_flags = args.op_flags;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_openat_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.open_flags = args.op_flags;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_statx_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
    sqe.statx_flags = args.op_flags;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_renameat_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
    sqe.rename_flags = args.op_flags;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_unlinkat_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.unlink_flags = args.op_flags;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_message_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args, IoUringOperation *operation) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(&operation->msg->header);
    sqe.len = 1U;
    sqe.msg_flags = args.op_flags;
    if (args.opcode == IORING_OP_RECVMSG && args.multishot) {
        sqe.ioprio |= IORING_RECV_MULTISHOT;
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_accept_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args, IoUringOperation *operation) noexcept {
    if (operation->socket_address != nullptr) {
        sqe.addr = reinterpret_cast<std::uint64_t>(&operation->socket_address->storage);
        sqe.addr2 = reinterpret_cast<std::uint64_t>(&operation->socket_address->size);
    }
    sqe.accept_flags = args.op_flags;
    if (args.multishot) {
        sqe.ioprio |= IORING_ACCEPT_MULTISHOT;
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_connect_sqe(
    io_uring_sqe &sqe, IoUringOperation *operation) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(&operation->socket_address->storage);
    sqe.off = operation->socket_address->size;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_socket_data_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.msg_flags = args.op_flags;
    if (args.opcode == IORING_OP_RECV && args.multishot) {
        sqe.ioprio |= IORING_RECV_MULTISHOT;
    }
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_fixed_buffer_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
    sqe.buf_index = args.fixed_buffer_index;
}

template <typename RuntimeT, typename TraitsT>
void Executor<RuntimeT, TraitsT>::fill_io_uring_generic_buffer_sqe(
    io_uring_sqe &sqe, const IoUringGenericSubmitArgs &args) noexcept {
    sqe.addr = reinterpret_cast<std::uint64_t>(args.data);
    sqe.len = static_cast<unsigned>(args.size);
    sqe.off = args.offset;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::submit_io_uring_op(
    std::uint8_t opcode, int fd, void *data, std::size_t size, std::uint64_t offset,
    std::uint32_t op_flags, std::uint32_t complete_events, Task *task, IoResult *result,
    sockaddr *message_name, socklen_t message_name_len, socklen_t *message_name_len_out,
    const sockaddr *socket_address, socklen_t socket_address_size, sockaddr *socket_address_out,
    socklen_t *socket_address_size_out, const iovec *message_iov, std::size_t message_iov_count,
    std::uint64_t extra, std::int32_t extra_fd, std::uint16_t fixed_buffer_index, bool fixed_file,
    bool multishot, bool zero_copy_send, std::uint16_t provided_buffer_group, bool buffer_select,
    int direct_file_index) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "io_uring submit must be called from its IO thread");
    if (result != nullptr) {
        result->completion_token = nullptr;
    }

    const IoUringGenericSubmitArgs args{opcode,
                                        fd,
                                        data,
                                        size,
                                        offset,
                                        op_flags,
                                        complete_events,
                                        task,
                                        result,
                                        message_name,
                                        message_name_len,
                                        message_name_len_out,
                                        socket_address,
                                        socket_address_size,
                                        socket_address_out,
                                        socket_address_size_out,
                                        message_iov,
                                        message_iov_count,
                                        extra,
                                        extra_fd,
                                        fixed_buffer_index,
                                        fixed_file,
                                        multishot,
                                        zero_copy_send,
                                        provided_buffer_group,
                                        buffer_select,
                                        direct_file_index};
    const IoUringGenericSubmitKind kind = classify_io_uring_generic_submit(args);
    if (!validate_io_uring_generic_submit(args, kind)) {
        return false;
    }

    IoUringOperation *operation = create_io_uring_generic_submit_operation(args, kind);
    if (operation == nullptr) {
        return false;
    }

    int reserve_error = 0;
    io_uring_sqe *sqe = reserve_io_uring_sqe(reserve_error);
    if (sqe == nullptr) {
        destroy_io_uring_operation(operation);
        set_io_uring_generic_submit_error(result, fd, reserve_error == 0 ? EBUSY : reserve_error);
        return false;
    }

    track_io_uring_operation(operation);
    fill_io_uring_generic_submit_sqe(*sqe, args, kind, operation);

    result->fd = fd;
    result->events = 0;
    result->error = 0;
    result->result = 0;
    result->completion_token = operation;

    if (io_uring_pending_submissions_ >= io_uring_submit_batch_threshold) {
        const int submit_error = flush_io_uring_submissions();
        if (submit_error == 0) {
            return true;
        }
        detail::set_io_result_error(*result, fd, submit_error);
        fail_io_uring_backend(submit_error, operation);
        return false;
    }

    return true;
}
#endif

} // namespace af::detail
