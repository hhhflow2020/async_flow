#pragma once

template <typename ThreadT> class IoDatagramSocket : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus bind(TaskT &task, const sockaddr *address,
                                socklen_t address_size) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_bind(task, this->thread_, this->fd_, address, address_size);
    }
    template <typename TaskT>
    [[nodiscard]] IoStatus recv_from_some(TaskT &task, void *data, std::size_t size,
                                          sockaddr *address, socklen_t *address_size,
                                          IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_recv_from_some(task, this->thread_, this->fd_, data, size, address,
                                     address_size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus recv_multishot(TaskT &task, std::uint16_t buffer_group,
                                          std::uint16_t *buffer_id, IoOpState &state,
                                          std::uint32_t flags = 0) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_recv_multishot(task, this->thread_, this->fd_, buffer_group, buffer_id, state,
                                     flags);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus
    recv_from_multishot(TaskT &task, std::uint16_t buffer_group, socklen_t name_capacity,
                        std::size_t control_capacity, std::uint16_t *buffer_id, IoOpState &state,
                        std::uint32_t flags = 0) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_recvmsg_multishot(task, this->thread_, this->fd_, buffer_group, name_capacity,
                                        control_capacity, buffer_id, state, flags);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus recvv_from_some(TaskT &task, const iovec *iov, int iov_count,
                                           sockaddr *address, socklen_t *address_size,
                                           IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_recvv_from_some(task, this->thread_, this->fd_, iov, iov_count, address,
                                      address_size, state);
    }
    template <typename TaskT>
    [[nodiscard]] IoStatus send_to_some(TaskT &task, const void *data, std::size_t size,
                                        const sockaddr *address, socklen_t address_size,
                                        IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_send_to_some(task, this->thread_, this->fd_, data, size, address,
                                   address_size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus send_zc_to_some(TaskT &task, const void *data, std::size_t size,
                                           const sockaddr *address, socklen_t address_size,
                                           IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_send_zc_to_some(task, this->thread_, this->fd_, data, size, address,
                                      address_size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_to_some(TaskT &task, const iovec *iov, int iov_count,
                                         const sockaddr *address, socklen_t address_size,
                                         IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_sendv_to_some(task, this->thread_, this->fd_, iov, iov_count, address,
                                    address_size, state);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus sendv_zc_to_some(TaskT &task, const iovec *iov, int iov_count,
                                            const sockaddr *address, socklen_t address_size,
                                            IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_sendv_zc_to_some(task, this->thread_, this->fd_, iov, iov_count, address,
                                       address_size, state);
    }
};
