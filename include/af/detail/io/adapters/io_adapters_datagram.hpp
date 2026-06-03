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
    [[nodiscard]] IoStatus sendv_to_some(TaskT &task, const iovec *iov, int iov_count,
                                         const sockaddr *address, socklen_t address_size,
                                         IoOpState &state) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoDatagramSocket thread type must match the task runtime thread type");
        return af::io_sendv_to_some(task, this->thread_, this->fd_, iov, iov_count, address,
                                    address_size, state);
    }
};
