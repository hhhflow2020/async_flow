#pragma once

template <typename ThreadT> class IoListener : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

    template <typename TaskT>
    [[nodiscard]] IoStatus bind(TaskT &task, const sockaddr *address,
                                socklen_t address_size) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoListener thread type must match the task runtime thread type");
        return af::io_bind(task, this->thread_, this->fd_, address, address_size);
    }

    template <typename TaskT>
    [[nodiscard]] IoStatus listen(TaskT &task, int backlog) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoListener thread type must match the task runtime thread type");
        return af::io_listen(task, this->thread_, this->fd_, backlog);
    }
    template <typename TaskT>
    [[nodiscard]] IoStatus
    accept_some(TaskT &task, sockaddr *address, socklen_t *address_size, int *accepted_fd,
                IoOpState &state, int flags = detail::io_default_accept_flags()) const noexcept {
        static_assert(std::is_same_v<typename TaskT::Thread, ThreadT>,
                      "IoListener thread type must match the task runtime thread type");
        return af::io_accept_some(task, this->thread_, this->fd_, address, address_size,
                                  accepted_fd, state, flags);
    }
};
