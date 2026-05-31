#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_datagram_lifecycle_fragment.hpp is an io_adapters implementation fragment"
#endif

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
