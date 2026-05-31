#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_file_descriptor_fixed_fragment.hpp is an io_adapters implementation fragment"
#endif

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
#endif
