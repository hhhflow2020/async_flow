#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_file_descriptor_write_fragment.hpp is an io_adapters implementation fragment"
#endif

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
