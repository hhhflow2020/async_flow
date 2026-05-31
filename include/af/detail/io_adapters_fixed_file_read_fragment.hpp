#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_fixed_file_read_fragment.hpp is an io_adapters implementation fragment"
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
