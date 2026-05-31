#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_fixed_file_recv_fragment.hpp is an io_adapters implementation fragment"
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
#endif
