#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_fixed_file_sync_fragment.hpp is an io_adapters implementation fragment"
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
