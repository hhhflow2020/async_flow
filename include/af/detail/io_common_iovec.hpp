#if !defined(AF_IO_COMMON_DETAIL_INCLUDE)
#error "io_common_iovec.hpp is internal to af/io_common.hpp"
#endif

#if !defined(_WIN32)
[[nodiscard]] inline int io_max_iov() noexcept {
#if defined(IOV_MAX)
  return IOV_MAX;
#else
  return 1024;
#endif
}

[[nodiscard]] inline bool io_validate_iov(const iovec *iov, int iov_count,
                                          std::size_t &total_size,
                                          int &error) noexcept {
  total_size = 0;
  error = 0;
  if (iov_count < 0 || iov_count > io_max_iov()) {
    error = EINVAL;
    return false;
  }
  if (iov_count == 0) {
    return true;
  }
  if (iov == nullptr) {
    error = EINVAL;
    return false;
  }

  for (int i = 0; i < iov_count; ++i) {
    const std::size_t len = iov[i].iov_len;
    if (len != 0U && iov[i].iov_base == nullptr) {
      error = EINVAL;
      return false;
    }
    if (len > std::numeric_limits<std::size_t>::max() - total_size) {
      error = EOVERFLOW;
      return false;
    }
    total_size += len;
  }
  return true;
}
#endif
