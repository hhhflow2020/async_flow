#if !defined(AF_IO_TYPES_DETAIL_INCLUDE)
#error "io_types_unique_fd.hpp is internal to af/io_types.hpp"
#endif

class UniqueFd {
public:
  UniqueFd() noexcept = default;
  explicit UniqueFd(int fd) noexcept : fd_(fd) {}

  UniqueFd(const UniqueFd &) = delete;
  UniqueFd &operator=(const UniqueFd &) = delete;

  UniqueFd(UniqueFd &&other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

  UniqueFd &operator=(UniqueFd &&other) noexcept {
    if (this != &other) {
      reset(std::exchange(other.fd_, -1));
    }
    return *this;
  }

  ~UniqueFd() { reset(); }

  [[nodiscard]] int get() const noexcept { return fd_; }

  [[nodiscard]] explicit operator bool() const noexcept { return fd_ >= 0; }

  [[nodiscard]] int release() noexcept { return std::exchange(fd_, -1); }

  void reset(int fd = -1) noexcept {
    if (fd_ == fd) {
      return;
    }
#if !defined(_WIN32)
    if (fd_ >= 0) {
      ::close(fd_);
    }
#endif
    fd_ = fd;
  }

private:
  int fd_{-1};
};
