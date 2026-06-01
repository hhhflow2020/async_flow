#pragma once

inline constexpr std::uint32_t io_readable = 1U << 0U;
inline constexpr std::uint32_t io_writable = 1U << 1U;
inline constexpr std::uint32_t io_error = 1U << 2U;
inline constexpr std::uint32_t io_hangup = 1U << 3U;
inline constexpr std::uint32_t io_more = 1U << 4U;
inline constexpr std::uint32_t io_buffer_selected = 1U << 5U;
inline constexpr std::uint32_t io_buffer_id_shift = 16U;
inline constexpr std::uint32_t io_buffer_id_mask = 0xffff0000U;

struct IoResult {
  int fd{-1};
  std::uint32_t events{0};
  int error{0};
  std::int64_t result{0};
  void *completion_token{nullptr};

  [[nodiscard]] bool readable() const noexcept {
    return (events & io_readable) != 0U;
  }

  [[nodiscard]] bool writable() const noexcept {
    return (events & io_writable) != 0U;
  }

  [[nodiscard]] bool failed() const noexcept {
    return error != 0 || (events & (io_error | io_hangup)) != 0U;
  }

  [[nodiscard]] bool buffer_selected() const noexcept {
    return (events & io_buffer_selected) != 0U;
  }

  [[nodiscard]] std::uint16_t buffer_id() const noexcept {
    return static_cast<std::uint16_t>((events & io_buffer_id_mask) >>
                                      io_buffer_id_shift);
  }
};

enum class IoWaitKind : std::uint8_t {
  None,
  Readiness,
  Completion,
};

struct IoOpState {
  IoResult wait{};
  IoWaitKind wait_kind{IoWaitKind::None};
  bool waiting{false};

  void reset() noexcept {
    wait = IoResult{};
    wait_kind = IoWaitKind::None;
    waiting = false;
  }
};
