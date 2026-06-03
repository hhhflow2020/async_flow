#pragma once

#include <cstddef>
#include <limits>
#include <stdexcept>

namespace af::detail {

inline constexpr std::size_t max_power_of_two_size =
    std::size_t{1} << (std::numeric_limits<std::size_t>::digits - 1U);

inline constexpr std::size_t max_bounded_queue_capacity =
    std::size_t{1} << (std::numeric_limits<std::size_t>::digits - 2U);

[[nodiscard]] inline constexpr bool bounded_queue_sequence_before(std::size_t sequence,
                                                                  std::size_t position) noexcept {
    return sequence - position >= max_power_of_two_size;
}

inline constexpr std::size_t next_power_of_two(std::size_t value) noexcept {
    if (value <= 1U) {
        return 1U;
    }
    if (value > max_power_of_two_size) {
        return 0U;
    }

    --value;
    for (std::size_t shift = 1U; shift < std::numeric_limits<std::size_t>::digits; shift <<= 1U) {
        value |= value >> shift;
    }
    return value + 1U;
}

[[nodiscard]] inline std::size_t checked_next_power_of_two(std::size_t value, const char *what) {
    const std::size_t capacity = next_power_of_two(value);
    if (capacity == 0U) [[unlikely]] {
        throw std::length_error(what);
    }
    return capacity;
}

[[nodiscard]] inline std::size_t normalize_bounded_queue_capacity(std::size_t requested) {
    const std::size_t value = requested < 2U ? 2U : requested;
    const std::size_t capacity =
        checked_next_power_of_two(value, "bounded queue capacity is too large");
    if (capacity > max_bounded_queue_capacity) [[unlikely]] {
        throw std::length_error("bounded queue capacity is too large");
    }
    return capacity;
}

} // namespace af::detail
