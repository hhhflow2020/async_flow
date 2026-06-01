#pragma once

#include <cstddef>

namespace af::detail {

inline constexpr std::size_t next_power_of_two(std::size_t value) noexcept {
    std::size_t result = 1;
    while (result < value) {
        result <<= 1U;
    }
    return result;
}

} // namespace af::detail
