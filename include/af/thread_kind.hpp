#pragma once

#include <cstdint>

namespace af {

enum class thread_kind : std::uint8_t {
    cpu,
    io,
};

} // namespace af
