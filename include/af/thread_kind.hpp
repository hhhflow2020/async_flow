#pragma once

#include <cstdint>

namespace af {

enum class thread_kind : std::uint8_t {
    cpu,
    io,
};

using ThreadKind = thread_kind;

} // namespace af
