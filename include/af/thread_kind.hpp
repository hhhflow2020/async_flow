#pragma once

#include <cstdint>

namespace af {

enum class ThreadKind : std::uint8_t {
    Worker,
    Log,
    Io,
    Epoll,
    Kqueue,
};

} // namespace af
