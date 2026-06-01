#pragma once

#include <cstdint>

namespace af {

enum class ThreadKind : std::uint8_t {
    Worker,
    Io,
    IoUring,
    Epoll,
    Kqueue,
};

} // namespace af
