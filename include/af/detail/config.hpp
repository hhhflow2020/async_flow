#pragma once

#include <cassert>
#include <cstddef>

#ifndef AF_ENABLE_ASSERTS
#if defined(NDEBUG)
#define AF_ENABLE_ASSERTS 0
#else
#define AF_ENABLE_ASSERTS 1
#endif
#endif

#if AF_ENABLE_ASSERTS
#define AF_ASSERT(expr) assert(expr)
#else
#define AF_ASSERT(expr) static_cast<void>(sizeof(expr))
#endif

namespace af::detail {

inline constexpr std::size_t hardware_cache_line_size = 64;

} // namespace af::detail
