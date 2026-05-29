#pragma once

#include <cassert>
#include <cstddef>

#ifndef CAF_ENABLE_ASSERTS
#if defined(NDEBUG)
#define CAF_ENABLE_ASSERTS 0
#else
#define CAF_ENABLE_ASSERTS 1
#endif
#endif

#if CAF_ENABLE_ASSERTS
#define CAF_ASSERT(expr) assert(expr)
#else
#define CAF_ASSERT(expr) static_cast<void>(sizeof(expr))
#endif

namespace caf::detail {

inline constexpr std::size_t hardware_cache_line_size = 64;

} // namespace caf::detail
