#pragma once

#include <array>
#include <cstdio>
#include <cstdint>
#include <string_view>

#include "af/detail/config.hpp"

#if !defined(_WIN32)
#include <pthread.h>
#endif

namespace af::detail {

inline void set_current_thread_name(std::string_view group_name,
                                    std::uint16_t group_offset) noexcept {
#if !defined(_WIN32)
    std::array<char, 16> name{};
    const auto offset = static_cast<unsigned>(group_offset);
    const int offset_digits = std::snprintf(nullptr, 0, "%u", offset);
    if (offset_digits <= 0) {
        return;
    }

    constexpr std::size_t max_name_chars = 15;
    const std::size_t fixed_chars = 4U + static_cast<std::size_t>(offset_digits);
    std::size_t group_chars = 0;
    if (fixed_chars < max_name_chars) {
        group_chars = max_name_chars - fixed_chars;
        if (group_chars > group_name.size()) {
            group_chars = group_name.size();
        }
    }

    const int written = std::snprintf(name.data(), name.size(), "af-%.*s-%u",
                                      static_cast<int>(group_chars), group_name.data(), offset);
    if (written <= 0) {
        return;
    }
#if defined(__APPLE__)
    static_cast<void>(::pthread_setname_np(name.data()));
#elif defined(__linux__)
    static_cast<void>(::pthread_setname_np(::pthread_self(), name.data()));
#endif
#else
    static_cast<void>(group_name);
    static_cast<void>(group_offset);
#endif
}

} // namespace af::detail
