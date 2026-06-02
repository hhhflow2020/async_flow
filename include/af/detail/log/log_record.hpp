#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "af/detail/config.hpp"

namespace af::detail {

inline constexpr std::size_t default_log_inline_message_bytes = 1024;

class alignas(hardware_cache_line_size) LogRecord {
public:
    explicit LogRecord(std::string_view message) {
        assign(message);
    }

    LogRecord(const LogRecord &) = delete;
    LogRecord &operator=(const LogRecord &) = delete;

    [[nodiscard]] std::string_view message() const noexcept {
        if (uses_heap_) {
            return heap_message_;
        }
        return {inline_message_.data(), size_};
    }

private:
    void assign(std::string_view message) {
        size_ = message.size();
        if (message.size() <= inline_message_.size()) {
            inline_message_.fill('\0');
            std::copy(message.begin(), message.end(), inline_message_.begin());
            uses_heap_ = false;
            return;
        }

        heap_message_.assign(message.data(), message.size());
        uses_heap_ = true;
    }

    std::array<char, default_log_inline_message_bytes> inline_message_{};
    std::string heap_message_;
    std::size_t size_{0};
    bool uses_heap_{false};
};

} // namespace af::detail
