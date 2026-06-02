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
    LogRecord() = default;

    explicit LogRecord(std::string_view message) {
        reset(message);
    }

    LogRecord(const LogRecord &) = delete;
    LogRecord &operator=(const LogRecord &) = delete;

    void reset(std::string_view message) {
        assign(message);
    }

    [[nodiscard]] std::string_view message() const noexcept {
        if (uses_heap_) {
            return heap_message_;
        }
        return {inline_message_.data(), size_};
    }

    void set_pool_slot(void *slot) noexcept {
        pool_slot_ = slot;
    }

    [[nodiscard]] void *pool_slot() const noexcept {
        return pool_slot_;
    }

private:
    void assign(std::string_view message) {
        size_ = message.size();
        if (message.size() <= inline_message_.size()) {
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
    void *pool_slot_{nullptr};
};

} // namespace af::detail
