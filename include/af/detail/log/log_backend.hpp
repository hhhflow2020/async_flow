#pragma once

#include <span>

#include "af/detail/log/log_record.hpp"

namespace af {

class LogBackend {
public:
    LogBackend() = default;
    LogBackend(const LogBackend &) = delete;
    LogBackend &operator=(const LogBackend &) = delete;
    virtual ~LogBackend() = default;

    virtual void write_batch(std::span<detail::LogRecord *const> records) noexcept = 0;
    virtual void flush() noexcept {}
};

} // namespace af
