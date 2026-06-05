#pragma once

#include <chrono>
#include "af/span.hpp"

#include "af/detail/log/log_record.hpp"

namespace af {

class LogBackend {
public:
    LogBackend() = default;
    LogBackend(const LogBackend &) = delete;
    LogBackend &operator=(const LogBackend &) = delete;
    virtual ~LogBackend() = default;

    virtual void write_batch(af::Span<detail::LogRecord *const> records) noexcept = 0;
    virtual void flush() noexcept {}
    [[nodiscard]] virtual bool flush(std::chrono::milliseconds timeout) noexcept {
        static_cast<void>(timeout);
        flush();
        return true;
    }
    virtual void shutdown() noexcept {
        flush();
    }
};

using log_backend = LogBackend;

} // namespace af
