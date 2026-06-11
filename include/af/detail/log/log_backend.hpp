#pragma once

#include <chrono>
#include "af/span.hpp"

#include "af/detail/log/log_record.hpp"

namespace af {

class log_backend {
public:
    log_backend() = default;
    log_backend(const log_backend &) = delete;
    log_backend &operator=(const log_backend &) = delete;
    virtual ~log_backend() = default;

    virtual void write_batch(af::span<detail::log_record *const> records) noexcept = 0;
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

using LogBackend = log_backend;

} // namespace af
