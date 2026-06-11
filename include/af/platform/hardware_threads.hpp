#pragma once

#include <cstddef>
#include <thread>

namespace af::detail {

[[nodiscard]] inline std::size_t hardware_thread_count() noexcept {
    return static_cast<std::size_t>(std::thread::hardware_concurrency());
}

} // namespace af::detail
