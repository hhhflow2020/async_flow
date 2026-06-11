#pragma once

#include <cstdint>

#include "af/memory/cache_line.hpp"

namespace af::detail {

struct alignas(hardware_cache_line_size) ordered_batch_state {
    std::uint64_t last_applied_batch_id{0};
};

} // namespace af::detail
