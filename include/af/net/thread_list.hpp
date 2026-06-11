#pragma once

#include <vector>

#include "af/runtime/config_types.hpp"

namespace af::net {

[[nodiscard]] inline std::vector<af::thread_ref> thread_list(af::thread_group_ref group) {
    std::vector<af::thread_ref> result;
    result.reserve(group.size());
    for (std::uint16_t thread : group) {
        result.emplace_back(thread);
    }
    return result;
}

} // namespace af::net
