#pragma once

#include <cstdint>
#include <vector>

#include "af/runtime/config_types.hpp"

namespace af::net {

template <typename Runtime, typename Group>
[[nodiscard]] std::vector<typename Runtime::Thread> thread_list(Group) {
    std::vector<typename Runtime::Thread> result;
    result.reserve(Group::count);
    for (std::uint16_t i = 0; i < Group::count; ++i) {
        result.push_back(Group::at(i));
    }
    return result;
}

[[nodiscard]] inline std::vector<af::thread_ref> thread_list(af::thread_group_ref group) {
    std::vector<af::thread_ref> result;
    result.reserve(group.size());
    for (std::uint16_t thread : group) {
        result.emplace_back(thread);
    }
    return result;
}

} // namespace af::net
