#pragma once

#include <cstdint>
#include <vector>

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

} // namespace af::net
