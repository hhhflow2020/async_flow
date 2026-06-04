#pragma once

#include <atomic>

namespace af::detail {

template <typename TaskT, bool Enabled> struct RuntimeTaskRegistryState {};

template <typename TaskT> struct RuntimeTaskRegistryState<TaskT, true> {
    std::atomic<TaskT *> pending_head{nullptr};
};

} // namespace af::detail
