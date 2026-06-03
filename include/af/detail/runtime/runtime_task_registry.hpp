#pragma once

#include <mutex>

namespace af::detail {

template <typename TaskT, bool Enabled> struct RuntimeTaskRegistryState {};

template <typename TaskT> struct RuntimeTaskRegistryState<TaskT, true> {
    std::mutex mutex;
    TaskT *head{nullptr};
};

} // namespace af::detail
