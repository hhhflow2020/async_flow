#pragma once

#include <cstddef>

namespace af::detail {

class RuntimeServiceTask {
public:
    RuntimeServiceTask() = default;
    RuntimeServiceTask(const RuntimeServiceTask &) = delete;
    RuntimeServiceTask &operator=(const RuntimeServiceTask &) = delete;
    virtual ~RuntimeServiceTask() = default;

    [[nodiscard]] virtual bool run_service(std::size_t budget) noexcept = 0;
};

} // namespace af::detail
