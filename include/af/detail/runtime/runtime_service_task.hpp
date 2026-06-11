#pragma once

#include <cstddef>

namespace af::detail {

class runtime_service_task {
public:
    runtime_service_task() = default;
    runtime_service_task(const runtime_service_task &) = delete;
    runtime_service_task &operator=(const runtime_service_task &) = delete;
    virtual ~runtime_service_task() = default;

    [[nodiscard]] virtual bool run_service(std::size_t budget) noexcept = 0;
};

using RuntimeServiceTask = runtime_service_task;

} // namespace af::detail
