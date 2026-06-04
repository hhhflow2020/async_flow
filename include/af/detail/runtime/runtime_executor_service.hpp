#pragma once

#if !defined(AF_ASYNC_RUNTIME_IMPL_INCLUDE)
#error "runtime_executor_service.hpp must be included by async_runtime.hpp"
#endif

namespace af::detail {

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::register_service_task(detail::RuntimeServiceTask *service) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "service task registration must run on the owner runtime thread");
    if (RuntimeT::current_thread_index_ != index_ || service == nullptr) {
        return false;
    }

    if (std::find(service_tasks_.begin(), service_tasks_.end(), service) != service_tasks_.end()) {
        return true;
    }

    try {
        service_tasks_.push_back(service);
        return true;
    } catch (...) {
        return false;
    }
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool
Executor<RuntimeT, TraitsT>::unregister_service_task(detail::RuntimeServiceTask *service) noexcept {
    AF_ASSERT(RuntimeT::current_thread_index_ == index_ &&
              "service task unregister must run on the owner runtime thread");
    if (RuntimeT::current_thread_index_ != index_ || service == nullptr) {
        return false;
    }

    auto it = std::find(service_tasks_.begin(), service_tasks_.end(), service);
    if (it == service_tasks_.end()) {
        return false;
    }

    const std::size_t removed_index = static_cast<std::size_t>(it - service_tasks_.begin());
    service_tasks_.erase(it);
    if (next_service_task_ > service_tasks_.size()) {
        next_service_task_ = 0;
    } else if (next_service_task_ != 0U && next_service_task_ > removed_index) {
        --next_service_task_;
    }
    return true;
}

template <typename RuntimeT, typename TraitsT>
[[nodiscard]] bool Executor<RuntimeT, TraitsT>::run_service_tasks() noexcept {
    if (service_tasks_.empty()) {
        return false;
    }

    bool did_work = false;
    const std::size_t count = service_tasks_.size();
    const std::size_t budget = service_task_budget < count ? service_task_budget : count;
    for (std::size_t i = 0; i < budget; ++i) {
        if (next_service_task_ >= service_tasks_.size()) {
            next_service_task_ = 0;
        }

        detail::RuntimeServiceTask *service = service_tasks_[next_service_task_];
        ++next_service_task_;
        if (service == nullptr) [[unlikely]] {
            continue;
        }
        did_work = service->run_service(service_task_budget) || did_work;
    }
    return did_work;
}

} // namespace af::detail
