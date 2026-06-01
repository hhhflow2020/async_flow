#pragma once

#include <utility>

#include "af/detail/config.hpp"

namespace af::detail {

template <typename RuntimeT, typename TaskT>
class [[nodiscard]] RuntimeTaskHandle {
public:
  RuntimeTaskHandle() noexcept = default;
  explicit RuntimeTaskHandle(TaskT *task) noexcept : task_(task) {}

  RuntimeTaskHandle(const RuntimeTaskHandle &) = delete;
  RuntimeTaskHandle &operator=(const RuntimeTaskHandle &) = delete;

  RuntimeTaskHandle(RuntimeTaskHandle &&other) noexcept
      : task_(std::exchange(other.task_, nullptr)) {}

  RuntimeTaskHandle &operator=(RuntimeTaskHandle &&other) noexcept {
    if (this != &other) {
      reset();
      task_ = std::exchange(other.task_, nullptr);
    }
    return *this;
  }

  ~RuntimeTaskHandle() { reset(); }

  [[nodiscard]] TaskT *get() const noexcept { return task_; }

  [[nodiscard]] TaskT &operator*() const noexcept {
    AF_ASSERT(task_ != nullptr);
    return *task_;
  }

  [[nodiscard]] TaskT *operator->() const noexcept {
    AF_ASSERT(task_ != nullptr);
    return task_;
  }

  [[nodiscard]] explicit operator bool() const noexcept {
    return task_ != nullptr;
  }

  [[nodiscard]] bool scheduled() const noexcept {
    return task_ != nullptr && !RuntimeT::is_task_created(task_);
  }

  void reset() noexcept {
    if (task_ != nullptr) {
      RuntimeT::release_task_handle(task_);
      task_ = nullptr;
    }
  }

private:
  TaskT *task_{nullptr};
};

} // namespace af::detail
