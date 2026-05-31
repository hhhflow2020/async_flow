#if !defined(AF_TASK_FRAGMENT_INCLUDE)
#error "basic_task_fragment.hpp is a task implementation fragment"
#endif

template <typename RuntimeT>
class BasicTask {
public:
#include "af/detail/basic_task_public_fragment.hpp"

protected:
#include "af/detail/basic_task_protected_fragment.hpp"

private:
    virtual TaskResult run() = 0;
    virtual void on_runtime_cancel() noexcept {}

#include "af/detail/basic_task_lifetime_fragment.hpp"
#include "af/detail/basic_task_schedule_fragment.hpp"
#include "af/detail/basic_task_storage_fragment.hpp"
};
