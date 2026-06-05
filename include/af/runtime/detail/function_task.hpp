#pragma once

#include <chrono>
#include <type_traits>
#include <utility>

#include "af/runtime/task.hpp"

namespace af::detail {

template <typename Fn> class runtime_delayed_function_task final : public runtime_task {
public:
    static_assert(std::is_invocable_v<Fn &, runtime &> || std::is_invocable_v<Fn &>,
                  "af::runtime::schedule_after callable must be invocable as fn(runtime&) or "
                  "fn()");

    runtime_delayed_function_task(runtime_task::factory_token token, runtime &owner, Fn &&fn)
        : runtime_task(token, owner), fn_(std::move(fn)) {}

    runtime_delayed_function_task(runtime_task::factory_token token, runtime &owner, const Fn &fn)
        : runtime_task(token, owner), fn_(fn) {}

    template <typename Rep, typename Period>
    [[nodiscard]] bool do_after(std::uint16_t thread,
                                std::chrono::duration<Rep, Period> delay) noexcept {
        return schedule_after(thread, delay);
    }

    template <typename Clock, typename Duration>
    [[nodiscard]] bool do_at(std::uint16_t thread,
                             std::chrono::time_point<Clock, Duration> time) noexcept {
        return schedule_at(thread, time);
    }

private:
    task_result run_task() noexcept override {
        try {
            if constexpr (std::is_invocable_v<Fn &, runtime &>) {
                fn_(owner());
            } else {
                fn_();
            }
        } catch (...) {
        }
        return done();
    }

    Fn fn_;
};

} // namespace af::detail
