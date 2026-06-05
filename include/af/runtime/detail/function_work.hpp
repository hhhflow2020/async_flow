#pragma once

#include <type_traits>
#include <utility>

#include "af/runtime/work.hpp"

namespace af::detail {

template <typename Fn> class runtime_function_work final : public runtime_work {
public:
    static_assert(std::is_invocable_v<Fn &, runtime &> || std::is_invocable_v<Fn &>,
                  "af::runtime::post callable must be invocable as fn(runtime&) or fn()");

    using destroy_fn = void (*)(runtime_function_work *) noexcept;

    runtime_function_work(destroy_fn destroy,
                          Fn &&fn) noexcept(std::is_nothrow_move_constructible_v<Fn>)
        : destroy_(destroy), fn_(std::move(fn)) {}

    runtime_function_work(destroy_fn destroy, const Fn &fn) : destroy_(destroy), fn_(fn) {}

    void run(runtime &owner) noexcept override {
        try {
            if constexpr (std::is_invocable_v<Fn &, runtime &>) {
                fn_(owner);
            } else {
                fn_();
            }
        } catch (...) {
        }
        destroy();
    }

    void destroy() noexcept {
        destroy_(this);
    }

private:
    destroy_fn destroy_{nullptr};
    Fn fn_;
};

} // namespace af::detail
