#pragma once

#include <type_traits>
#include <utility>

#include "af/detail/queue/intrusive_mpsc_queue.hpp"
#include "af/runtime/detail/pooled_object.hpp"

namespace af {

class runtime;

class runtime_work {
public:
    runtime_work() = default;
    runtime_work(const runtime_work &) = delete;
    runtime_work &operator=(const runtime_work &) = delete;
    virtual ~runtime_work() = default;

    virtual void run(runtime &owner) noexcept = 0;

private:
    detail::IntrusiveMpscNode<runtime_work> intrusive_mpsc_node_{this};

    template <typename T> friend class detail::IntrusiveMpscQueue;
};

namespace detail {

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

} // namespace detail

} // namespace af
