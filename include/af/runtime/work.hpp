#pragma once

#include "af/queue/intrusive_mpsc_queue.hpp"

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
    detail::intrusive_mpsc_node<runtime_work> intrusive_mpsc_node_{this};

    template <typename T> friend class detail::intrusive_mpsc_queue;
};

} // namespace af
