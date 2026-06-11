#pragma once

namespace af::net {

inline send_result tcp_connection_handle::send(af::buffer buffer) const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return send_result::closed;
    }
    detail::tcp_connection_owner *owner = state->owner.load(std::memory_order_acquire);
    if (owner == nullptr) {
        return send_result::closed;
    }
    if (af::runtime::current() == &owner->runtime_owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return owner->send_to_connection(slot_, generation_, std::move(buffer));
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return send_result::closed;
    }
    if (buffer.empty()) {
        return send_result::queued;
    }
    try {
        const bool posted = owner->runtime_owner().post(
            owner_thread_, [state = std::move(state), slot = slot_, generation = generation_,
                            buffer = std::move(buffer)]() mutable {
                if (!state->accepting_operations.load(std::memory_order_acquire)) {
                    return;
                }
                detail::tcp_connection_owner *target = state->owner.load(std::memory_order_acquire);
                if (target == nullptr) {
                    return;
                }
                static_cast<void>(target->send_to_connection(slot, generation, std::move(buffer)));
            });
        return posted ? send_result::queued : send_result::backpressure;
    } catch (...) {
        return send_result::backpressure;
    }
}

inline send_result tcp_connection_handle::send(af::buffer_view view) const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return send_result::closed;
    }
    detail::tcp_connection_owner *owner = state->owner.load(std::memory_order_acquire);
    if (owner == nullptr) {
        return send_result::closed;
    }
    if (af::runtime::current() == &owner->runtime_owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return owner->send_to_connection(slot_, generation_, view);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return send_result::closed;
    }
    try {
        af::buffer buffer = af::buffer::copy(view);
        return send(std::move(buffer));
    } catch (...) {
        return send_result::backpressure;
    }
}

inline bool tcp_connection_handle::pause_read() const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return false;
    }
    detail::tcp_connection_owner *owner = state->owner.load(std::memory_order_acquire);
    if (owner == nullptr) {
        return false;
    }
    if (af::runtime::current() == &owner->runtime_owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return owner->pause_connection_read(slot_, generation_);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return false;
    }
    try {
        return owner->runtime_owner().post(
            owner_thread_, [state = std::move(state), slot = slot_, generation = generation_]() {
                if (!state->accepting_operations.load(std::memory_order_acquire)) {
                    return;
                }
                detail::tcp_connection_owner *target = state->owner.load(std::memory_order_acquire);
                if (target == nullptr) {
                    return;
                }
                static_cast<void>(target->pause_connection_read(slot, generation));
            });
    } catch (...) {
        return false;
    }
}

inline bool tcp_connection_handle::resume_read() const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return false;
    }
    detail::tcp_connection_owner *owner = state->owner.load(std::memory_order_acquire);
    if (owner == nullptr) {
        return false;
    }
    if (af::runtime::current() == &owner->runtime_owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return owner->resume_connection_read(slot_, generation_);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return false;
    }
    try {
        return owner->runtime_owner().post(
            owner_thread_, [state = std::move(state), slot = slot_, generation = generation_]() {
                if (!state->accepting_operations.load(std::memory_order_acquire)) {
                    return;
                }
                detail::tcp_connection_owner *target = state->owner.load(std::memory_order_acquire);
                if (target == nullptr) {
                    return;
                }
                static_cast<void>(target->resume_connection_read(slot, generation));
            });
    } catch (...) {
        return false;
    }
}

inline bool tcp_connection_handle::set_no_delay(bool enabled) const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return false;
    }
    detail::tcp_connection_owner *owner = state->owner.load(std::memory_order_acquire);
    if (owner == nullptr) {
        return false;
    }
    if (af::runtime::current() == &owner->runtime_owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return owner->set_connection_no_delay(slot_, generation_, enabled);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return false;
    }
    try {
        return owner->runtime_owner().post(owner_thread_, [state = std::move(state), slot = slot_,
                                                           generation = generation_, enabled]() {
            if (!state->accepting_operations.load(std::memory_order_acquire)) {
                return;
            }
            detail::tcp_connection_owner *target = state->owner.load(std::memory_order_acquire);
            if (target == nullptr) {
                return;
            }
            static_cast<void>(target->set_connection_no_delay(slot, generation, enabled));
        });
    } catch (...) {
        return false;
    }
}

inline bool tcp_connection_handle::set_keepalive(bool enabled) const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return false;
    }
    detail::tcp_connection_owner *owner = state->owner.load(std::memory_order_acquire);
    if (owner == nullptr) {
        return false;
    }
    if (af::runtime::current() == &owner->runtime_owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return owner->set_connection_keepalive(slot_, generation_, enabled);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return false;
    }
    try {
        return owner->runtime_owner().post(owner_thread_, [state = std::move(state), slot = slot_,
                                                           generation = generation_, enabled]() {
            if (!state->accepting_operations.load(std::memory_order_acquire)) {
                return;
            }
            detail::tcp_connection_owner *target = state->owner.load(std::memory_order_acquire);
            if (target == nullptr) {
                return;
            }
            static_cast<void>(target->set_connection_keepalive(slot, generation, enabled));
        });
    } catch (...) {
        return false;
    }
}

inline bool tcp_connection_handle::close(close_reason reason) const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return false;
    }
    detail::tcp_connection_owner *owner = state->owner.load(std::memory_order_acquire);
    if (owner == nullptr) {
        return false;
    }
    if (af::runtime::current() == &owner->runtime_owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return owner->close_connection(slot_, generation_, reason);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return false;
    }
    try {
        return owner->runtime_owner().post(owner_thread_, [state = std::move(state), slot = slot_,
                                                           generation = generation_, reason]() {
            if (!state->accepting_operations.load(std::memory_order_acquire)) {
                return;
            }
            detail::tcp_connection_owner *target = state->owner.load(std::memory_order_acquire);
            if (target == nullptr) {
                return;
            }
            static_cast<void>(target->close_connection(slot, generation, reason));
        });
    } catch (...) {
        return false;
    }
}

inline bool tcp_connection_handle::close_now(close_reason reason) const noexcept {
    return close(reason);
}

inline bool tcp_connection_handle::close_after_flush() const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return false;
    }
    detail::tcp_connection_owner *owner = state->owner.load(std::memory_order_acquire);
    if (owner == nullptr) {
        return false;
    }
    if (af::runtime::current() == &owner->runtime_owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return owner->close_connection_after_flush(slot_, generation_);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return false;
    }
    try {
        return owner->runtime_owner().post(
            owner_thread_, [state = std::move(state), slot = slot_, generation = generation_]() {
                if (!state->accepting_operations.load(std::memory_order_acquire)) {
                    return;
                }
                detail::tcp_connection_owner *target = state->owner.load(std::memory_order_acquire);
                if (target == nullptr) {
                    return;
                }
                static_cast<void>(target->close_connection_after_flush(slot, generation));
            });
    } catch (...) {
        return false;
    }
}

} // namespace af::net
