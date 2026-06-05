#pragma once

namespace af::net {

inline send_result tcp_connection_handle::send(af::Buffer buffer) const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return send_result::closed;
    }
    tcp_server *server = state->server.load(std::memory_order_acquire);
    if (server == nullptr) {
        return send_result::closed;
    }
    if (af::runtime::current() == &server->owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return server->send_to_connection(slot_, generation_, std::move(buffer));
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return send_result::closed;
    }
    if (buffer.empty()) {
        return send_result::queued;
    }
    try {
        const bool posted = server->owner().post(
            owner_thread_, [state = std::move(state), slot = slot_, generation = generation_,
                            buffer = std::move(buffer)]() mutable {
                if (!state->accepting_operations.load(std::memory_order_acquire)) {
                    return;
                }
                tcp_server *target = state->server.load(std::memory_order_acquire);
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

inline send_result tcp_connection_handle::send(af::BufferView view) const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return send_result::closed;
    }
    tcp_server *server = state->server.load(std::memory_order_acquire);
    if (server == nullptr) {
        return send_result::closed;
    }
    if (af::runtime::current() == &server->owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return server->send_to_connection(slot_, generation_, view);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return send_result::closed;
    }
    try {
        af::Buffer buffer = af::Buffer::copy(view);
        return send(std::move(buffer));
    } catch (...) {
        return send_result::backpressure;
    }
}

inline bool tcp_connection_handle::close(close_reason reason) const noexcept {
    auto state = state_.lock();
    if (state == nullptr) {
        return false;
    }
    tcp_server *server = state->server.load(std::memory_order_acquire);
    if (server == nullptr) {
        return false;
    }
    if (af::runtime::current() == &server->owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return server->close_connection(slot_, generation_, reason);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return false;
    }
    try {
        return server->owner().post(owner_thread_, [state = std::move(state), slot = slot_,
                                                    generation = generation_, reason]() {
            if (!state->accepting_operations.load(std::memory_order_acquire)) {
                return;
            }
            tcp_server *target = state->server.load(std::memory_order_acquire);
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
    tcp_server *server = state->server.load(std::memory_order_acquire);
    if (server == nullptr) {
        return false;
    }
    if (af::runtime::current() == &server->owner() &&
        af::runtime::current_thread_index() == owner_thread_.index) {
        return server->close_connection_after_flush(slot_, generation_);
    }
    if (!state->accepting_operations.load(std::memory_order_acquire)) {
        return false;
    }
    try {
        return server->owner().post(
            owner_thread_, [state = std::move(state), slot = slot_, generation = generation_]() {
                if (!state->accepting_operations.load(std::memory_order_acquire)) {
                    return;
                }
                tcp_server *target = state->server.load(std::memory_order_acquire);
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
