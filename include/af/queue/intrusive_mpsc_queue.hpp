#pragma once

#include <atomic>

#include "af/detail/config.hpp"
#include "af/memory/cache_line.hpp"
#include "af/queue/queue_backoff.hpp"

namespace af::detail {

template <typename T> struct intrusive_mpsc_node {
    intrusive_mpsc_node() noexcept = default;

    explicit intrusive_mpsc_node(T *owner) noexcept : owner(owner) {}

    std::atomic<intrusive_mpsc_node *> next{nullptr};
    T *owner{nullptr};
};

template <typename T> class intrusive_mpsc_queue {
public:
    using node_type = intrusive_mpsc_node<T>;

    intrusive_mpsc_queue() noexcept {
        head_.store(&stub_, std::memory_order_relaxed);
        tail_ = &stub_;
    }

    intrusive_mpsc_queue(const intrusive_mpsc_queue &) = delete;
    intrusive_mpsc_queue &operator=(const intrusive_mpsc_queue &) = delete;

    void push(T *value) noexcept {
        AF_ASSERT(value != nullptr);
        push_node(&node(*value));
    }

    [[nodiscard]] T *try_pop() noexcept {
        queue_full_backoff inconsistent_backoff(inconsistent_spin_count);
        for (;;) {
            node_type *tail = tail_;
            node_type *next = tail->next.load(std::memory_order_acquire);
            if (next != nullptr) {
                T *value = consume_linked(tail, next);
                if (value == nullptr) {
                    continue;
                }
                return value;
            }

            if (tail != head_.load(std::memory_order_acquire)) {
                bool consumed_stub = false;
                for (std::size_t spin = 0; spin < inconsistent_spin_count; ++spin) {
                    queue_full_cpu_relax();
                    next = tail->next.load(std::memory_order_acquire);
                    if (next != nullptr) {
                        T *value = consume_linked(tail, next);
                        if (value == nullptr) {
                            consumed_stub = true;
                            break;
                        }
                        return value;
                    }
                }
                if (consumed_stub) {
                    continue;
                }
                inconsistent_backoff.wait();
                continue;
            }
            if (tail == &stub_) {
                return nullptr;
            }
            inconsistent_backoff.reset();
            push_node(&stub_);
        }
    }

    [[nodiscard]] bool empty() const noexcept {
        node_type *tail = tail_;
        return tail == &stub_ && tail->next.load(std::memory_order_acquire) == nullptr &&
               tail == head_.load(std::memory_order_acquire);
    }

private:
    static constexpr std::size_t inconsistent_spin_count = 64;

    [[nodiscard]] static node_type &node(T &value) noexcept {
        return value.intrusive_mpsc_node_;
    }

    T *consume_linked(node_type *tail, node_type *next) noexcept {
        tail_ = next;
        tail->next.store(nullptr, std::memory_order_relaxed);
        if (tail == &stub_) {
            return nullptr;
        }
        return tail->owner;
    }

    void push_node(node_type *node) noexcept {
        node->next.store(nullptr, std::memory_order_relaxed);
        node_type *previous = head_.exchange(node, std::memory_order_acq_rel);
        previous->next.store(node, std::memory_order_release);
    }

    alignas(hardware_cache_line_size) std::atomic<node_type *> head_{nullptr};
    alignas(hardware_cache_line_size) node_type *tail_{nullptr};
    node_type stub_{};
};

} // namespace af::detail
