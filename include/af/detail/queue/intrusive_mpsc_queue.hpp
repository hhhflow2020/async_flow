#pragma once

#include <atomic>

#include "af/detail/config.hpp"
#include "af/detail/queue/queue_backoff.hpp"

namespace af::detail {

template <typename T> struct IntrusiveMpscNode {
    IntrusiveMpscNode() noexcept = default;

    explicit IntrusiveMpscNode(T *owner) noexcept : owner(owner) {}

    std::atomic<IntrusiveMpscNode *> next{nullptr};
    T *owner{nullptr};
};

template <typename T> class IntrusiveMpscQueue {
public:
    using Node = IntrusiveMpscNode<T>;

    IntrusiveMpscQueue() noexcept {
        head_.store(&stub_, std::memory_order_relaxed);
        tail_ = &stub_;
    }

    IntrusiveMpscQueue(const IntrusiveMpscQueue &) = delete;
    IntrusiveMpscQueue &operator=(const IntrusiveMpscQueue &) = delete;

    void push(T *value) noexcept {
        AF_ASSERT(value != nullptr);
        push_node(&node(*value));
    }

    [[nodiscard]] T *try_pop() noexcept {
        QueueFullBackoff inconsistent_backoff(inconsistent_spin_count);
        for (;;) {
            Node *tail = tail_;
            Node *next = tail->next.load(std::memory_order_acquire);
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
        Node *tail = tail_;
        return tail->next.load(std::memory_order_acquire) == nullptr &&
               tail == head_.load(std::memory_order_acquire);
    }

private:
    static constexpr std::size_t inconsistent_spin_count = 64;

    [[nodiscard]] static Node &node(T &value) noexcept {
        return value.intrusive_mpsc_node_;
    }

    T *consume_linked(Node *tail, Node *next) noexcept {
        tail_ = next;
        tail->next.store(nullptr, std::memory_order_relaxed);
        if (tail == &stub_) {
            return nullptr;
        }
        return tail->owner;
    }

    void push_node(Node *node) noexcept {
        node->next.store(nullptr, std::memory_order_relaxed);
        Node *previous = head_.exchange(node, std::memory_order_acq_rel);
        previous->next.store(node, std::memory_order_release);
    }

    alignas(hardware_cache_line_size) std::atomic<Node *> head_{nullptr};
    alignas(hardware_cache_line_size) Node *tail_{nullptr};
    Node stub_{};
};

} // namespace af::detail
