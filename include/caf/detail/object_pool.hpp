#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "caf/detail/config.hpp"

namespace caf::detail {

template <typename T, std::size_t ChunkSize = 256>
class ObjectPool {
public:
    ObjectPool() = default;
    ObjectPool(const ObjectPool&) = delete;
    ObjectPool& operator=(const ObjectPool&) = delete;

    ~ObjectPool() {
        Block* block = blocks_.load(std::memory_order_relaxed);
        while (block != nullptr) {
            Block* next = block->next;
            delete block;
            block = next;
        }
    }

    template <typename... Args>
    [[nodiscard]] T* create(Args&&... args) {
        void* memory = acquire_slot();
        try {
            return std::construct_at(static_cast<T*>(memory), std::forward<Args>(args)...);
        } catch (...) {
            release_slot(memory);
            throw;
        }
    }

    void destroy(T* object) noexcept {
        std::destroy_at(object);
        release_slot(object);
    }

private:
    struct FreeNode {
        FreeNode* next;
    };

    static constexpr std::size_t slot_size =
        sizeof(T) > sizeof(FreeNode) ? sizeof(T) : sizeof(FreeNode);
    static constexpr std::size_t slot_align =
        alignof(T) > alignof(FreeNode) ? alignof(T) : alignof(FreeNode);

    struct alignas(slot_align) Slot {
        std::byte storage[slot_size];
    };

    struct Block {
        Block* next{nullptr};
        Slot slots[ChunkSize];
    };

    [[nodiscard]] void* acquire_slot() {
        if (FreeNode* node = pop_free()) {
            return node;
        }

        add_block();
        FreeNode* node = pop_free();
        CAF_ASSERT(node != nullptr);
        return node;
    }

    [[nodiscard]] FreeNode* pop_free() noexcept {
        FreeNode* head = free_.load(std::memory_order_acquire);
        while (head != nullptr) {
            FreeNode* next = head->next;
            if (free_.compare_exchange_weak(
                    head,
                    next,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                return head;
            }
        }
        return nullptr;
    }

    void release_slot(void* memory) noexcept {
        auto* node = static_cast<FreeNode*>(memory);
        FreeNode* head = free_.load(std::memory_order_relaxed);
        do {
            node->next = head;
        } while (!free_.compare_exchange_weak(
            head,
            node,
            std::memory_order_release,
            std::memory_order_relaxed));
    }

    void add_block() {
        auto* block = new Block;
        Block* head = blocks_.load(std::memory_order_relaxed);
        do {
            block->next = head;
        } while (!blocks_.compare_exchange_weak(
            head,
            block,
            std::memory_order_release,
            std::memory_order_relaxed));

        for (auto& slot : block->slots) {
            release_slot(slot.storage);
        }
    }

    alignas(hardware_cache_line_size) std::atomic<FreeNode*> free_{nullptr};
    std::atomic<Block*> blocks_{nullptr};
};

} // namespace caf::detail
