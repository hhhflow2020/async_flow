#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

#include "af/detail/bounded_queues.hpp"
#include "af/detail/config.hpp"

namespace af::detail {

template <typename T, std::size_t ChunkSize = 256>
class ObjectPool {
    static_assert(ChunkSize > 0, "ObjectPool chunk size must be greater than zero");

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
    static constexpr std::size_t slot_size =
        sizeof(T) > sizeof(void*) ? sizeof(T) : sizeof(void*);
    static constexpr std::size_t slot_align = alignof(T) > alignof(void*) ? alignof(T) : alignof(void*);

    struct Block;

    struct Slot {
        Block* owner{nullptr};
        alignas(slot_align) std::byte storage[slot_size];
    };

    struct Block {
        explicit Block() : free_slots(ChunkSize) {
            for (auto& slot : slots) {
                slot.owner = this;
                [[maybe_unused]] const bool ok = free_slots.try_push(&slot);
                AF_ASSERT(ok);
            }
        }

        Block* next{nullptr};
        BoundedMpmcQueue<Slot> free_slots;
        Slot slots[ChunkSize];
    };

    [[nodiscard]] void* acquire_slot() {
        for (;;) {
            if (Block* block = hot_block_.load(std::memory_order_acquire)) {
                if (Slot* slot = block->free_slots.try_pop()) {
                    return slot->storage;
                }
            }

            for (Block* block = blocks_.load(std::memory_order_acquire);
                 block != nullptr;
                 block = block->next) {
                if (Slot* slot = block->free_slots.try_pop()) {
                    hot_block_.store(block, std::memory_order_release);
                    return slot->storage;
                }
            }

            Block* block = add_block();
            if (Slot* slot = block->free_slots.try_pop()) {
                return slot->storage;
            }
        }
    }

    void release_slot(void* memory) noexcept {
        Slot* slot = slot_from_memory(memory);
        [[maybe_unused]] const bool ok = slot->owner->free_slots.try_push(slot);
        AF_ASSERT(ok);
    }

    [[nodiscard]] static Slot* slot_from_memory(void* memory) noexcept {
        auto* bytes = static_cast<std::byte*>(memory);
        return reinterpret_cast<Slot*>(bytes - offsetof(Slot, storage));
    }

    [[nodiscard]] Block* add_block() {
        auto* block = new Block;
        Block* head = blocks_.load(std::memory_order_relaxed);
        do {
            block->next = head;
        } while (!blocks_.compare_exchange_weak(
            head,
            block,
            std::memory_order_release,
            std::memory_order_relaxed));
        hot_block_.store(block, std::memory_order_release);
        return block;
    }

    alignas(hardware_cache_line_size) std::atomic<Block*> blocks_{nullptr};
    alignas(hardware_cache_line_size) std::atomic<Block*> hot_block_{nullptr};
};

} // namespace af::detail
