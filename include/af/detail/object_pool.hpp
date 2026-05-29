#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <thread>
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
        tls_cache().discard_if_owner(this);
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
    static constexpr std::size_t local_cache_capacity = 32;
    static constexpr std::size_t slot_size =
        sizeof(T) > hardware_cache_line_size ? sizeof(T) : hardware_cache_line_size;
    static constexpr std::size_t slot_align =
        alignof(T) > hardware_cache_line_size ? alignof(T) : hardware_cache_line_size;

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

    struct LocalCache {
        ObjectPool* owner{nullptr};
        Slot* slots[local_cache_capacity]{};
        std::size_t size{0};

        ~LocalCache() {
            flush();
        }

        void reset_for(ObjectPool* pool) noexcept {
            if (owner == pool) {
                return;
            }
            flush();
            owner = pool;
        }

        void discard_if_owner(const ObjectPool* pool) noexcept {
            if (owner == pool) {
                size = 0;
                owner = nullptr;
            }
        }

        void flush() noexcept {
            while (size != 0) {
                Slot* slot = slots[--size];
                std::uint32_t spins = 0;
                while (!slot->owner->free_slots.try_push(slot)) {
                    AF_ASSERT(++spins < 1'000'000U &&
                              "object pool free queue did not accept cached slot");
                    std::this_thread::yield();
                }
            }
            owner = nullptr;
        }
    };

    [[nodiscard]] void* acquire_slot() {
        for (;;) {
            LocalCache& cache = local_cache();
            if (cache.size != 0) {
                return cache.slots[--cache.size]->storage;
            }

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
        LocalCache& cache = local_cache();
        if (cache.size < local_cache_capacity) {
            cache.slots[cache.size++] = slot;
            return;
        }

        std::uint32_t spins = 0;
        while (!slot->owner->free_slots.try_push(slot)) {
            AF_ASSERT(++spins < 1'000'000U && "object pool free queue did not accept released slot");
            std::this_thread::yield();
        }
    }

    [[nodiscard]] static Slot* slot_from_memory(void* memory) noexcept {
        auto* bytes = static_cast<std::byte*>(memory);
        return reinterpret_cast<Slot*>(bytes - offsetof(Slot, storage));
    }

    [[nodiscard]] LocalCache& local_cache() noexcept {
        LocalCache& cache = tls_cache();
        cache.reset_for(this);
        return cache;
    }

    [[nodiscard]] static LocalCache& tls_cache() noexcept {
        thread_local LocalCache cache;
        return cache;
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
