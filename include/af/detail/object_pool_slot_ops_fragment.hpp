#if !defined(AF_OBJECT_POOL_FRAGMENT_INCLUDE)
#error "object_pool_slot_ops_fragment.hpp is an ObjectPool implementation fragment"
#endif

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
