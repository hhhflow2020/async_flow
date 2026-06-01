#if !defined(AF_OBJECT_POOL_FRAGMENT_INCLUDE)
#error "object_pool_storage_fragment.hpp is an ObjectPool implementation fragment"
#endif

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
