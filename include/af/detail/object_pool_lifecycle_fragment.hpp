#if !defined(AF_OBJECT_POOL_FRAGMENT_INCLUDE)
#error "object_pool_lifecycle_fragment.hpp is an ObjectPool implementation fragment"
#endif

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
