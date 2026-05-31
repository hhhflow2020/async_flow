#if !defined(AF_ASYNC_RUNTIME_FRAGMENT_INCLUDE)
#error "runtime_cache_line_atomic_fragment.hpp is an AsyncRuntime implementation fragment"
#endif

    template <typename T>
    struct alignas(detail::hardware_cache_line_size) CacheLineAtomic {
        std::atomic<T> value;

        constexpr CacheLineAtomic() noexcept = default;
        constexpr explicit CacheLineAtomic(T initial) noexcept : value(initial) {}

        CacheLineAtomic(const CacheLineAtomic&) = delete;
        CacheLineAtomic& operator=(const CacheLineAtomic&) = delete;

        [[nodiscard]] T load(std::memory_order order = std::memory_order_seq_cst) const noexcept {
            return value.load(order);
        }

        void store(T desired, std::memory_order order = std::memory_order_seq_cst) noexcept {
            value.store(desired, order);
        }

        [[nodiscard]] bool compare_exchange_weak(
            T& expected,
            T desired,
            std::memory_order success,
            std::memory_order failure) noexcept {
            return value.compare_exchange_weak(expected, desired, success, failure);
        }

        [[nodiscard]] bool compare_exchange_strong(
            T& expected,
            T desired,
            std::memory_order success,
            std::memory_order failure) noexcept {
            return value.compare_exchange_strong(expected, desired, success, failure);
        }

        T fetch_add(T arg, std::memory_order order = std::memory_order_seq_cst) noexcept {
            return value.fetch_add(arg, order);
        }

        T fetch_sub(T arg, std::memory_order order = std::memory_order_seq_cst) noexcept {
            return value.fetch_sub(arg, order);
        }

        T fetch_and(T arg, std::memory_order order = std::memory_order_seq_cst) noexcept {
            return value.fetch_and(arg, order);
        }

        void wait(T old, std::memory_order order = std::memory_order_seq_cst) const noexcept {
            value.wait(old, order);
        }

        void notify_one() noexcept {
            value.notify_one();
        }

        void notify_all() noexcept {
            value.notify_all();
        }
    };
