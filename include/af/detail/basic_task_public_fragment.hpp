#if !defined(AF_TASK_FRAGMENT_INCLUDE)
#error "basic_task_public_fragment.hpp is a task implementation fragment"
#endif

    using Runtime = RuntimeT;
    using Thread = typename Runtime::Thread;
    using DestroyFn = void (*)(BasicTask*) noexcept;

    class FactoryToken {
    public:
        FactoryToken(const FactoryToken&) noexcept = default;
        FactoryToken& operator=(const FactoryToken&) = delete;

    private:
        constexpr FactoryToken() noexcept = default;

        template <typename TraitsT>
        friend class AsyncRuntime;
    };

    BasicTask() = delete;
    BasicTask(const BasicTask&) = delete;
    BasicTask& operator=(const BasicTask&) = delete;
    virtual ~BasicTask() = default;

    static void* operator new(std::size_t) = delete;
    static void* operator new[](std::size_t) = delete;
    static void* operator new(std::size_t, std::align_val_t) = delete;
    static void* operator new[](std::size_t, std::align_val_t) = delete;
