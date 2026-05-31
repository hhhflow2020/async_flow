#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_io_uring_file_fixed_buffer_file_submit_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

#if !defined(_WIN32)
        [[nodiscard]] bool submit_io_uring_read_fixed_file(
            int file_index,
            void* data,
            std::size_t size,
            std::uint64_t offset,
            std::uint16_t buffer_index,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fixed_file_rw(
                IORING_OP_READ_FIXED,
                file_index,
                data,
                size,
                offset,
                io_readable,
                task,
                result,
                buffer_index,
                true);
#else
            static_cast<void>(file_index);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(offset);
            static_cast<void>(buffer_index);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

        [[nodiscard]] bool submit_io_uring_write_fixed_file(
            int file_index,
            const void* data,
            std::size_t size,
            std::uint64_t offset,
            std::uint16_t buffer_index,
            Task* task,
            IoResult* result) noexcept {
#if defined(__linux__)
            return submit_io_uring_fixed_file_rw(
                IORING_OP_WRITE_FIXED,
                file_index,
                const_cast<void*>(data),
                size,
                offset,
                io_writable,
                task,
                result,
                buffer_index,
                true);
#else
            static_cast<void>(file_index);
            static_cast<void>(data);
            static_cast<void>(size);
            static_cast<void>(offset);
            static_cast<void>(buffer_index);
            static_cast<void>(task);
            if (result != nullptr) {
                result->error = ENOSYS;
            }
            return false;
#endif
        }

#endif
