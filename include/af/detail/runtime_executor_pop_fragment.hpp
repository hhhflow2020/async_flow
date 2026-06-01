#if !defined(AF_ASYNC_RUNTIME_EXECUTOR_FRAGMENT_INCLUDE)
#error "runtime_executor_pop_fragment.hpp is an AsyncRuntime::Executor implementation fragment"
#endif

        void advance_ready_word_cursor_after(std::size_t word) noexcept {
            if constexpr (decltype(ready_sources_)::word_count > 1U) {
                std::size_t next = word + 1U;
                if (next == decltype(ready_sources_)::word_count) {
                    next = 0;
                }
                next_ready_word_ = static_cast<std::uint16_t>(next);
            } else {
                static_cast<void>(word);
            }
        }

        Task* pop_one() noexcept {
            if (Task* task = try_pop_local()) {
                return task;
            }

            for (std::size_t checked_word = 0;
                 checked_word < decltype(ready_sources_)::word_count;
                 ++checked_word) {
                std::size_t word = checked_word;
                if constexpr (decltype(ready_sources_)::word_count > 1U) {
                    word += next_ready_word_;
                    if (word >= decltype(ready_sources_)::word_count) {
                        word -= decltype(ready_sources_)::word_count;
                    }
                }
                std::uint64_t mask = ready_sources_.load_word(word);
                while (mask != 0U) {
                    const std::uint16_t source = static_cast<std::uint16_t>(
                        decltype(ready_sources_)::word_base(word) + std::countr_zero(mask));
                    const std::uint64_t bit = 1ULL << (source & 63U);
                    mask &= ~bit;
                    if (source == index_) {
                        continue;
                    }
                    if (Task* task = spsc_queue(source, index_).try_pop()) {
                        advance_ready_word_cursor_after(word);
                        next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
                        return task;
                    }
                    ready_sources_.clear(source);
                    if (Task* task = spsc_queue(source, index_).try_pop()) {
                        advance_ready_word_cursor_after(word);
                        next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
                        mark_ready(source);
                        return task;
                    }
                }
            }

            for (std::uint16_t checked = 0; checked < thread_count; ++checked) {
                const std::uint16_t source =
                    static_cast<std::uint16_t>((next_source_ + checked) % thread_count);
                if (source == index_) {
                    continue;
                }
                if (Task* task = spsc_queue(source, index_).try_pop()) {
                    next_source_ = static_cast<std::uint16_t>((source + 1U) % thread_count);
                    mark_ready(source);
                    return task;
                }
            }

            if (external_ready_.load(std::memory_order_acquire)) {
                if (Task* task = external_queues_[index_]->try_pop()) {
                    return task;
                }

                external_ready_.store(false, std::memory_order_release);
                if (Task* task = external_queues_[index_]->try_pop()) {
                    external_ready_.store(true, std::memory_order_release);
                    return task;
                }
            }

            return external_queues_[index_]->try_pop();
        }
