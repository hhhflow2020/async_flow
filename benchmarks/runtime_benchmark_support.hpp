#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "af/detail/runtime/atomic_wait.hpp"
#include "af/runtime.hpp"
#include "af/runtime_config.hpp"

namespace af_bench::runtime {

[[nodiscard]] inline af::runtime_config make_runtime_config(std::size_t local_cache_size = 256,
                                                            std::size_t slab_object_count = 4096) {
    af::runtime_config config;
    config.threads = {
        af::cpu_threads("bench-cpu", 4),
        af::io_threads("bench-io", 1),
    };
    config.scheduler.task_drain_budget = 4096;
    config.scheduler.service_task_budget = 1024;
    config.task_pool.local_cache_size = local_cache_size;
    config.task_pool.slab_object_count = slab_object_count;
    config.logger.consumer_thread = af::thread_selector::cpu(0);
    return config;
}

struct bench_threads {
    af::thread_group_ref logic;
    af::thread_group_ref io;
    af::thread_ref logic_0;
    af::thread_ref logic_1;
    af::thread_ref logic_2;
    af::thread_ref logic_3;
    af::thread_ref io_0;
};

[[nodiscard]] inline bench_threads select_threads(af::runtime &owner) noexcept {
    bench_threads threads;
    threads.logic = owner.cpu_threads();
    threads.io = owner.io_threads();
    threads.logic_0 = threads.logic.at(0);
    threads.logic_1 = threads.logic.at(1);
    threads.logic_2 = threads.logic.at(2);
    threads.logic_3 = threads.logic.at(3);
    threads.io_0 = threads.io.at(0);
    return threads;
}

inline void wait_for_active_threads(af::runtime &owner) {
    const auto expected = owner.thread_count();
    while (owner.active_thread_count() != expected) {
        std::this_thread::yield();
    }
}

inline void wait_zero(std::atomic<int> &remaining) {
    while (remaining.load(std::memory_order_acquire) != 0) {
        const int observed = remaining.load(std::memory_order_acquire);
        if (observed != 0) {
            af::detail::atomic_wait_value(remaining, observed, std::memory_order_acquire);
        }
    }
}

inline void mark_one_done(std::atomic<int> &remaining) noexcept {
    if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        af::detail::atomic_notify_one(remaining);
    }
}

class CountTask final : public af::runtime_task {
public:
    CountTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    bool do_it(af::thread_ref thread, std::atomic<int> *remaining) noexcept {
        remaining_ = remaining;
        return schedule_to(thread);
    }

private:
    af::task_result run_task() noexcept override {
        mark_one_done(*remaining_);
        return done();
    }

    std::atomic<int> *remaining_{nullptr};
};

class HopTask final : public af::runtime_task {
public:
    HopTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    bool do_it(bench_threads threads, int hops, std::atomic<int> *remaining) noexcept {
        threads_ = threads;
        hops_ = hops;
        remaining_ = remaining;
        return schedule_to(threads_.logic_0);
    }

private:
    af::task_result run_task() noexcept override {
        if (hops_-- > 0) {
            const auto current = af::runtime::current_thread_index();
            const auto next =
                current == threads_.logic_0.index ? threads_.logic_1 : threads_.logic_0;
            return pending_to(next);
        }

        mark_one_done(*remaining_);
        return done();
    }

    bench_threads threads_;
    int hops_{0};
    std::atomic<int> *remaining_{nullptr};
};

class SameThreadTask final : public af::runtime_task {
public:
    SameThreadTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    bool do_it(af::thread_ref thread, int rounds, std::atomic<int> *remaining) noexcept {
        rounds_ = rounds;
        remaining_ = remaining;
        return schedule_to(thread);
    }

private:
    af::task_result run_task() noexcept override {
        if (rounds_-- > 0) {
            return reschedule();
        }

        mark_one_done(*remaining_);
        return done();
    }

    int rounds_{0};
    std::atomic<int> *remaining_{nullptr};
};

class IoHopTask final : public af::runtime_task {
public:
    IoHopTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    bool do_it(bench_threads threads, std::atomic<int> *remaining) noexcept {
        threads_ = threads;
        remaining_ = remaining;
        state_ = state::logic;
        return schedule_to(threads_.logic_0);
    }

private:
    enum class state : std::uint8_t {
        logic,
        io,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::logic:
            state_ = state::io;
            return pending_to(threads_.io_0);

        case state::io:
            mark_one_done(*remaining_);
            return done();
        }
        return failed();
    }

    bench_threads threads_;
    state state_{state::logic};
    std::atomic<int> *remaining_{nullptr};
};

class ParallelShardTask final : public af::runtime_task {
public:
    ParallelShardTask(factory_token token, af::runtime &owner) : runtime_task(token, owner) {}

    bool do_it(bench_threads threads, std::atomic<int> *remaining,
               std::atomic<std::uint64_t> *sum) {
        threads_ = threads;
        remaining_ = remaining;
        sum_ = sum;
        ops_ = af::sharded_ops<std::uint64_t>(4);
        for (std::uint64_t i = 0; i < 1024; ++i) {
            ops_.shards[i & 3U].push_back(i);
        }
        return schedule_to(threads_.logic_0);
    }

private:
    enum class state : std::uint8_t {
        split,
        finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::split: {
            state_ = state::finish;
            const bool ok = owner().parallel_shards(
                threads_.logic, ops_, af::parallel_mode::all_shards, this,
                [this](std::uint16_t, std::vector<std::uint64_t> &shard_ops) {
                    std::uint64_t local = 0;
                    for (auto value : shard_ops) {
                        local += value;
                    }
                    sum_->fetch_add(local, std::memory_order_relaxed);
                });
            return ok ? pending() : failed();
        }

        case state::finish:
            mark_one_done(*remaining_);
            return done();
        }

        return failed();
    }

    bench_threads threads_;
    state state_{state::split};
    af::sharded_ops<std::uint64_t> ops_{4};
    std::atomic<int> *remaining_{nullptr};
    std::atomic<std::uint64_t> *sum_{nullptr};
};

} // namespace af_bench::runtime
