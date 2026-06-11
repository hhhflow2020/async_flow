#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <thread>

#include "af/runtime.hpp"

namespace {

std::mutex cout_mutex;

[[nodiscard]] af::runtime_config make_app_runtime_config() {
    af::runtime_config config;
    config.threads = {
        af::cpu_threads("logic", 4),
        af::cpu_threads("db", 1),
        af::io_threads("io", 1),
    };
    return config;
}

[[nodiscard]] bool wait_for_counter(std::atomic<int> &counter, int expected) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (counter.load(std::memory_order_acquire) >= expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return counter.load(std::memory_order_acquire) >= expected;
}

class add_gold_task final : public af::runtime_task {
public:
    add_gold_task(af::runtime_task::factory_token token, af::runtime &owner) noexcept
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(std::uint64_t player_id, int gold, af::thread_group_ref logic_threads,
                             std::atomic<int> &completed) noexcept {
        player_id_ = player_id;
        gold_ = gold;
        completed_ = &completed;
        return schedule_to(logic_threads.shard(player_id_));
    }

private:
    af::task_result run_task() noexcept override {
        const std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << "add " << gold_ << " gold to player " << player_id_ << " on logic thread "
                  << af::runtime::current_thread_index() << '\n';
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    std::uint64_t player_id_{0};
    int gold_{0};
};

class login_task final : public af::runtime_task {
public:
    login_task(af::runtime_task::factory_token token, af::runtime &owner) noexcept
        : af::runtime_task(token, owner) {}

    [[nodiscard]] bool do_it(std::uint64_t player_id, af::thread_group_ref logic_threads,
                             af::thread_ref db_thread, std::atomic<int> &completed) noexcept {
        player_id_ = player_id;
        logic_threads_ = logic_threads;
        db_thread_ = db_thread;
        completed_ = &completed;
        state_ = state::start;
        return schedule_to(logic_threads_.shard(player_id_));
    }

private:
    enum class state : std::uint8_t {
        start,
        query_db,
        back_to_logic,
        finish,
    };

    af::task_result run_task() noexcept override {
        switch (state_) {
        case state::start:
            return start_query();

        case state::query_db:
            return query_db();

        case state::back_to_logic:
            return back_to_logic();

        case state::finish:
            return finish();
        }

        return done();
    }

    af::task_result start_query() noexcept {
        state_ = state::query_db;
        return pending_to(db_thread_);
    }

    af::task_result query_db() noexcept {
        {
            const std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "query login data for player " << player_id_ << " on DB thread "
                      << af::runtime::current_thread_index() << '\n';
        }
        state_ = state::back_to_logic;
        return pending_to(logic_threads_.shard(player_id_));
    }

    af::task_result back_to_logic() noexcept {
        state_ = state::finish;
        return reschedule();
    }

    af::task_result finish() noexcept {
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    std::atomic<int> *completed_{nullptr};
    af::thread_group_ref logic_threads_;
    af::thread_ref db_thread_;
    state state_{state::start};
    std::uint64_t player_id_{0};
};

} // namespace

int main() {
    af::runtime runtime(make_app_runtime_config());
    if (!runtime.start()) {
        std::cerr << "failed to start runtime\n";
        return 1;
    }

    const af::thread_group_ref logic_threads = runtime.thread_group("logic");
    const af::thread_group_ref db_threads = runtime.thread_group("db");
    if (logic_threads.empty() || db_threads.empty()) {
        std::cerr << "runtime thread layout is missing logic or db threads\n";
        runtime.stop();
        return 1;
    }

    std::atomic<int> completed{0};
    bool started = true;
    {
        auto add_task = af::make_task<add_gold_task>(runtime);
        auto login = af::make_task<login_task>(runtime);

        const bool add_started = add_task->do_it(1001U, 100, logic_threads, completed);
        const bool login_started =
            login->do_it(1002U, logic_threads, db_threads.front(), completed);
        started = add_started && login_started;
    }

    const bool completed_all = wait_for_counter(completed, 2);
    runtime.stop();
    return started && completed_all ? 0 : 1;
}
