#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <thread>

#include "caf/caf.hpp"

enum class AppThread : std::uint16_t {
    Logic_0,
    Logic_1,
    Logic_2,
    Logic_3,

    DB_0,
    IO_0,

    enum_num_end,
};

struct AppRuntimeTraits {
    using Thread = AppThread;

    static constexpr std::uint16_t thread_count =
        static_cast<std::uint16_t>(AppThread::enum_num_end);

    static constexpr AppThread logic_begin = AppThread::Logic_0;
    static constexpr std::uint16_t logic_count = 4;
};

using Runtime = caf::AsyncRuntime<AppRuntimeTraits>;
using Task = Runtime::Task;

static AppThread player_thread(std::uint64_t player_id) noexcept {
    return Runtime::shard_by<AppRuntimeTraits::logic_begin, AppRuntimeTraits::logic_count>(
        player_id);
}

class AddGoldTask final : public Task {
public:
    void do_it(std::uint64_t player_id, int gold, std::atomic<int>* completed) {
        player_id_ = player_id;
        gold_ = gold;
        completed_ = completed;
        [[maybe_unused]] const bool scheduled = schedule(player_thread(player_id_));
        CAF_ASSERT(scheduled);
    }

private:
    caf::TaskResult run() override {
        std::cout << "add " << gold_ << " gold to player " << player_id_
                  << " on logic thread " << Runtime::current_thread_index() << '\n';
        completed_->fetch_add(1, std::memory_order_release);
        completed_->notify_one();
        return done();
    }

    std::uint64_t player_id_{0};
    int gold_{0};
    std::atomic<int>* completed_{nullptr};
};

class LoginTask final : public Task {
public:
    void do_it(std::uint64_t player_id, std::atomic<int>* completed) {
        player_id_ = player_id;
        completed_ = completed;
        state_ = State::Start;
        [[maybe_unused]] const bool scheduled = schedule(player_thread(player_id_));
        CAF_ASSERT(scheduled);
    }

private:
    enum class State : std::uint8_t {
        Start,
        QueryDb,
        BackToLogic,
        Finish,
    };

    caf::TaskResult run() override {
        switch (state_) {
        case State::Start:
            state_ = State::QueryDb;
            return pending_on(AppThread::DB_0);

        case State::QueryDb:
            std::cout << "query login data for player " << player_id_ << " on DB\n";
            state_ = State::BackToLogic;
            return pending_on(player_thread(player_id_));

        case State::BackToLogic:
            state_ = State::Finish;
            return again();

        case State::Finish:
            completed_->fetch_add(1, std::memory_order_release);
            completed_->notify_one();
            return done();
        }

        return done();
    }

    State state_{State::Start};
    std::uint64_t player_id_{0};
    std::atomic<int>* completed_{nullptr};
};

int main() {
    Runtime::init();

    std::atomic<int> completed{0};
    [[maybe_unused]] const bool add_started =
        Runtime::start_task<AddGoldTask>(1001U, 100, &completed);
    [[maybe_unused]] const bool login_started = Runtime::start_task<LoginTask>(1002U, &completed);
    CAF_ASSERT(add_started);
    CAF_ASSERT(login_started);

    while (completed.load(std::memory_order_acquire) < 2) {
        const int observed = completed.load(std::memory_order_acquire);
        completed.wait(observed, std::memory_order_acquire);
    }

    Runtime::shutdown();
    return 0;
}
