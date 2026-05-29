#include <atomic>
#include <cstdint>
#include <iostream>

#include "app_runtime.hpp"

class AddGoldTask final : public Task {
public:
    explicit AddGoldTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::uint64_t player_id, int gold, std::atomic<int>* completed) {
        player_id_ = player_id;
        gold_ = gold;
        completed_ = completed;
        return schedule(player_thread(player_id_));
    }

private:
    af::TaskResult run() override {
        std::cout << "add " << gold_ << " gold to player " << player_id_
                  << " on logic thread " << Flow::current_thread_index() << '\n';
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
    explicit LoginTask(Task::FactoryToken token) : Task(token) {}

    void do_it(std::uint64_t player_id, std::atomic<int>* completed) {
        player_id_ = player_id;
        completed_ = completed;
        state_ = State::Start;
        [[maybe_unused]] const bool scheduled = schedule(player_thread(player_id_));
        AF_ASSERT(scheduled);
    }

private:
    enum class State : std::uint8_t {
        Start,
        QueryDb,
        BackToLogic,
        Finish,
    };

    af::TaskResult run() override {
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
    Flow::init();

    std::atomic<int> completed{0};
    bool add_started = false;
    {
        auto add_task = Flow::make_task<AddGoldTask>();
        add_started = add_task->do_it(1001U, 100, &completed);
    }
    [[maybe_unused]] const bool login_started = Flow::start_task<LoginTask>(1002U, &completed);
    AF_ASSERT(add_started);
    AF_ASSERT(login_started);

    wait_completed(completed, 2);

    Flow::shutdown();
    return 0;
}
