#include <cstdint>
#include <iostream>
#include <mutex>

#include "app_runtime.hpp"

namespace {

std::mutex cout_mutex;

} // namespace

class AddGoldTask final : public Task {
public:
    explicit AddGoldTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::uint64_t player_id, int gold) {
        player_id_ = player_id;
        gold_ = gold;
        return schedule(player_thread(player_id_));
    }

private:
    af::TaskResult run() override {
        const std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << "add " << gold_ << " gold to player " << player_id_
                  << " on logic thread " << async::current_thread_index() << '\n';
        return done();
    }

    std::uint64_t player_id_{0};
    int gold_{0};
};

class LoginTask final : public Task {
public:
    explicit LoginTask(Task::FactoryToken token) : Task(token) {}

    bool do_it(std::uint64_t player_id) {
        player_id_ = player_id;
        state_ = State::Start;
        return schedule(player_thread(player_id_));
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
            return start_query();

        case State::QueryDb:
            return query_db();

        case State::BackToLogic:
            return back_to_logic();

        case State::Finish:
            return finish();
        }

        return done();
    }

    af::TaskResult start_query() {
        state_ = State::QueryDb;
        return pending_on(AppThread::DB_0);
    }

    af::TaskResult query_db() {
        const std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << "query login data for player " << player_id_ << " on DB\n";
        state_ = State::BackToLogic;
        return pending_on(player_thread(player_id_));
    }

    af::TaskResult back_to_logic() {
        state_ = State::Finish;
        return again();
    }

    af::TaskResult finish() {
        return done();
    }

    State state_{State::Start};
    std::uint64_t player_id_{0};
};

int main() {
    async::init();

    {
        auto add_task = async::make_task<AddGoldTask>();
        auto login_task = async::make_task<LoginTask>();

        [[maybe_unused]] const bool add_started = add_task->do_it(1001U, 100);
        [[maybe_unused]] const bool login_started = login_task->do_it(1002U);
        AF_ASSERT(add_started);
        AF_ASSERT(login_started);
    }

    async::shutdown();
    return 0;
}
