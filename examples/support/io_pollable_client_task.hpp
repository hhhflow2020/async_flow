#pragma once

#include <iostream>
#include <string_view>

#include "io_pollable_echo_client.hpp"

namespace io_pollable_client_example {

#if defined(__linux__)

class PollableClientTask final : public PollableTaskBase {
public:
    explicit PollableClientTask(PollableTaskBase::FactoryToken token) : PollableTaskBase(token) {}

    bool do_it(int fd) {
        client_.reset(fd);
        state_ = State::Drive;
        wait_.reset();
        return schedule(ClientThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Drive,
        Wait,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Drive:
            return drive();
        case State::Wait:
            return drive();
        }
        return failed();
    }

    af::TaskResult drive() {
        for (;;) {
            const PollableStep step = client_.step();
            if (step.error != 0) {
                std::cout << "pollable client failed: " << step.error << '\n';
                return failed();
            }
            if (step.done) {
                std::cout << "pollable client response="
                          << std::string_view(client_.response_data(), client_.response_size())
                          << '\n';
                return done();
            }
            if (step.want == 0U) {
                continue;
            }

            state_ = State::Wait;
            if (!wait_io(ClientThreads::IO_0, client_.fd(), step.want, &wait_.wait)) {
                std::cout << "io_wait failed: " << wait_.wait.error << '\n';
                return failed();
            }
            wait_.waiting = true;
            wait_.wait_kind = af::IoWaitKind::Readiness;
            return pending();
        }
    }

    State state_{State::Drive};
    PollableEchoClient client_{};
    af::IoOpState wait_{};
};

#endif

} // namespace io_pollable_client_example
