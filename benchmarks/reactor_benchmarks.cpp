#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include <benchmark/benchmark.h>

#include "af/platform.hpp"
#include "af/reactor.hpp"

#include <fcntl.h>
#include <unistd.h>

namespace {

class unique_fd {
public:
    unique_fd() noexcept = default;
    explicit unique_fd(int fd) noexcept : fd_(fd) {}

    unique_fd(const unique_fd &) = delete;
    unique_fd &operator=(const unique_fd &) = delete;

    unique_fd(unique_fd &&other) noexcept : fd_(other.release()) {}

    unique_fd &operator=(unique_fd &&other) noexcept {
        if (this != &other) {
            reset(other.release());
        }
        return *this;
    }

    ~unique_fd() {
        reset();
    }

    [[nodiscard]] int get() const noexcept {
        return fd_;
    }

    [[nodiscard]] int release() noexcept {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

    void reset(int fd = -1) noexcept {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        fd_ = fd;
    }

private:
    int fd_{-1};
};

[[nodiscard]] bool set_nonblocking(int fd) noexcept {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

struct ready_pipe {
    unique_fd read_fd;
    unique_fd write_fd;
    af::fd_event_source source{};
    std::uint64_t *dispatched{nullptr};
};

void on_ready_pipe(void *owner, af::fd_event_source &, std::uint32_t events) noexcept {
    auto *pipe = static_cast<ready_pipe *>(owner);
    if (pipe == nullptr || (events & af::reactor_readable) == 0U) {
        return;
    }

    std::array<char, 64> buffer{};
    for (;;) {
        const ssize_t n = ::read(pipe->read_fd.get(), buffer.data(), buffer.size());
        if (n > 0) {
            if (pipe->dispatched != nullptr) {
                ++(*pipe->dispatched);
            }
            return;
        }
        if (n == 0) {
            return;
        }
        if (errno == EINTR) {
            continue;
        }
        return;
    }
}

[[nodiscard]] bool backend_supported(af::reactor_backend backend) noexcept {
    switch (backend) {
    case af::reactor_backend::auto_select:
    case af::reactor_backend::select:
        return true;
    case af::reactor_backend::epoll:
        return af::supports_epoll;
    case af::reactor_backend::kqueue:
        return af::supports_kqueue;
    }
    return false;
}

[[nodiscard]] std::string_view backend_name(af::reactor_backend backend) noexcept {
    switch (backend) {
    case af::reactor_backend::auto_select:
        return "auto";
    case af::reactor_backend::select:
        return "select";
    case af::reactor_backend::epoll:
        return "epoll";
    case af::reactor_backend::kqueue:
        return "kqueue";
    }
    return "unknown";
}

[[nodiscard]] bool make_ready_pipes(std::vector<ready_pipe> &pipes, std::uint64_t &dispatched) {
    for (ready_pipe &pipe : pipes) {
        int fds[2]{-1, -1};
        if (::pipe(fds) != 0) {
            return false;
        }
        pipe.read_fd.reset(fds[0]);
        pipe.write_fd.reset(fds[1]);
        if (!set_nonblocking(pipe.read_fd.get()) || !set_nonblocking(pipe.write_fd.get())) {
            return false;
        }
        pipe.source.fd = pipe.read_fd.get();
        pipe.source.interests = af::reactor_readable;
        pipe.source.owner = &pipe;
        pipe.source.on_event = &on_ready_pipe;
        pipe.dispatched = &dispatched;
    }
    return true;
}

[[nodiscard]] bool arm_ready_pipes(std::vector<ready_pipe> &pipes) noexcept {
    constexpr char byte = 'x';
    for (ready_pipe &pipe : pipes) {
        for (;;) {
            const ssize_t n = ::write(pipe.write_fd.get(), &byte, 1);
            if (n == 1) {
                break;
            }
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return false;
        }
    }
    return true;
}

void run_reactor_ready_batch_dispatch_benchmark(benchmark::State &state,
                                                af::reactor_backend backend) {
    const auto source_count = static_cast<std::size_t>(state.range(0));
    const auto event_budget = static_cast<std::size_t>(state.range(1));
    if (!backend_supported(backend)) {
        state.SkipWithError("reactor backend is not supported on this platform");
        return;
    }

    af::reactor_config config;
    config.backend = backend;
    config.event_budget = event_budget;
    std::unique_ptr<af::reactor> reactor = af::make_reactor(config);
    if (!reactor) {
        state.SkipWithError("failed to create reactor backend");
        return;
    }

    std::uint64_t dispatched = 0;
    std::vector<ready_pipe> pipes(source_count);
    if (!make_ready_pipes(pipes, dispatched)) {
        state.SkipWithError("failed to create ready pipes");
        return;
    }

    for (ready_pipe &pipe : pipes) {
        if (!reactor->add(&pipe.source)) {
            state.SkipWithError("failed to register reactor source");
            return;
        }
    }

    bool failed = false;
    for (auto _ : state) {
        dispatched = 0;
        if (!arm_ready_pipes(pipes)) {
            state.SkipWithError("failed to arm ready pipes");
            failed = true;
            break;
        }

        const std::uint64_t target = static_cast<std::uint64_t>(source_count);
        while (dispatched < target) {
            if (!reactor->poll(std::chrono::nanoseconds(0))) {
                state.SkipWithError("reactor poll failed");
                failed = true;
                break;
            }
        }
        if (failed) {
            break;
        }
    }

    for (ready_pipe &pipe : pipes) {
        static_cast<void>(reactor->del(&pipe.source));
    }

    if (!failed) {
        state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(source_count));
        state.counters["backend"] =
            benchmark::Counter(static_cast<double>(static_cast<unsigned>(backend)));
        state.counters["event_budget"] = benchmark::Counter(static_cast<double>(event_budget));
    }
}

void BM_ReactorSelectReadyBatchDispatch(benchmark::State &state) {
    run_reactor_ready_batch_dispatch_benchmark(state, af::reactor_backend::select);
}

void BM_ReactorAutoReadyBatchDispatch(benchmark::State &state) {
    run_reactor_ready_batch_dispatch_benchmark(state, af::reactor_backend::auto_select);
}

void BM_ReactorEpollReadyBatchDispatch(benchmark::State &state) {
    run_reactor_ready_batch_dispatch_benchmark(state, af::reactor_backend::epoll);
}

void BM_ReactorKqueueReadyBatchDispatch(benchmark::State &state) {
    run_reactor_ready_batch_dispatch_benchmark(state, af::reactor_backend::kqueue);
}

BENCHMARK(BM_ReactorSelectReadyBatchDispatch)
    ->Args({64, 16})
    ->Args({64, 64})
    ->Args({256, 64})
    ->Args({256, 256})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_ReactorAutoReadyBatchDispatch)
    ->Args({64, 16})
    ->Args({64, 64})
    ->Args({256, 64})
    ->Args({256, 256})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_ReactorEpollReadyBatchDispatch)
    ->Args({64, 16})
    ->Args({64, 64})
    ->Args({256, 64})
    ->Args({256, 256})
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_ReactorKqueueReadyBatchDispatch)
    ->Args({64, 16})
    ->Args({64, 64})
    ->Args({256, 64})
    ->Args({256, 256})
    ->Unit(benchmark::kMicrosecond);

} // namespace
