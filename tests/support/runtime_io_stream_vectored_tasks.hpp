#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_io_stream_vectored_tasks.hpp is a runtime_io_test_support implementation detail"
#endif

class StreamVectoredEchoTask final : public IoTaskBase {
public:
    explicit StreamVectoredEchoTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(
        int fd,
        std::atomic<int>* armed,
        std::atomic<int>* completed,
        std::atomic<int>* request_seen) {
        stream_.reset(IoTestThread::IO_0, fd);
        armed_ = armed;
        completed_ = completed;
        request_seen_ = request_seen;
        return schedule(IoTestThread::IO_0);
    }

private:
    enum class State : std::uint8_t {
        ReadRequest,
        SendResponse,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::ReadRequest:
            return read_request();

        case State::SendResponse:
            return send_response();
        }
        return failed();
    }

    af::TaskResult read_request() {
        iovec request_iov[2]{
            iovec{&request_[0], 1},
            iovec{&request_[1], 1},
        };
        const af::IoStatus status = stream_.recvv_some(*this, request_iov, 2, read_);
        if (status.pending()) {
            armed_->fetch_add(1, std::memory_order_release);
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(request_)) {
            return failed();
        }
        const int combined =
            (static_cast<int>(request_[0]) << 8) | static_cast<unsigned char>(request_[1]);
        request_seen_->store(combined, std::memory_order_release);
        state_ = State::SendResponse;
        return again();
    }

    af::TaskResult send_response() {
        iovec response_iov[2]{
            iovec{&response_[0], 1},
            iovec{&response_[1], 1},
        };
        const af::IoStatus status = stream_.sendv_some(*this, response_iov, 2, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(response_)) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::ReadRequest};
    af::TcpStream<IoTestThread> stream_{};
    char request_[2]{};
    char response_[2]{'X', 'Y'};
    af::IoOpState read_{};
    af::IoOpState write_{};
    std::atomic<int>* armed_{nullptr};
    std::atomic<int>* completed_{nullptr};
    std::atomic<int>* request_seen_{nullptr};
};
