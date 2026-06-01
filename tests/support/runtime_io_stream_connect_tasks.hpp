#if !defined(AF_RUNTIME_IO_TEST_SUPPORT_DETAIL_INCLUDE)
#error "runtime_io_stream_connect_tasks.hpp is a runtime_io_test_support implementation detail"
#endif

class TcpConnectTask final : public IoTaskBase {
public:
    explicit TcpConnectTask(IoTaskBase::FactoryToken token) : IoTaskBase(token) {}

    bool do_it(int fd, sockaddr_in address, socklen_t address_size, std::atomic<int>* completed) {
        stream_.reset(IoTestThread::IO_0, fd);
        address_ = address;
        address_size_ = address_size;
        completed_ = completed;
        return schedule(IoTestThread::IO_0);
    }

private:
    af::TaskResult run() override {
        const af::IoStatus status = stream_.connect(
            *this,
            reinterpret_cast<const sockaddr*>(&address_),
            address_size_,
            connect_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    af::TcpStream<IoTestThread> stream_{};
    sockaddr_in address_{};
    socklen_t address_size_{sizeof(address_)};
    af::IoOpState connect_{};
    std::atomic<int>* completed_{nullptr};
};
