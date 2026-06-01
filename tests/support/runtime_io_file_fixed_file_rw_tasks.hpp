#pragma once

class UringFixedFileTask final : public UringIoTaskBase {
public:
    explicit UringFixedFileTask(UringIoTaskBase::FactoryToken token) : UringIoTaskBase(token) {}

    bool do_it(int fd, std::atomic<int> *completed, std::atomic<char> *byte_read) {
        fd_ = fd;
        file_.reset(IoTestThreads::IO_0, 0);
        completed_ = completed;
        byte_read_ = byte_read;
        return schedule(IoTestThreads::IO_0);
    }

private:
    enum class State : std::uint8_t {
        Register,
        Write,
        WriteVectored,
        Fsync,
        Read,
        ReadVectored,
        Unregister,
    };

    af::TaskResult run() override {
        switch (state_) {
        case State::Register:
            return register_file();

        case State::Write:
            return write_value();

        case State::WriteVectored:
            return write_vectored();

        case State::Fsync:
            return fsync_value();

        case State::Read:
            return read_value();

        case State::ReadVectored:
            return read_vectored();

        case State::Unregister:
            return unregister_file();
        }
        return failed();
    }

    af::TaskResult register_file() {
        const af::IoStatus no_table = file_.write_at(*this, &value_, sizeof(value_), 0, no_table_);
        if (!no_table.failed() || no_table.error != ENXIO) {
            return failed();
        }

        int error = 0;
        if (!UringIoRuntime::io_register_files(IoTestThreads::IO_0, &fd_, 1, &error)) {
            return failed();
        }

        int duplicate_error = 0;
        if (UringIoRuntime::io_register_files(IoTestThreads::IO_0, &fd_, 1, &duplicate_error) ||
            duplicate_error != EALREADY) {
            return failed();
        }

        af::IoFixedFile<IoTestThread> bad_file(IoTestThreads::IO_0, 1);
        const af::IoStatus bad_index =
            bad_file.write_at(*this, &value_, sizeof(value_), 0, bad_index_);
        if (!bad_index.failed() || bad_index.error != EINVAL) {
            return failed();
        }

        const af::IoStatus no_buffer = file_.write_fixed_at(*this, buffer_, 1, 0, 0, no_buffer_);
        if (!no_buffer.failed() || no_buffer.error != ENOBUFS) {
            return failed();
        }

        iovec iov{buffer_, sizeof(buffer_)};
        int buffer_error = 0;
        if (!UringIoRuntime::io_register_buffers(IoTestThreads::IO_0, &iov, 1, &buffer_error)) {
            return failed();
        }

        buffer_[0] = value_;
        state_ = State::Write;
        return again();
    }

    af::TaskResult write_value() {
        const af::IoStatus status =
            file_.write_fixed_at(*this, af::IoFixedBuffer{buffer_, 1, 0}, 0, write_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U) {
            return failed();
        }
        buffer_[0] = 0;
        state_ = State::WriteVectored;
        return again();
    }

    af::TaskResult write_vectored() {
        write_iov_[0] = iovec{&vector_write_[0], 1};
        write_iov_[1] = iovec{&vector_write_[1], 1};
        const af::IoStatus status = file_.writev_at(*this, write_iov_, 2, 1, writev_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(vector_write_)) {
            return failed();
        }
        state_ = State::Fsync;
        return again();
    }

    af::TaskResult fsync_value() {
        const af::IoStatus status = file_.fsync(*this, fsync_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready()) {
            return failed();
        }
        state_ = State::Read;
        return again();
    }

    af::TaskResult read_value() {
        const af::IoStatus status =
            file_.read_fixed_at(*this, af::IoFixedBuffer{buffer_, 1, 0}, 0, read_state_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != 1U || buffer_[0] != value_) {
            return failed();
        }
        state_ = State::ReadVectored;
        return again();
    }

    af::TaskResult read_vectored() {
        read_iov_[0] = iovec{&vector_read_[0], 1};
        read_iov_[1] = iovec{&vector_read_[1], 1};
        const af::IoStatus status = file_.readv_at(*this, read_iov_, 2, 1, readv_);
        if (status.pending()) {
            return pending();
        }
        if (!status.ready() || status.bytes != sizeof(vector_read_) ||
            vector_read_[0] != vector_write_[0] || vector_read_[1] != vector_write_[1]) {
            return failed();
        }
        state_ = State::Unregister;
        return again();
    }

    af::TaskResult unregister_file() {
        int error = 0;
        if (!UringIoRuntime::io_unregister_buffers(IoTestThreads::IO_0, &error)) {
            return failed();
        }
        if (!UringIoRuntime::io_unregister_files(IoTestThreads::IO_0, &error)) {
            return failed();
        }
        byte_read_->store(buffer_[0], std::memory_order_release);
        completed_->fetch_add(1, std::memory_order_release);
        return done();
    }

    State state_{State::Register};
    int fd_{-1};
    af::IoFixedFile<IoTestThread> file_{};
    alignas(64) char buffer_[64]{};
    char value_{'F'};
    char vector_write_[2]{'I', 'O'};
    char vector_read_[2]{};
    iovec write_iov_[2]{};
    iovec read_iov_[2]{};
    af::IoOpState no_table_{};
    af::IoOpState bad_index_{};
    af::IoOpState no_buffer_{};
    af::IoOpState write_{};
    af::IoOpState writev_{};
    af::IoOpState fsync_{};
    af::IoOpState read_state_{};
    af::IoOpState readv_{};
    std::atomic<int> *completed_{nullptr};
    std::atomic<char> *byte_read_{nullptr};
};
