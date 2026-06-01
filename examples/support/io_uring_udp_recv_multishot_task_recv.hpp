#if !defined(AF_EXAMPLE_IO_URING_UDP_RECV_MULTISHOT_TASK_FRAGMENT_INCLUDE)
#error "io_uring_udp_recv_multishot_task_recv.hpp is a UdpRecvMultishotTask implementation fragment"
#endif

af::TaskResult recv_one() {
    std::uint16_t buffer_id = 0;
    const af::IoStatus status =
        socket_.recv_multishot(*this, buffer_group, &buffer_id, recv_);
    if (status.pending()) {
        if (!armed_once_) {
            armed_once_ = true;
            armed_->fetch_add(1, std::memory_order_release);
        }
        return pending();
    }
    if (status.failed()) {
        return complete(status.error);
    }
    if (!status.ready() || status.bytes != 1U || buffer_id >= buffer_count) {
        return stop_recv(EIO);
    }

    const int shifted = received_ == 0 ? 8 : 0;
    *packed_read_ |= static_cast<int>(
        static_cast<unsigned char>(buffers_[buffer_id])) << shifted;
    ++received_;

    const af::IoProvidedBuffer buffer{
        &buffers_[buffer_id],
        sizeof(buffers_[buffer_id]),
        buffer_id};
    int add_error = 0;
    if (!ring_.add(&buffer, 1, add_error)) {
        return stop_recv(add_error == 0 ? EIO : add_error);
    }

    if (received_ < target_reads) {
        return pending();
    }
    if (!recv_.waiting) {
        state_ = State::Unregister;
        return again();
    }
    finish_error_ = 0;
    if (!udp_recv_async::cancel_io(UdpRecvThread::IO_0, recv_)) {
        return complete(recv_.wait.error == 0 ? EIO : recv_.wait.error);
    }
    state_ = State::Cancel;
    return pending();
}

af::TaskResult finish_cancel() {
    std::uint16_t ignored = 0;
    const af::IoStatus status =
        socket_.recv_multishot(*this, buffer_group, &ignored, recv_);
    if (status.pending()) {
        return pending();
    }
    if (!status.failed() || status.error != ECANCELED) {
        return complete(status.failed() ? status.error : EIO);
    }
    state_ = State::Unregister;
    return again();
}

af::TaskResult stop_recv(int error) {
    finish_error_ = error == 0 ? EIO : error;
    if (!recv_.waiting) {
        state_ = State::Unregister;
        return again();
    }
    if (!udp_recv_async::cancel_io(UdpRecvThread::IO_0, recv_)) {
        return complete(recv_.wait.error == 0 ? finish_error_ : recv_.wait.error);
    }
    state_ = State::Cancel;
    return pending();
}
