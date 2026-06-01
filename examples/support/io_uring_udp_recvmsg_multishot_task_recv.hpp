#if !defined(AF_EXAMPLE_IO_URING_UDP_RECVMSG_MULTISHOT_TASK_FRAGMENT_INCLUDE)
#error "io_uring_udp_recvmsg_multishot_task_recv.hpp is a UdpRecvmsgMultishotTask implementation fragment"
#endif

af::TaskResult recv_one() {
    std::uint16_t buffer_id = 0;
    const af::IoStatus status = socket_.recv_from_multishot(
        *this,
        buffer_group,
        name_capacity,
        0,
        &buffer_id,
        recv_);
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
    if (!status.ready() || buffer_id >= buffer_count) {
        return stop_recv(EIO);
    }

    af::IoRecvmsgMultishotView view{};
    int parse_error = 0;
    if (!af::io_parse_recvmsg_multishot_buffer(
            buffers_[buffer_id],
            buffer_size,
            status.bytes,
            name_capacity,
            0,
            view,
            parse_error)) {
        return stop_recv(parse_error == 0 ? EIO : parse_error);
    }
    if (view.name_size < sizeof(sockaddr_in) || view.payload_size != 1U) {
        return stop_recv(EIO);
    }

    const auto* peer = reinterpret_cast<const sockaddr_in*>(
        buffers_[buffer_id] + view.name_offset);
    if (peer->sin_family != AF_INET || peer->sin_port != expected_port_) {
        return stop_recv(EIO);
    }
    ++*peer_count_;

    const int shifted = received_ == 0 ? 8 : 0;
    *packed_read_ |= static_cast<int>(
        static_cast<unsigned char>(buffers_[buffer_id][view.payload_offset])) << shifted;
    ++received_;

    const af::IoProvidedBuffer buffer{buffers_[buffer_id], buffer_size, buffer_id};
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
    if (!udp_recvmsg_async::cancel_io(UdpRecvmsgThread::IO_0, recv_)) {
        return complete(recv_.wait.error == 0 ? EIO : recv_.wait.error);
    }
    state_ = State::Cancel;
    return pending();
}

af::TaskResult finish_cancel() {
    std::uint16_t ignored = 0;
    const af::IoStatus status = socket_.recv_from_multishot(
        *this,
        buffer_group,
        name_capacity,
        0,
        &ignored,
        recv_);
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
    if (!udp_recvmsg_async::cancel_io(UdpRecvmsgThread::IO_0, recv_)) {
        return complete(recv_.wait.error == 0 ? finish_error_ : recv_.wait.error);
    }
    state_ = State::Cancel;
    return pending();
}
