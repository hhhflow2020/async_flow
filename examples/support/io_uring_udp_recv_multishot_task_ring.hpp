#if !defined(AF_EXAMPLE_IO_URING_UDP_RECV_MULTISHOT_TASK_FRAGMENT_INCLUDE)
#error "io_uring_udp_recv_multishot_task_ring.hpp is a UdpRecvMultishotTask implementation fragment"
#endif

af::TaskResult register_ring() {
    int init_error = 0;
    if (!ring_.init(buffer_count, init_error)) {
        return complete(init_error == 0 ? EIO : init_error);
    }

    af::IoProvidedBuffer buffers[buffer_count]{};
    for (std::uint16_t i = 0; i < buffer_count; ++i) {
        buffers[i] = af::IoProvidedBuffer{&buffers_[i], sizeof(buffers_[i]), i};
    }
    int add_error = 0;
    if (!ring_.add(buffers, buffer_count, add_error)) {
        return complete(add_error == 0 ? EIO : add_error);
    }

    int register_error = 0;
    if (!udp_recv_async::io_register_provided_buffer_ring(
            UdpRecvThread::IO_0,
            ring_.ring(),
            ring_.entries(),
            buffer_group,
            &register_error)) {
        return complete(register_error == 0 ? EIO : register_error);
    }
    registered_ = true;
    state_ = State::Recv;
    return again();
}

af::TaskResult unregister_ring() {
    if (registered_) {
        int unregister_error = 0;
        if (!udp_recv_async::io_unregister_provided_buffer_ring(
                UdpRecvThread::IO_0,
                buffer_group,
                &unregister_error)) {
            return complete(unregister_error == 0 ? EIO : unregister_error);
        }
        registered_ = false;
    }
    return complete(finish_error_);
}
