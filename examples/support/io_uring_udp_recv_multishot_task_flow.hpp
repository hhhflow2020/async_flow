#if !defined(AF_EXAMPLE_IO_URING_UDP_RECV_MULTISHOT_TASK_FRAGMENT_INCLUDE)
#error "io_uring_udp_recv_multishot_task_flow.hpp is a UdpRecvMultishotTask implementation fragment"
#endif

af::TaskResult run() override {
    switch (state_) {
    case State::Register:
        return register_ring();
    case State::Recv:
        return recv_one();
    case State::Cancel:
        return finish_cancel();
    case State::Unregister:
        return unregister_ring();
    }
    return complete(EIO);
}

af::TaskResult complete(int error) {
    if (registered_) {
        int unregister_error = 0;
        if (udp_recv_async::io_unregister_provided_buffer_ring(
                UdpRecvThread::IO_0,
                buffer_group,
                &unregister_error)) {
            registered_ = false;
        }
    }
    error_->store(error, std::memory_order_release);
    return done();
}
