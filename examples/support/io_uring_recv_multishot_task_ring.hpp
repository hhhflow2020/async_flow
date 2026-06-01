#if !defined(AF_EXAMPLE_IO_URING_RECV_MULTISHOT_TASK_FRAGMENT_INCLUDE)
#error "io_uring_recv_multishot_task_ring.hpp is a RecvMultishotTask implementation fragment"
#endif

af::TaskResult register_ring() {
    int init_error = 0;
    if (!ring_.init(buffer_count, init_error)) {
        return complete(init_error == 0 ? EIO : init_error);
    }

    af::IoProvidedBuffer provided[buffer_count]{};
    for (std::uint16_t i = 0; i < buffer_count; ++i) {
        provided[i] = af::IoProvidedBuffer{&buffers_[i], sizeof(buffers_[i]), i};
    }
    int add_error = 0;
    if (!ring_.add(provided, buffer_count, add_error)) {
        return complete(add_error == 0 ? EIO : add_error);
    }

    int register_error = 0;
    if (!recv_async::io_register_provided_buffer_ring(
            RecvThread::IO_0,
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
        if (!recv_async::io_unregister_provided_buffer_ring(
                RecvThread::IO_0,
                buffer_group,
                &unregister_error)) {
            return complete(unregister_error == 0 ? EIO : unregister_error);
        }
        registered_ = false;
    }
    return complete(finish_error_);
}
