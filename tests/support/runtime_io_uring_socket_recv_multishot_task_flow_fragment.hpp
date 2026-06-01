#if !defined(AF_RUNTIME_IO_URING_RECV_MULTISHOT_TASK_FRAGMENT_INCLUDE)
#error "runtime_io_uring_socket_recv_multishot_task_flow_fragment.hpp is a UringRecvMultishotTask implementation fragment"
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
    return failed();
}
