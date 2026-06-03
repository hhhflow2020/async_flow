#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "io_rpc_length_prefixed_runtime.hpp"

#if defined(__linux__)

namespace io_rpc_length_prefixed_example {

#include "io_rpc_length_prefixed_process_task_decl.hpp"
#include "io_rpc_length_prefixed_server_task.hpp"
#include "io_rpc_length_prefixed_process_task_impl.hpp"

} // namespace io_rpc_length_prefixed_example

#else

namespace io_rpc_length_prefixed_example {

class RpcServerTask final : public RpcTask {
public:
    explicit RpcServerTask(RpcTask::FactoryToken token) : RpcTask(token) {}

    bool do_it(int listener_fd, bool *ok, int *error) {
        static_cast<void>(listener_fd);
        static_cast<void>(ok);
        static_cast<void>(error);
        return false;
    }

private:
    af::TaskResult run() override {
        return failed();
    }
};

} // namespace io_rpc_length_prefixed_example

#endif
