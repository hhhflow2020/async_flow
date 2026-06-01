#if !defined(IO_RPC_LENGTH_PREFIXED_SERVER_FRAGMENT_INCLUDE)
#error "io_rpc_length_prefixed_process_task_decl.hpp is an RPC server implementation fragment"
#endif

class RpcServerTask;

class RpcProcessTask final : public RpcTask {
public:
    explicit RpcProcessTask(RpcTask::FactoryToken token) : RpcTask(token) {}

    bool do_it(RpcServerTask* server);

private:
    af::TaskResult run() override;

    RpcServerTask* server_{nullptr};
};
