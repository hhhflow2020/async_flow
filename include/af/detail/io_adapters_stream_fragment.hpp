#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_stream_fragment.hpp is an io_adapters implementation fragment"
#endif

template <typename ThreadT>
class IoStream : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

#include "af/detail/io_adapters_stream_recv_fragment.hpp"
#include "af/detail/io_adapters_stream_send_fragment.hpp"
#include "af/detail/io_adapters_stream_transfer_fragment.hpp"
#include "af/detail/io_adapters_stream_alias_fragment.hpp"
};
