#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_datagram_fragment.hpp is an io_adapters implementation fragment"
#endif

template <typename ThreadT>
class IoDatagramSocket : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

#include "af/detail/io_adapters_datagram_lifecycle_fragment.hpp"
#include "af/detail/io_adapters_datagram_recv_fragment.hpp"
#include "af/detail/io_adapters_datagram_send_fragment.hpp"
};
