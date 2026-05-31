#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_aliases_fragment.hpp is an io_adapters implementation fragment"
#endif

template <typename ThreadT>
using TcpStream = IoStream<ThreadT>;

template <typename ThreadT>
using TcpListener = IoListener<ThreadT>;

template <typename ThreadT>
using UdpSocket = IoDatagramSocket<ThreadT>;

