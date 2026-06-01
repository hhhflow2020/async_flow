#if !defined(AF_IO_ADAPTERS_DETAIL_INCLUDE)
#error "io_adapters_aliases.hpp is internal to af/io_adapters.hpp"
#endif

template <typename ThreadT> using TcpStream = IoStream<ThreadT>;

template <typename ThreadT> using TcpListener = IoListener<ThreadT>;

template <typename ThreadT> using UdpSocket = IoDatagramSocket<ThreadT>;
