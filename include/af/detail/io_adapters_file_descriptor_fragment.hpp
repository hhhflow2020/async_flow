#if !defined(AF_IO_ADAPTERS_FRAGMENT_INCLUDE)
#error "io_adapters_file_descriptor_fragment.hpp is an io_adapters implementation fragment"
#endif

template <typename ThreadT>
class IoFile : public IoDescriptor<ThreadT> {
public:
    using IoDescriptor<ThreadT>::IoDescriptor;

#include "af/detail/io_adapters_file_descriptor_read_fragment.hpp"
#include "af/detail/io_adapters_file_descriptor_write_fragment.hpp"
#include "af/detail/io_adapters_file_descriptor_fixed_fragment.hpp"
#include "af/detail/io_adapters_file_descriptor_sync_fragment.hpp"
};
