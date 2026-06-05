#pragma once

// Legacy opt-in async IO facade. New code should use af/runtime.hpp and af/net.hpp.

#include "af/platform.hpp"

#include "af/io_types.hpp"
#include "af/io_common.hpp"
#include "af/io_socket.hpp"
#include "af/io_file.hpp"
#include "af/io_filesystem.hpp"
#include "af/io_datagram.hpp"
#include "af/io_event_timer.hpp"
#include "af/io_timeout.hpp"
#include "af/io_adapters.hpp"
