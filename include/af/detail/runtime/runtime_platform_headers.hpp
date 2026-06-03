#pragma once

#include "af/detail/config.hpp"

#if !defined(_WIN32)
#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#endif

#if AF_DETAIL_HAS_EPOLL
#include <algorithm>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <unistd.h>
#endif

#if AF_DETAIL_HAS_KQUEUE
#include <sys/event.h>
#include <sys/time.h>
#include <unistd.h>
#endif
