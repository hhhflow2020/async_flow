#pragma once

#include "absl/log/absl_log.h"
#include "absl/log/log.h"

#include "af/runtime.hpp"

#include "af/log/file_backend.hpp"
#include "af/log/log_backend.hpp"
#include "af/log/logger.hpp"
#include "af/log/tcp_backend.hpp"
#include "af/log/udp_backend.hpp"

#define AF_LOG(severity)                                                                           \
    ABSL_LOG_IF(severity, ::af::should_log(::af::detail::af_log_level_##severity))

#define AF_LOG_IF(severity, condition)                                                             \
    ABSL_LOG_IF(severity, ::af::should_log(::af::detail::af_log_level_##severity) && (condition))
