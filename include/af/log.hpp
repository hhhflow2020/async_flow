#pragma once

#include "absl/log/absl_log.h"
#include "absl/log/log.h"

#include "af/runtime.hpp"

#include "af/log/detail/absl_log_sink.hpp"
#include "af/log/detail/async_logger.hpp"
#include "af/log/detail/file_log_backend.hpp"
#include "af/log/detail/log_backend.hpp"
#include "af/log/detail/network_log_backend.hpp"

#define AF_LOG(severity)                                                                           \
    ABSL_LOG_IF(severity, ::af::should_log(::af::detail::af_log_level_##severity))

#define AF_LOG_IF(severity, condition)                                                             \
    ABSL_LOG_IF(severity, ::af::should_log(::af::detail::af_log_level_##severity) && (condition))
