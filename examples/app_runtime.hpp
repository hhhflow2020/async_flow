#pragma once

#include <cstdint>

#include "af/runtime.hpp"

inline constexpr std::uint16_t player_logic_shard_count = 4;

[[nodiscard]] inline af::runtime_config make_app_runtime_config() {
    af::runtime_config config;
    config.threads = {
        af::cpu_threads("logic", player_logic_shard_count),
        af::cpu_threads("db", 1),
        af::io_threads("io", 1),
    };
    return config;
}
