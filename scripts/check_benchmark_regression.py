#!/usr/bin/env python3
"""Compare Google Benchmark JSON output against a checked-in baseline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


UNIT_TO_NS = {
    "ns": 1.0,
    "us": 1_000.0,
    "ms": 1_000_000.0,
    "s": 1_000_000_000.0,
}


def to_ns(value: float, unit: str) -> float:
    try:
        return value * UNIT_TO_NS[unit]
    except KeyError as exc:
        raise ValueError(f"unsupported benchmark time unit: {unit}") from exc


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def result_by_name(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    benchmarks = result.get("benchmarks", [])
    results: dict[str, dict[str, Any]] = {}
    for item in benchmarks:
        if "name" not in item:
            continue

        run_type = item.get("run_type", "iteration")
        if run_type == "iteration":
            results.setdefault(item["name"], item)
            continue

        if run_type == "aggregate" and item.get("aggregate_name") == "median":
            base_name = item.get("run_name", item["name"])
            if base_name.endswith("_median"):
                base_name = base_name[:-7]
            results[base_name] = item

    return results


def check_regressions(result_path: Path, baseline_path: Path) -> int:
    result = load_json(result_path)
    baseline = load_json(baseline_path)

    metric = baseline.get("metric", "real_time")
    default_unit = baseline.get("unit", "ns")
    default_threshold = float(baseline.get("default_max_regression", 0.25))
    results = result_by_name(result)

    failures: list[str] = []
    checked = 0
    for name, spec in baseline.get("benchmarks", {}).items():
        if name not in results:
            failures.append(f"{name}: missing from benchmark output")
            continue

        observed = results[name]
        baseline_value = float(spec[metric])
        baseline_unit = spec.get("unit", default_unit)
        observed_value = float(observed[metric])
        observed_unit = observed.get("time_unit", baseline_unit)
        threshold = float(spec.get("max_regression", default_threshold))

        baseline_ns = to_ns(baseline_value, baseline_unit)
        observed_ns = to_ns(observed_value, observed_unit)
        limit_ns = baseline_ns * (1.0 + threshold)
        checked += 1

        if observed_ns > limit_ns:
            failures.append(
                f"{name}: {observed_ns / baseline_ns:.2f}x baseline "
                f"(limit {1.0 + threshold:.2f}x)"
            )

    if failures:
        print("Benchmark regression check failed:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print(f"Benchmark regression check passed ({checked} baseline entries).")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_json", type=Path)
    parser.add_argument("baseline_json", type=Path)
    args = parser.parse_args()
    return check_regressions(args.result_json, args.baseline_json)


if __name__ == "__main__":
    sys.exit(main())
