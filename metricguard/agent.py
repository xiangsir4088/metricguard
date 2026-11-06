"""metricguard.agent — 命令行入口。

一个模拟可观测性 agent 的采集/下发程序：读一批带标签的样本，经 Emitter 下发到
InMemorySink 或文件，并在启用 --max-series-per-metric 等参数时接入 CardinalityGuard。

用法示例：

    # 不开启守卫（默认行为，无任何限制）
    python -m metricguard.agent --samples samples.json

    # 开启卡迪纳利守卫
    python -m metricguard.agent --samples samples.json \
        --max-series-per-metric 100 --max-total-series 1000 \
        --inactive-ttl-seconds 3600

输出为 JSON：每条样本是否下发、守卫统计、各指标下发后的 series 基数。
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .core import LabelSet, Series
from .emitter import Emitter, InMemorySink
from .cardinality import CardinalityGuard


def _load_samples(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("samples 文件必须是一个 JSON 数组")
    return data


def _to_series(raw: dict) -> Series:
    return Series(
        metric=str(raw["metric"]),
        labels=LabelSet.from_mapping(raw.get("labels") or {}),
        value=float(raw["value"]),
        timestamp=float(raw["timestamp"]),
    )


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="metricguard 指标采集/下发 agent（含可选卡迪纳利守卫）"
    )
    parser.add_argument("--samples", required=True, help="样本 JSON 文件路径")
    parser.add_argument("--out", help="下发结果写入的 JSON 路径（缺省打印到 stdout）")
    parser.add_argument("--max-series-per-metric", type=int, default=0,
                        help="每指标不同标签集上限；0 表示不开启守卫")
    parser.add_argument("--max-total-series", type=int, default=0,
                        help="全库不同标签集上限；0 表示不开启守卫")
    parser.add_argument("--inactive-ttl-seconds", type=float, default=3600.0,
                        help="不活跃 series 的回收 TTL（秒）")
    args = parser.parse_args(argv)

    sink = InMemorySink()
    guard = None
    if args.max_series_per_metric > 0 or args.max_total_series > 0:
        guard = CardinalityGuard(
            max_series_per_metric=args.max_series_per_metric or 5000,
            max_total_series=args.max_total_series or 50000,
            inactive_ttl_seconds=args.inactive_ttl_seconds,
        )
    emitter = Emitter(sink, guard=guard)

    samples = _load_samples(args.samples)
    results: list[dict[str, Any]] = []
    for raw in samples:
        series = _to_series(raw)
        emitted = emitter.emit(series)
        results.append(
            {
                "metric": series.metric,
                "labels": series.labels.as_dict(),
                "emitted": emitted,
            }
        )

    guard_stats = None
    if guard is not None:
        guard_stats = {
            "dropped_series": guard.dropped_series(),
            "total_active_series": guard.total_active_series(),
        }

    # 汇总每个指标下发的不同标签集基数
    per_metric: dict[str, int] = {}
    for raw in samples:
        series = _to_series(raw)
        if series.metric not in per_metric:
            per_metric[series.metric] = len(sink.distinct_labels(series.metric))

    report = {
        "guard_enabled": guard is not None,
        "guard_stats": guard_stats,
        "samples": results,
        "per_metric_cardinality": per_metric,
    }

    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
