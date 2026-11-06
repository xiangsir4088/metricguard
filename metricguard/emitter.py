"""metricguard.emitter — 发射路径：把 Series 经守卫下发给 Sink。

发射路径是这条链路的心脏：

    Registry / 采集侧  ->  Emitter.emit(series)  ->  CardinalityGuard.check/record  ->  Sink.write

当前 Emitter 已经接好守卫挂点（guard 参数），但守卫本身是未实现的口子（见
cardinality.py）。本模块是模型需要理解的「发射路径」主体。
"""

from __future__ import annotations

import threading
from typing import Protocol

from .core import Series


class Sink(Protocol):
    """下发目标。write 收到一条已通过守卫的 series。"""

    def write(self, series: Series) -> None: ...


class InMemorySink:
    """内存 Sink：把已下发样本按指标名聚集成 list，供断言与调试。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_metric: dict[str, list[Series]] = {}

    def write(self, series: Series) -> None:
        with self._lock:
            self._by_metric.setdefault(series.metric, []).append(series)

    def series_for(self, metric: str) -> list[Series]:
        with self._lock:
            return list(self._by_metric.get(metric, []))

    def distinct_labels(self, metric: str) -> set:
        """该指标下已下发样本的不同标签集个数（series 基数）。"""
        return {s.labels for s in self.series_for(metric)}

    def total_samples(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._by_metric.values())


class Emitter:
    """发射器。guard 为空时不做任何限制（默认行为），有 guard 时走守卫。"""

    def __init__(self, sink: Sink, *, guard=None) -> None:
        self._sink = sink
        self._guard = guard

    @property
    def guard(self):
        return self._guard

    def emit(self, series: Series) -> bool:
        """发射一条 series。返回 True 表示已下发，False 表示被守卫丢弃。

        守卫挂点已接好：有 guard 时先 check（是否允许下发），下发后再 record
        （推进该 series 的活跃时间）。守卫实现见 metricguard.cardinality。
        """
        if self._guard is not None:
            if not self._guard.check(series.metric, series.labels, series.timestamp):
                return False
        self._sink.write(series)
        if self._guard is not None:
            self._guard.record(series.metric, series.labels, series.timestamp)
        return True

    def emit_all(self, series_list) -> list[bool]:
        """批量发射，返回每条是否下发的布尔列表。"""
        return [self.emit(s) for s in series_list]
