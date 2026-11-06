"""metricguard.core — 基础数据类型：标签集、指标类型、样本。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Tuple


class MetricType(Enum):
    """指标类型。Counter 单调递增，Gauge 可增可减。"""

    COUNTER = "counter"
    GAUGE = "gauge"


@dataclass(frozen=True)
class LabelSet:
    """一组有序、不可变、可哈希的标签。

    标签的键值对按键名排序后存储，因此同组标签无论以何种顺序插入都相等，
    可安全用作 dict / set 的键。空标签集代表无标签指标（单条 series）。
    """

    _pairs: Tuple[Tuple[str, str], ...] = ()

    @classmethod
    def from_mapping(cls, labels: Mapping[str, str] | None = None) -> "LabelSet":
        """从普通 dict 构造，键值统一转字符串并按键名排序。"""
        if not labels:
            return cls(())
        pairs = tuple(sorted((str(k), str(v)) for k, v in labels.items()))
        return cls(pairs)

    def as_dict(self) -> dict[str, str]:
        return dict(self._pairs)

    def __len__(self) -> int:
        return len(self._pairs)

    def __bool__(self) -> bool:
        return bool(self._pairs)

    def __str__(self) -> str:
        return "{" + ", ".join(f"{k}={v}" for k, v in self._pairs) + "}"


@dataclass(frozen=True)
class Series:
    """一条时间序列：指标名 + 标签集 + 当前值 + 采样时间戳（Unix 秒）。"""

    metric: str
    labels: LabelSet
    value: float
    timestamp: float

    def key(self) -> tuple[str, LabelSet]:
        """序列标识：同一指标 + 同一标签集即同一 series。"""
        return (self.metric, self.labels)
