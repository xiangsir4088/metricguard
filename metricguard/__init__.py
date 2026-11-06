"""metricguard — 指标采集与下发库。

一个面向可观测性 agent 的轻量指标库：采集带标签的样本，经 Emitter 下发到 Sink
（远端 TSDB / 内存 / 文件）。核心职责是「采集 → 守卫 → 下发」这条发射路径。
"""

from .core import LabelSet, MetricType, Series
from .emitter import Emitter, InMemorySink, Sink
from .cardinality import CardinalityGuard

__all__ = [
    "LabelSet",
    "MetricType",
    "Series",
    "Emitter",
    "Sink",
    "InMemorySink",
    "CardinalityGuard",
]

__version__ = "1.0.0"
