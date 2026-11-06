"""metricguard.cardinality — 卡迪纳利守卫（Cardinality Guard）。

════════════════════════════════════════════════════════════════════════
真实场景（SRE 复盘）
════════════════════════════════════════════════════════════════════════
某服务的请求计数器被打上了高基数标签（例如把 user_id / request_id 直接当作
label 打进来）。指标库不设上限，于是：

    · agent 进程内存被海量 series 撑爆（每条 label 集都要驻留状态）
    · 远端 TSDB 收到爆炸式的 series 写入，写放大严重、查询退化
    · 一个业务配置错误把整个可观测性链路拖垮

结论：指标下发路径必须加一道「卡迪纳利守卫」，限制同一指标名下以及全库范围内
的**不同标签集（series 基数）**数量。超过上限的新 series 直接丢弃并计数，避免
把问题扩散到下游；长期不活跃的 series 让出的额度按 TTL 回收，使指标在波动后能
恢复容量。

════════════════════════════════════════════════════════════════════════
待实现（功能补全）
════════════════════════════════════════════════════════════════════════
CardinalityGuard 目前是**不限制的桩**：check() 恒返回 True，record() 不做事，
所有统计恒为 0。也就是说 agent 开启 --max-series-per-metric 等参数后**实际什么
都不限**——这正是要修复的缺陷。

请实现 CardinalityGuard，使其满足下面「行为契约」。实现方式不限（内部数据结构、
回收算法、加锁方式均可自选），但可观察行为必须符合契约；且**不得**破坏未开启
守卫时的既有行为，**不得**为通过测试而写死任何固定输入。
"""

from __future__ import annotations

import threading

from .core import LabelSet


class CardinalityGuard:
    """限制指标下发路径上不同标签集（series 基数）的数量。

    ────────────────────────────────────────────────────────────────────
    行为契约（模型必须实现的可观察行为）
    ────────────────────────────────────────────────────────────────────

    关键概念：**不同标签集（series 基数）** 是计数的单位。同一 metric 下，
    每条不同的 LabelSet 计为一条 series；同一 LabelSet 多次出现属于「同一条
    series 的多个样本」，**只计一次**，绝不重复计数。

    1. 每指标上限  max_series_per_metric
       同一 metric 名下「当前活跃」的不同标签集个数不得超过该上限。达到上限
       后，**新的**标签集（该指标名下此前未出现过、且未被 TTL 回收的标签集）
       应被 check() 判为丢弃（返回 False）。

    2. 全局预算  max_total_series
       所有指标名下「当前活跃」的不同标签集总数不得超过该上限（全局预算）。
       某指标即使尚未达到自身每指标上限，若全局预算已耗尽，其新标签集同样应
       被丢弃。

    3. 已活跃的 series 不被误杀
       在 TTL 有效期内（尚未被回收）的同一 metric + 同一 LabelSet，之后再次
       出现属于「同一条 series 的新样本」，**必须始终**被允许下发（返回 True），
       不得因计数、时间或顺序而被误判为丢弃，也不得重复计数。

    4. TTL 回收  inactive_ttl_seconds
       某 series 距其最后一次被 record()（或首次被允许下发）的时间间隔超过
       inactive_ttl_seconds 时，视为已不活跃，其占用的额度（该指标计数 + 全局
       计数）应被**回收**，使后续新标签集可重新获得下发资格。回收只释放额度，
       不影响已下发的历史样本。被回收后的同一 LabelSet 若再次出现，按「新标签
       集」重新判定（受上限约束）。

    5. 阈值语义
       不同标签集个数**恰好等于**上限时，已有的每个活跃标签集都允许下发；
       **下一个新标签集**才被丢弃。回收后计数应严格回落。

    6. 并发安全（务必在 check 内原子完成预留）
       check() 与 record() 会在并发线程中被调用（发射路径可能多线程）。check()
       必须在锁内**原子完成**基数判定与计数预留（允许则登记该标签集并递增全局
       计数，拒绝则递增丢弃计数），不得把「判定」与「计数」拆到 record() 里——
       否则并发下多个线程会同时通过上限判定导致计数失真、基数超限。record()
       仅推进活跃时间（锁内）。最终计数不得因并发而失真或超限。

    7. 统计口径
       dropped_series()：因超出上限而被丢弃的**新标签集**总数（只计真正丢弃的）。
       active_series(metric)：该指标当前占用额度的不同标签集个数。
       total_active_series()：全库当前占用额度的不同标签集个数。
       三个统计在回收后必须与内部状态严格一致。

    8. 时钟
       时间由调用方以 now（Unix 秒，float）传入，守卫**不得**自行读取系统时钟
       （保证可复现测试）；也不得依赖样本到达顺序以外的任何外部状态。

    # ──────────────────────────────────────────────────────────────────
    # 以下为当前桩实现（不限制）。请整体替换为满足契约的实现。
    # ──────────────────────────────────────────────────────────────────
    """

    def __init__(
        self,
        *,
        max_series_per_metric: int = 5000,
        max_total_series: int = 50000,
        inactive_ttl_seconds: float = 3600.0,
    ) -> None:
        self._per_metric_limit = int(max_series_per_metric)
        self._total_limit = int(max_total_series)
        self._ttl = float(inactive_ttl_seconds)
        self._lock = threading.RLock()
        # —— 桩状态（实现时可自由替换）——
        self._dropped = 0

    # ---- 对外契约 ----

    def check(self, metric: str, labels: LabelSet, now: float) -> bool:
        """判断一条 series 是否允许下发。返回 True=允许，False=丢弃。

        当前桩：恒返回 True（不限制）。请实现为按契约原子判定并预留计数。
        """
        return True

    def record(self, metric: str, labels: LabelSet, now: float) -> None:
        """标记一条已下发 series 在 now 时刻活跃。

        当前桩：不做事。请实现为推进该 series 的最后活跃时间（锁内）。
        """

    def dropped_series(self) -> int:
        """因超限被丢弃的新标签集总数。"""
        return self._dropped

    def active_series(self, metric: str) -> int:
        """指定指标当前占用额度的不同标签集个数。"""
        return 0

    def total_active_series(self) -> int:
        """全库当前占用额度的不同标签集个数。"""
        return 0

    def limits(self) -> dict:
        """返回当前配置的上限与 TTL（供展示/测试核对）。"""
        return {
            "max_series_per_metric": self._per_metric_limit,
            "max_total_series": self._total_limit,
            "inactive_ttl_seconds": self._ttl,
        }
