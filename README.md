# metricguard

面向可观测性 agent 的轻量指标采集与下发库。

## 背景

`metricguard` 是模拟可观测性采集 agent 的指标库：采集带标签的样本，经
`Emitter` 沿「采集 → 守卫 → 下发」这条发射路径送进 `Sink`（内存 / 文件 /
远端 TSDB）。代码刻意保持小而清晰，便于理解发射路径与扩展守卫。

## 结构

```
metricguard/
├── __init__.py        # 包导出
├── core.py            # LabelSet / MetricType / Series
├── emitter.py         # Emitter（发射路径）+ Sink / InMemorySink
├── cardinality.py     # CardinalityGuard（卡迪纳利守卫，见下）
└── agent.py           # 命令行入口
```

## 快速上手

```bash
# 不开启守卫：默认行为，无任何限制
python -m metricguard.agent --samples samples.json

# 开启卡迪纳利守卫
python -m metricguard.agent --samples samples.json \
    --max-series-per-metric 100 --max-total-series 1000
```

`samples.json` 形如：

```json
[
  {"metric": "http_requests_total", "labels": {"method": "GET", "code": "200"}, "value": 1, "timestamp": 1700000000.0},
  {"metric": "http_requests_total", "labels": {"method": "GET", "code": "500"}, "value": 1, "timestamp": 1700000000.5}
]
```

## CardinalityGuard（卡迪纳利守卫）

真实运维场景：某服务把 `user_id` / `request_id` 等**高基数标签**直接打进指标，
导致 agent 内存被海量 series 撑爆、远端 TSDB 写入放大。守卫在发射路径上限制
**不同标签集（series 基数）**的数量：每指标上限 + 全局预算 + TTL 回收 + 丢弃
计数，防止一个误配置拖垮整条可观测性链路。

> ⚠️ 当前 `CardinalityGuard` 为**未实现的口子**（不限制的桩）。开启
> `--max-series-per-metric` 后实际不会限制——这是本库待补全的功能缺口。
> 实现契约见 `metricguard/cardinality.py` 顶部的「行为契约」docstring。
