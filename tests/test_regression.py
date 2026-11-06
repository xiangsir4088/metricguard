"""回归测试：基础数据类型与发射路径（未开启守卫时的既有行为）。"""

import unittest

from metricguard.core import LabelSet, MetricType, Series
from metricguard.emitter import Emitter, InMemorySink
from metricguard.cardinality import CardinalityGuard


class TestLabelSet(unittest.TestCase):
    def test_from_mapping_sorted_and_hashable(self):
        a = LabelSet.from_mapping({"b": "2", "a": "1"})
        b = LabelSet.from_mapping({"a": "1", "b": "2"})
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        self.assertEqual(a.as_dict(), {"a": "1", "b": "2"})

    def test_empty_labelset(self):
        self.assertEqual(len(LabelSet.from_mapping()), 0)
        self.assertFalse(LabelSet.from_mapping())
        self.assertTrue(LabelSet.from_mapping({"x": "y"}))

    def test_values_stringified(self):
        ls = LabelSet.from_mapping({"code": 200})
        self.assertEqual(ls.as_dict(), {"code": "200"})


class TestSeries(unittest.TestCase):
    def test_key_equality(self):
        s1 = Series("m", LabelSet.from_mapping({"a": "1"}), 1.0, 0.0)
        s2 = Series("m", LabelSet.from_mapping({"a": "1"}), 2.0, 1.0)
        self.assertEqual(s1.key(), s2.key())
        s3 = Series("m", LabelSet.from_mapping({"a": "2"}), 1.0, 0.0)
        self.assertNotEqual(s1.key(), s3.key())


class TestEmitterNoGuard(unittest.TestCase):
    def test_no_guard_emits_everything(self):
        sink = InMemorySink()
        emitter = Emitter(sink)
        series = [
            Series("m", LabelSet.from_mapping({"id": str(i)}), float(i), float(i))
            for i in range(10)
        ]
        results = emitter.emit_all(series)
        self.assertTrue(all(results))
        self.assertEqual(sink.total_samples(), 10)
        self.assertEqual(len(sink.distinct_labels("m")), 10)

    def test_guard_enabled_but_stub_does_not_limit(self):
        """当前守卫是桩：开启后仍不限制（缺口）。模型实现后此断言会改变。"""
        sink = InMemorySink()
        guard = CardinalityGuard(max_series_per_metric=3, max_total_series=100)
        emitter = Emitter(sink, guard=guard)
        series = [
            Series("m", LabelSet.from_mapping({"id": str(i)}), 1.0, float(i))
            for i in range(10)
        ]
        results = emitter.emit_all(series)
        self.assertTrue(all(results))  # 桩：全部放行
        self.assertEqual(sink.total_samples(), 10)


class TestGuardInterface(unittest.TestCase):
    def test_limits_contract(self):
        guard = CardinalityGuard(max_series_per_metric=10, max_total_series=100,
                                 inactive_ttl_seconds=30)
        self.assertEqual(guard.limits()["max_series_per_metric"], 10)
        self.assertEqual(guard.limits()["max_total_series"], 100)
        self.assertEqual(guard.limits()["inactive_ttl_seconds"], 30)

    def test_stub_stats_zero(self):
        guard = CardinalityGuard()
        self.assertEqual(guard.dropped_series(), 0)
        self.assertEqual(guard.active_series("any"), 0)
        self.assertEqual(guard.total_active_series(), 0)


if __name__ == "__main__":
    unittest.main()
