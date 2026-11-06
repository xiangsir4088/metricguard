"""回归测试：基础数据类型与发射路径的既有行为（不得被卡迪纳利守卫实现破坏）。

注意：本套用例是「可见回归」——修复卡迪纳利守卫后也必须保持全绿。因此只断言
与守卫实现无关、或任何正确实现都应成立的行为；不要把「桩不限制」这类缺陷编码
成断言。
"""

import unittest

from metricguard.core import LabelSet, Series
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
        """未开启守卫时，任何实现都不得改变发射行为：全部下发。"""
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


class TestGuardInterface(unittest.TestCase):
    def test_limits_contract(self):
        guard = CardinalityGuard(max_series_per_metric=10, max_total_series=100,
                                 inactive_ttl_seconds=30)
        self.assertEqual(guard.limits()["max_series_per_metric"], 10)
        self.assertEqual(guard.limits()["max_total_series"], 100)
        self.assertEqual(guard.limits()["inactive_ttl_seconds"], 30)

    def test_methods_callable(self):
        """守卫对外方法签名必须存在且可调用（实现与否不影响签名）。"""
        guard = CardinalityGuard()
        ls = LabelSet.from_mapping({"a": "1"})
        self.assertTrue(callable(guard.check))
        self.assertTrue(callable(guard.record))
        self.assertTrue(callable(guard.dropped_series))
        self.assertTrue(callable(guard.active_series))
        self.assertTrue(callable(guard.total_active_series))
        # check 必须返回 bool
        self.assertIsInstance(guard.check("m", ls, 0.0), bool)


if __name__ == "__main__":
    unittest.main()
