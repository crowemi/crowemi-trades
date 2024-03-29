from datetime import datetime
import os
import unittest
import json

from crowemi_trades.helpers.polygon import PolygonHelper


class TestPolygon(unittest.TestCase):
    def setUp(self) -> None:
        self.client = PolygonHelper()
        return super().setUp()

    def test_get_aggs(self):
        d = self.client.get_aggregates(
            ticker="C:EURUSD",
            timespan="minute",
            interval=1,
            start_date="2023-01-03",
            end_date="2023-01-03",
            raw=True,
        )
        self.assertEqual(d.get("status"), 200)

    def test_apply_timestamp(self):
        # 2023-01-02T23:55:00, 2023-01-02 23:50:00
        ret = self.client.apply_timestamp([{"t": 1672703700000}, {"t": 1672703400000}])
        self.assertEqual(ret[0]["timestamp_s"], "2023-01-02T23:55:00")
        self.assertEqual(ret[1]["timestamp_s"], "2023-01-02T23:50:00")


if __name__ == "__main__":
    unittest.main()
