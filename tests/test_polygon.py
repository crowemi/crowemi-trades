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
            interval=5,
            start_date="2023-01-03",
            end_date="2023-01-03",
            raw=True,
        )
        self.assertEqual(d.status, 200)
