import unittest
import json

from crowemi_trades.helpers.polygon import PolygonHelper


class TestPolygon(unittest.TestCase):
    def test_get_aggs(self):
        for x in range(3):
            d = PolygonHelper().get_aggregates(
                ticker="C:EURUSD",
                timespan="minute",
                interval=5,
                start_date="2023-01-03",
                end_date="2023-01-03",
                raw=True,
            )
            with open("tests/data.json", "w") as f:
                j = json.loads(d.data)
                f.write(json.dumps(j))
            assert d
