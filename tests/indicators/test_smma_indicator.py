import os
import unittest
from datetime import datetime, timedelta

from polars import DataFrame, Series

from crowemi_trades.storage.mongodb_storage import MongoDbStorage
from crowemi_trades.indicators.smma_indicator import SmmaIndicator


class TestSmmaIndicator(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = MongoDbStorage(os.getenv("MONGODB_URI"))

        data = list()
        dt = datetime.now()
        for r in range(20):
            current_date = dt + timedelta(r)
            data.append({"timestamp_s": current_date.isoformat(), "close": 1})

        self.dataset = DataFrame(data)
        self.period = 5
        return super().setUp()

    def test_generate_period_sum(
        self,
    ):
        ret = SmmaIndicator.generate_period_sum(self.dataset, self.period)
        for r in ret.rows(named=True):
            if r.row_number <= self.period:
                self.assertEqual(r.row_number, r.sum)
            else:
                self.assertEqual(r.sum, 6)

    def test_generate_start_smma(
        self,
    ):
        # TODO: build out test for generating the start SMMA
        dataset = SmmaIndicator.generate_period_sum(self.dataset, self.period)
        ret = SmmaIndicator.calculate(dataset, self.period)
        r = ret.row(0, named=True)
        self.assertEqual(len(ret), 15)
        self.assertEqual(r.i_smma_5, 1)

    def test_run(
        self,
    ):
        dataset = SmmaIndicator().run(
            self.dataset,
            period=self.period,
        )
        self.assertEqual(len(dataset), len(self.dataset))
        self.assertEqual(
            dataset.columns, ["timestamp_s", "close", f"i_smma_{self.period}"]
        )
        for i, r in enumerate(dataset.rows(named=True)):
            if i < self.period:
                self.assertEqual(r[2], None)
            else:
                self.assertEqual(r[2], 1.0)


if __name__ == "__main__":
    unittest.main()
