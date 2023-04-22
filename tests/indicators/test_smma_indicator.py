import os
import unittest
from datetime import datetime, timedelta

from polars import DataFrame, Series

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.indicators.smma_indicator import SmmaIndicator


class TestSmmaIndicator(unittest.TestCase):
    def setUp(self) -> None:
        # self.indicator = SmmaIndicator()
        self.stor = S3Storage(
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            endpoint_override="http://localhost:4566",
            region="us-east-1",
        )
        self.bucket = "crowemi-trades"

        data = list()
        dt = datetime.now()
        for r in range(20):
            current_date = dt + timedelta(r)
            data.append({"ts": current_date.isoformat(), "c": 1})

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

    def test_generate_start_smma(self,):
        # TODO: build out test for generating the start SMMA
        dataset = SmmaIndicator.generate_period_sum(self.dataset, self.period)
        ret = SmmaIndicator.generate_period_average(dataset, self.period)
        r = ret.row(0, named=True)
        self.assertEqual(len(ret), 1)
        self.assertEqual(r.avg, 1)

    def test_has_start_smma(self,):
        ret = SmmaIndicator.has_start_smma(self.dataset, self.period)
        self.assertEqual(ret, False)
        
        i_smma_period = []
        for r in range(len(self.dataset)):
            if r != self.period:
                i_smma_period.append(None)
            else:
                i_smma_period.append(1.1)

        ds: DataFrame = self.dataset.with_columns(Series(name=f"i_smma_{self.period}", values=i_smma_period))
        ret = SmmaIndicator().has_start_smma(ds, self.period)
        self.assertEqual(ret, True)

    def test_run(self,):
        dataset = SmmaIndicator().run(self.dataset, period=self.period,)
        self.assertEqual(len(dataset), len(self.dataset))
        self.assertEqual(dataset.columns, ["ts", "c", f"i_smma_{self.period}"])
        for i, r in enumerate(dataset.rows(named=True)):
            if i < self.period:
                self.assertEqual(r[2], None)
            else:
                self.assertEqual(r[2], 1.0)

if __name__ == "__main__":
    unittest.main()
