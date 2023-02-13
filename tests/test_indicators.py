import unittest

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.indicators.smma_indicator import SmmaIndicator


class TestIndicator(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = S3Storage()
        self.bucket = "crowemi-trades"
        return super().setUp()

    def test_smma_indicator(
        self,
    ):
        list_objects = self.stor.get_list_objects(
            self.bucket,
            "C:EURUSD/minute/5/2022/01",
        )
        df = self.stor.read_all_parquet(self.bucket, list_objects)
        smma = SmmaIndicator(21, df)
        smma.calculate_sum()
