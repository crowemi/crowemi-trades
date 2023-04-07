import unittest
from datetime import datetime

import polars as pl

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.indicators.smma_indicator import SmmaIndicator


class TestSmmaIndicator(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = S3Storage()
        self.bucket = "crowemi-trades"
        return super().setUp()

    def test_smma_indicator(
        self,
    ):
        pass
