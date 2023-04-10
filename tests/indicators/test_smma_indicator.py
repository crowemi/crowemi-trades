import os
import unittest
from datetime import datetime

import polars as pl

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.indicators.smma_indicator import SmmaIndicator


class TestSmmaIndicator(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = S3Storage(
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            endpoint_override="http://localhost:4566",
            region="us-east-1",
        )
        self.bucket = "crowemi-trades"
        return super().setUp()

    def test_smma_indicator(
        self,
    ):
        pass


if __name__ == "__main__":
    unittest.main()
