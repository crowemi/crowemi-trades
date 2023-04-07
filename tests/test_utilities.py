import unittest
from datetime import datetime, timedelta

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.utilities.get_daily_data import get_daily_data


class TestUtilities(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = S3Storage(
            access_key="test",
            secret_access_key="test",
            endpoint_override="http://localhost:4566",
        )
        return super().setUp()

    def test_get_daily_data(self):
        today = datetime.now() + timedelta(-1)
        # "ticket": "C:EURUSD", "interval": "5", "timespan": "minute", "bucket": "crowemi-trades", "last_modified": "20230404"
        ret, keys = get_daily_data(
            ticker="C:EURUSD",
            interval=5,
            timespan="minute",
            start_date=datetime(today.year, today.month, today.day),
            end_date=datetime(today.year, today.month, today.day),
            bucket="crowemi-trades",
            storage=self.stor,
        )
        self.assertTrue(ret)
