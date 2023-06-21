import os
import unittest
from datetime import datetime, timedelta

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.storage.mongodb_storage import MongoDbStorage
from crowemi_trades.utilities.get_daily_data import get_daily_data


class TestUtilities(unittest.TestCase):
    def setUp(self) -> None:
        uri = os.getenv("MONGODB_URI")
        self.stor = MongoDbStorage(uri)
        return super().setUp()

    def test_get_daily_data(self):
        today = datetime(year=2023, month=4, day=3) + timedelta(-1)
        # "ticket": "C:EURUSD", "interval": "5", "timespan": "minute", "bucket": "crowemi-trades", "last_modified": "20230404"
        ret = get_daily_data(
            ticker="C:EURUSD",
            interval=5,
            timespan="minute",
            start_date=datetime(today.year, today.month, today.day),
            end_date=datetime(today.year, 4, 4),
            storage=self.stor,
        )
        self.assertEqual(ret, True)


if __name__ == "__main__":
    unittest.main()
