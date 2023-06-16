import os
import unittest
from datetime import datetime, timedelta

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.storage.mongodb_storage import MongoDbStorage
from crowemi_trades.utilities.get_daily_data import get_daily_data


class TestUtilities(unittest.TestCase):
    def setUp(self) -> None:
        access_key = os.getenv("AWS_ACCESS_KEY_ID", None)
        secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", None)
        endpoint_override = (
            None if (access_key or secret_access_key) else "http://localhost:4566"
        )
        bucket = os.getenv("BUCKET", "crowemi-trades")
        region = "us-west-2" if not endpoint_override else "us-east-1"
        access_key = access_key if access_key else "test"
        secret_access_key = secret_access_key if secret_access_key else "test"
        # self.stor = S3Storage(
        #     access_key=access_key,
        #     secret_access_key=secret_access_key,
        #     endpoint_override=endpoint_override,
        #     bucket=bucket,
        #     region=region,
        # )
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
            end_date=datetime(today.year, 4, 5),
            storage=self.stor,
        )
        self.assertEqual(ret, True)


if __name__ == "__main__":
    unittest.main()
