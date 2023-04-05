import unittest

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.utilities.get_daily_data import get_daily_data


class TestUtilities(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = S3Storage(access_key="test", secret_access_key="test")
        return super().setUp()

    def test_get_daily_data(self):
        pass
