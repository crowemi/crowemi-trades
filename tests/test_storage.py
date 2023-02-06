import unittest
import polars as pl

from crowemi_trades.storage.s3_storage import S3Storage


class TestStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = S3Storage("us-west-2")
        self.bucket = "crowemi-trades"
        return super().setUp()

    def test_read(self):
        df = self.stor.read(
            self.bucket,
            "C:EURUSD/minute/5/20220101.parquet.gzip",
        )
        assert not df.is_empty()

    def test_read_multiple(self):
        obj = list()
        obj = self.stor.get_list_objects(self.bucket, "C:EURUSD/minute/5/2023")
        df = self.stor.read_all(self.bucket, obj)
        assert not df.is_empty()
