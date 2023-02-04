import unittest
import polars as pl

from crowemi_helps.aws.aws_s3 import AwsS3
from crowemi_trades.storage.s3_storage import S3Storage


class TestStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = S3Storage()
        self.s3 = AwsS3("us-west-2")
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
        next_token = None
        while True:
            ret = self.s3.list_objects(
                prefix="C:EURUSD/minute/5/2023",
                bucket=self.bucket,
                next_token=next_token,
            )
            list(map(lambda x: obj.append(x), ret["Contents"]))
            if "NextContinuationToken" in ret:
                next_token = ret["NextContinuationToken"]
                break
            else:
                break
        df = self.stor.read_all(self.bucket, obj)
        assert not df.is_empty()
