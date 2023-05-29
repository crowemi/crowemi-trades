import unittest
import os
import json

import polars as pl

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.helpers.aws import get_buckets, create_bucket


class TestS3Storage(unittest.TestCase):
    def setUp(self) -> None:
        self.bucket = "crowemi-trades"
        self.key = "test_hello_world.json"
        self.content = {"hello": "world"}
        self.local_stor = S3Storage(
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            endpoint_override="http://localhost:4566",
            region="us-east-1",
            bucket=self.bucket,
        )

        if "crowemi-trades" not in get_buckets(self.local_stor.aws_client):
            print("creating bucket crowemi-trades...")
            create_bucket(self.local_stor.aws_client, "crowemi-trades")

        return super().setUp()

    def test_write_parquet(
        self,
    ):
        df = pl.DataFrame(data=self.content)
        try:
            ret = self.local_stor.write_parquet(
                "crowemi-trades",
                "test_write",
                df,
            )
        except Exception as e:
            ret = False
            print(e)

        self.assertTrue(ret)

    def test_read_parquet(
        self,
    ):
        self.test_write_parquet()
        df = self.local_stor.read_parquet(
            "crowemi-trades/test_write.parquet.gzip",
        )
        self.assertFalse(df.is_empty())

    def test_write(
        self,
    ):
        ret = self.local_stor.write(
            key=self.key,
            content=bytes(json.dumps(self.content), "utf-8"),
        )
        self.assertTrue(ret)

    def test_read(
        self,
    ):
        self.test_write()
        content = self.local_stor.read_content(self.bucket, self.key)
        self.assertTrue(len(content), len(json.dumps(self.content)))

    def test_generate_prefix(
        self,
    ):
        prefix = self.local_stor.generate_prefix("ticker", "interval", "timespan")
        self.assertEqual(prefix, "ticker/timespan/interval/")

    def test_generate_key(
        self,
    ):
        ticket = "ticket"
        interval = "interval"
        timespan = "timespan"
        key = self.local_stor.generate_key(
            ticket,
            interval,
            timespan,
        )
        self.assertEqual(key, f"{self.bucket}/{ticket}/{timespan}/{interval}/")

    def test_get_data(
        self,
    ):
        pass


class TestMongoDBStorage(unittest.TestCase):
    pass


if __name__ == "__main__":
    unittest.main()
