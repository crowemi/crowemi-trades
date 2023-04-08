import unittest
import os
import json

import polars as pl

from crowemi_trades.storage.s3_storage import S3Storage


class TestS3Storage(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = S3Storage(
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            endpoint_override="http://localhost:4566",
            region="us-east-1",
        )
        self.bucket = "crowemi-trades"
        self.key = "test_hello_world.json"
        self.content = {"hello": "world"}

        if "crowemi-trades" not in self.stor.get_buckets():
            print("creating bucket crowemi-trades...")
            self.stor.create_bucket("crowemi-trades")

        return super().setUp()

    def test_write_parquet(self):
        df = pl.DataFrame(data=self.content)
        try:
            ret = self.stor.write_parquet(
                "crowemi-trades",
                "test_write",
                df,
            )
        except Exception as e:
            ret = False
            print(e)

        self.assertTrue(ret)

    def test_read_parquet(self):
        self.test_write_parquet()
        df = self.stor.read_parquet(
            self.bucket,
            "test_write.parquet.gzip",
        )
        self.assertFalse(df.is_empty())

    def test_write(self):
        ret = self.stor.write(
            self.bucket,
            self.key,
            bytes(json.dumps(self.content), "utf-8"),
        )
        self.assertTrue(ret)

    def test_read(self):
        content = self.stor.read_content(self.bucket, self.key)
        self.assertTrue(len(content), len(json.dumps(self.content)))


class TestMongoDBStorage(unittest.TestCase):
    pass


if __name__ == "__main__":
    unittest.main()
