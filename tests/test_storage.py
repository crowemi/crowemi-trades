import unittest
import json
import polars as pl

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.helpers.polygon import PolygonHelper


class TestStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = S3Storage("us-west-2")
        self.bucket = "crowemi-trades"
        self.key = "test_hello_world.json"
        self.content = {"hello": "world"}
        return super().setUp()

    def test_write_parquet(self):
        p = PolygonHelper()
        ret = p.get_aggregates(
            "C:EURUSD",
            5,
            "minute",
            "2023-02-07",
            "2023-02-07",
            True,
        )
        df = pl.DataFrame(data=json.loads(ret.data))
        try:
            ret = self.stor.write_parquet(
                "crowemi-trades",
                "test_write",
                df,
            )
            ret = True
        except Exception as e:
            ret = False
            print(e)

        assert ret

    def test_read_parquet(self):
        df = self.stor.read_parquet(
            self.bucket,
            "C:EURUSD/minute/5/2022/01/20220101.parquet.gzip",
        )
        assert not df.is_empty()

    def test_read_all_parquet(self):
        obj = list()
        obj = self.stor.get_list_objects(self.bucket, "C:EURUSD/minute/5/2022/01/")
        df = self.stor.read_all_parquet(self.bucket, obj)
        assert not df.is_empty()

    def test_write(self):
        ret = self.stor.write(
            self.bucket,
            self.key,
            bytes(json.dumps(self.content), "utf-8"),
        )
        assert ret

    def test_read(self):
        content = self.stor.read_content(self.bucket, self.key)
        assert len(content) == len(json.dumps(self.content))
