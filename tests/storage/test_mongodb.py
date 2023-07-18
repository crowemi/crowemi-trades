import os
import unittest
from datetime import datetime

import polars as pl

from tests.test_common import configure_mongodb
from crowemi_trades.storage.mongodb_storage import MongoDbStorage


class TestMongoDb(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = configure_mongodb()
        return super().setUp()

    def test_read(self):
        ret, res = self.stor.read(
            database="data",
            collection="C:EURUSD/minute/5",
        )
        self.assertTrue(ret)
        self.assertTrue(len(res) > 0)
