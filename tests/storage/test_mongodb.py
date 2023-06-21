import os
import unittest

import polars as pl

from crowemi_trades.storage.mongodb_storage import MongoDbStorage


class TestMongoDb(unittest.TestCase):
    def setUp(self) -> None:
        uri = os.getenv("MONGODB_URI")
        self.stor = MongoDbStorage(uri)
        return super().setUp()
