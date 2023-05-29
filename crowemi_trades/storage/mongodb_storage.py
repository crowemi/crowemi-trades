from datetime import datetime
import polars as pl

import pymongo
from bson.json_util import dumps

from crowemi_trades.storage.base_storage import BaseStorage


class MongoDbStorage(BaseStorage):
    def __init__(self, uri) -> None:
        super().__init__(type="mongodb")
        self.client = pymongo.MongoClient(uri)

    def read(
        self,
        ticker: str,
        interval: str,
        timespan: str,
        start_date: datetime = None,
        end_date: datetime = None,
        results_only: bool = False,
    ) -> pl.DataFrame:
        pass

    def write(self, **kwargs):
        df: pl.DataFrame = kwargs.get("content")
        ticker: str = kwargs.get("ticker")
        timespan: str = kwargs.get("timespan")
        interval: str = kwargs.get("interval")

        collection = f"{ticker}/{timespan}/{interval}"

        records = df.to_dicts()
        d = self.client.get_database("data").get_collection(collection)
        d.insert_many(records)
