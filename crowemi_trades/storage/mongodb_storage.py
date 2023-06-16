from datetime import datetime
import polars as pl

import pymongo
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId
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

    def write(self, records: dict = None, **kwargs) -> bool:
        ret: bool = True
        # TODO: we want to accept these params to derive the collection OR collection param itself
        ticker: str = kwargs.get("ticker")
        timespan: str = kwargs.get("timespan")
        interval: str = kwargs.get("interval")
        collection = f"{ticker}/{timespan}/{interval}"

        try:
            d = self.client.get_database("data").get_collection(collection)
            d.insert_many(records)
            return True
        except BulkWriteError as e:
            # we first attempt bulk insert, catch failed duplicate key on unique index and process as replace
            if e.code == 65:
                self.update(records, collection=collection)
                print(e)
        except Exception as e:
            print(e)
        finally:
            return ret

    def update(self, records: dict, **kwargs) -> bool:
        collection: str = kwargs.get("collection")
        try:
            for rec in records:
                self.client.get_database("data").get_collection(collection).replace_one(
                    filter={"_id": rec.get("_id")}, replacement=rec
                )

        except Exception as e:
            print(e)
