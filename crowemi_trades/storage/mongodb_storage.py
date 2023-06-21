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
        database: str = kwargs.get("database", None)
        collection: str = kwargs.get("collection", None)

        try:
            d = self.client.get_database(database).get_collection(collection)
            d.insert_many(records)
            ret = True
        except BulkWriteError as e:
            # we first attempt bulk insert, catch failed duplicate key on unique index and process as replace
            if e.code == 65:
                ret = self.update(records, database=database, collection=collection)
        except Exception as e:
            # TODO: log exception
            print(e)
            ret = False
        finally:
            return ret

    def update(self, records: dict, **kwargs) -> bool:
        ret: bool = True
        try:
            database: str = kwargs.get("database", None)
            collection: str = kwargs.get("collection", None)
            if database and collection:
                [
                    self.client.get_database(database)
                    .get_collection(collection)
                    .replace_one(
                        filter={"_id": rec.get("_id")},
                        replacement=rec,
                    )
                    for rec in records
                ]
                ret = True
            else:
                ret = False
        except Exception as e:
            # TODO: log exception
            ret = False
        finally:
            return ret
