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
        **kwargs,
    ):
        ret: bool = True
        res = list()

        database, collection = self.get_details(kwargs)
        predicate = kwargs.get("predicate", {})
        limit = kwargs.get("limit", None)
        # default sort timestamp descending
        sort = kwargs.get("sort", [("timestamp", pymongo.DESCENDING)])

        try:
            c = self.client.get_database(database).get_collection(collection)
            if limit:
                [
                    res.append(record)
                    for record in c.find(
                        predicate,
                        sort=sort,
                    ).limit(limit)
                ]
            else:
                [
                    res.append(record)
                    for record in c.find(
                        predicate,
                        sort=sort,
                    )
                ]
        except Exception as e:
            # TODO: log exception
            print(e)
            ret = False
        finally:
            return ret, res

    def write(self, records: dict = None, **kwargs) -> bool:
        ret: bool = True
        database, collection = self.get_details(kwargs)

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

    def get_details(self, kwargs):
        database: str = kwargs.get("database", None)
        collection: str = kwargs.get("collection", None)
        return database, collection

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
