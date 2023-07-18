import boto3
import os

from crowemi_trades.processes.process_get_data import ProcessGetData
from crowemi_trades.storage.mongodb_storage import MongoDbStorage
from crowemi_trades.storage.s3_storage import S3Storage


def handler(event, context):
    mongo_storage = MongoDbStorage(os.getenv("MONGODB_URI", None))
    process = ProcessGetData().run(
        storage=mongo_storage,
        manifest={
            "manifest_storage": mongo_storage,
        },
    )
    if process == False:
        raise Exception("ProcessGetData failed.")


if __name__ == "__main__":
    handler(None, None)
