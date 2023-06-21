import boto3
import os

from crowemi_trades.processes.process_get_data import ProcessGetData
from crowemi_trades.storage.mongodb_storage import MongoDbStorage
from crowemi_trades.storage.s3_storage import S3Storage


def handler(event, context):
    process = ProcessGetData().run(
        storage=MongoDbStorage(os.getenv("MONGODB_URI", None)),
        manifest={
            "manifest_key": "manifest.json",
            "manifest_storage": S3Storage(
                bucket="crowemi-trades", session=boto3.Session()
            ),
        },
    )
    if process == False:
        raise Exception("ProcessGetData failed.")


if __name__ == "__main__":
    handler(None, None)
