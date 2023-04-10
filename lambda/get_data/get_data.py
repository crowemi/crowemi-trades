import boto3
import os

from crowemi_trades.processes.process_get_data import ProcessGetData
from crowemi_trades.storage.s3_storage import S3Storage


def handler(event, context):
    process = ProcessGetData().run(
        storage=S3Storage(session=boto3.Session()),
        bucket=os.getenv("BUCKET", None),
        manifest_key="manifest.json",
    )
    assert process == True


if __name__ == "__main__":
    handler(None, None)
