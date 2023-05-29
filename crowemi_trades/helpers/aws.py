from datetime import datetime, timedelta
from boto3 import client, Session

from crowemi_trades.helpers.logging import create_logger

LOGGER = create_logger(__name__)


def aws_session_factory(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = "us-west-2",
) -> Session:
    return Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )


def aws_client_factory(
    session: Session,
    service: str,
    region_name: str,
    endpoint_override: str,
) -> client:
    return session.client(
        service_name=service,
        region_name=region_name,
        endpoint_url=endpoint_override,
    )


def get_list_objects(
    aws_client: client,
    bucket: str,
    prefix: str,
    limit: int = None,
    start_date: datetime = None,
    end_date: datetime = None,
) -> list:
    """A method to get a listing of objects from S3.

    Args:
        bucket (str): The name of the S3 bucket to query.
        prefix (str): The prefix of the objects to list.
        limit (int, optional): The maximum number of objects to return. Defaults to None, which means no limit.
        start_date (datetime, optional): The earliest date of the objects to list. Defaults to None, which means no filter by date.
        end_date (datetime, optional): The latest date of the objects to list. Defaults to None, which means no filter by date.

    Returns:
        list: A list of dictionaries containing information about the objects, such as key, size, last modified date, etc.

    Raises:
        ClientError: If there is an error in communicating with the S3 service.
    """
    if start_date:
        return get_list_objects_by_date(
            aws_client,
            bucket,
            prefix,
            start_date,
            end_date,
        )
    else:
        return get_list_objects_by_prefix(
            aws_client,
            bucket,
            prefix,
            limit,
        )


def get_list_objects_by_prefix(
    aws_client: client,
    bucket: str,
    prefix: str,
    limit: int = None,
) -> list:
    list_objects = list()
    next_token = None
    while True:
        if next_token:
            ret = aws_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=next_token,
            )
        else:
            ret = aws_client.list_objects_v2(
                bucket,
                prefix,
            )
        if "Contents" in ret:
            list(map(lambda x: list_objects.append(x["Key"]), ret["Contents"]))
            if "NextContinuationToken" in ret:
                next_token = ret["NextContinuationToken"]
                break
            else:
                break
        else:
            LOGGER.warning(f"No objects at path s3://{bucket}/{prefix}.")
            break
    return list_objects


def get_list_objects_by_date(
    aws_client: client,
    bucket: str,
    prefix: str,
    start_date: datetime,
    end_date: datetime = datetime.now(),
    safe: bool = True,
) -> list:
    # C:EURUSD/minute/5/2022/01/20220101.parquet.gzip
    list_objects = list()
    while start_date <= end_date:
        file = f"{prefix}{start_date.year}/{start_date.month:02}/{start_date.strftime('%Y%m%d')}.parquet.gzip"
        if safe:
            try:
                # make sure that objects exists
                if object_exists(
                    aws_client,
                    bucket,
                    file,
                ):
                    list_objects.append(f"{bucket}/{file}")
                else:
                    LOGGER.warning(
                        f"Failed to list objects by date at path s3://{bucket}/{prefix}."
                    )
            except Exception as e:
                LOGGER.exception(e)
        start_date = start_date + timedelta(days=1)
    return list_objects


def object_exists(aws_client: client, bucket: str, prefix: str) -> bool:
    ret = aws_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return True if ret["KeyCount"] > 0 else False


def create_bucket(aws_client: client, bucket: str):
    return aws_client.create_bucket(Bucket=bucket)


def get_buckets(aws_client: client) -> list:
    return list(map(lambda x: x.get("Name"), aws_client.list_buckets()["Buckets"]))
