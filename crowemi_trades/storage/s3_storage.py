import os
import boto3
from datetime import datetime
import polars
from concurrent.futures import ThreadPoolExecutor
import pyarrow.parquet as pq

from crowemi_trades.storage.base_storage import BaseStorage


class S3Storage(BaseStorage):
    def __init__(
        self,
        access_key: str = None,
        secret_access_key: str = None,
        region: str = None,
        session: boto3.Session = None,
    ) -> None:
        self.access_key = os.getenv("AWS_ACCESS_KEY_ID", access_key)
        self.secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", secret_access_key)
        self.region = os.getenv("AWS_REGION", region)

        super().__init__(type="aws")

        if session:
            self.aws_client = session.client("s3")
            sts = session.client("sts")
            caller = sts.get_caller_identity()
            self.LOGGER.debug(caller)
        else:
            self.aws_client = boto3.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_access_key,
                region_name=self.region,
            ).client("s3")

    def read(
        self,
    ):
        pass

    def read_content(
        self,
        bucket: str,
        key: str,
    ) -> str:
        return super().read_content(bucket, key)

    def write(
        self,
        bucket: str,
        key: str,
        contents: bytes,
    ):
        return super().write(bucket, key, contents)

    def get_list_objects(
        self,
        bucket: str,
        prefix: str,
        limit: int = None,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> list:
        """A method to get a listing of objects from S3. \n
        ---
        bucket: \n
        prefix: \n
        limit: \n
        start_date: \n
        end_date: \n
        """
        list_objects = list()
        next_token = None
        bucket = bucket if bucket else self.bucket
        while True:
            if next_token:
                ret = self.aws_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                    ContinuationToken=next_token,
                )
            else:
                ret = self.aws_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=prefix,
                )
            if "Contents" in ret:
                list(map(lambda x: list_objects.append(x), ret["Contents"]))
                if "NextContinuationToken" in ret:
                    next_token = ret["NextContinuationToken"]
                    break
                else:
                    break
            else:
                self.LOGGER.warning(f"No objects at path s3://{bucket}/{prefix}.")
                break
        return list_objects

    def read_all_parquet(
        self,
        bucket: str,
        file_list: list,
    ) -> polars.DataFrame:
        """Reads multiple files from storage into a single dataframe.\n
        ---
        bucket: the S3 bucket containing the files. \n
        file_list: the file list to read. \n
        """
        primary = list[polars.DataFrame()]
        file_list = list(map(lambda x: {"key": x["Key"], "bucket": bucket}, file_list))
        with ThreadPoolExecutor(5) as tp:
            primary = list(tp.map(self._read_from_list_object, file_list))

        if len(primary) > 0:
            ret = polars.DataFrame()
            for df in primary:
                ret = self._combine_data(ret, df)
        return ret

    def _read_from_list_object(
        self,
        keys: list,
    ) -> polars.DataFrame:
        return self.read_parquet(keys["bucket"], keys["key"])

    def read_parquet(
        self,
        bucket: str,
        key: str,
    ) -> polars.DataFrame:
        """Reads a parquet object from S3."""
        ret = None

        if not self.access_key:
            raise Exception("No AWS Access Key supplied.")
        if not self.secret_access_key:
            raise Exception("No AWS Secret Access Key supplied.")

        try:
            dataset = pq.ParquetDataset(f"{bucket}/{key}", filesystem=self.file_system)
            ret = polars.from_arrow(dataset.read())
        except Exception as e:
            self.LOGGER.error("Failed to read parquet file to S3.")
            self.LOGGER.exception(e)
        return ret

    def write_parquet(
        self,
        bucket: str,
        key: str,
        df: polars.DataFrame,
    ) -> bool:
        """Writes a parquet object to S3.
        ---
        key: S3 key {bucket}/{key}; exclude extension. \n
        df: Polars DataFrame containing contents to write
        """
        try:
            # TODO: check key for extension, or add extension param
            compression = "gzip"  # TODO: support multiple compression types
            data, schema = self.create_data_table(df)
            pq.ParquetWriter(
                f"{bucket}/{key}.parquet.{compression}",
                schema,
                compression=compression,
                filesystem=self.file_system,
            ).write_table(data)
            return True
        except Exception as e:
            self.LOGGER.error("Failed to write parquet file to S3.")
            self.LOGGER.exception(e)
            return False
