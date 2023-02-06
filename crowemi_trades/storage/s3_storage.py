import os
from datetime import datetime
from polars import DataFrame, from_arrow, concat
from concurrent.futures import ThreadPoolExecutor
import pyarrow.parquet as pq
from pyarrow import fs

from crowemi_trades.storage.base_storage import BaseStorage
from crowemi_trades.helpers.core import debug
from crowemi_helps.aws.aws_s3 import AwsS3


class S3Storage(BaseStorage):
    def __init__(
        self,
        access_key: str = None,
        secret_access_key: str = None,
        region: str = None,
    ) -> None:
        self.access_key = os.getenv("AWS_ACCESS_KEY_ID", access_key)
        self.secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", secret_access_key)
        self.region = os.getenv("AWS_REGION", "us-west-2")

        super().__init__(type="aws", region="us-west-2")

        self.aws_client = AwsS3(region)
        self._create_file_system()

    def get_list_objects(
        self,
        bucket: str,
        prefix: str,
        limit: int = None,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> list:
        """A method to get a listing of objects from S3."""
        list_objects = list()
        next_token = None
        bucket = bucket if bucket else self.bucket
        while True:
            ret = self.aws_client.list_objects(
                prefix=prefix,
                bucket=bucket,
                next_token=next_token,
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

    def _create_file_system(self):
        """Creates a pyarrow filesystem object."""
        self.file_system = fs.S3FileSystem(
            access_key=self.access_key,
            secret_key=self.secret_access_key,
        )

    def _combine_data(
        self,
        primary: DataFrame,
        incoming: DataFrame,
    ) -> DataFrame:
        """Combines raw dataframes into single dataframe."""
        self.LOGGER.debug("Enter _combine_data.")
        try:
            if primary.is_empty():
                primary = incoming
            else:
                primary = concat([primary, incoming], how="vertical")
        except Exception as e:
            self.LOGGER.error("Unable to concat dataframes")
            self.LOGGER.exception(e)
        self.LOGGER.debug("Exit _combine_data.")
        return primary

    def read_all(
        self,
        bucket: str,
        file_list: list,
    ) -> DataFrame:
        """Reads multiple files from storage into a single dataframe.\n
        ---
        bucket: \n
        file_list: \n
        """
        primary = list[DataFrame()]
        file_list = list(map(lambda x: {"key": x["Key"], "bucket": bucket}, file_list))
        with ThreadPoolExecutor(5) as tp:
            primary = list(tp.map(self._read_from_list_object, file_list))

        if len(primary) > 0:
            ret = DataFrame()
            for df in primary:
                ret = self._combine_data(ret, df)
        return ret

    def _read_from_list_object(self, keys: list) -> DataFrame:
        return self.read(keys["bucket"], keys["key"])

    def read(
        self,
        bucket: str,
        key: str,
    ) -> DataFrame:
        """Reads a parquet object from S3."""
        ret = None

        if not self.access_key:
            raise Exception("No AWS Access Key supplied.")
        if not self.secret_access_key:
            raise Exception("No AWS Secret Access Key supplied.")

        try:
            dataset = pq.ParquetDataset(f"{bucket}/{key}", filesystem=self.file_system)
            ret = from_arrow(dataset.read())
        except Exception as e:
            self.LOGGER.error("Failed to read parquet file to S3.")
            self.LOGGER.exception(e)
        return ret

    def write(self, key: str, df: DataFrame) -> None:
        """Writes a parquet object to S3."""
        try:
            compression = "gzip"  # TODO: support multiple compression types
            data, schema = self.create_data_table(df)
            pq.ParquetWriter(
                f"{key}.parquet.{compression}",
                schema,
                compression=compression,
                filesystem=self.file_system,
            ).write_table(data)
        except Exception as e:
            self.LOGGER.error("Failed to write parquet file to S3.")
            self.LOGGER.exception(e)
            raise
