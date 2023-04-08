import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from boto3 import Session
import polars
import pyarrow.parquet as pq
from smart_open import open

from crowemi_trades.storage.base_storage import BaseStorage


class S3Storage(BaseStorage):
    def __init__(
        self,
        access_key: str = None,
        secret_access_key: str = None,
        region: str = None,
        session: Session = None,
        endpoint_override: str = None,
    ) -> None:
        self.endpoint_override = endpoint_override
        if session:
            self.aws_client = session.client(
                "s3",
                endpoint_url=self.endpoint_override,
            )
        else:
            session = Session(
                aws_access_key_id=os.getenv(
                    "AWS_ACCESS_KEY_ID",
                    access_key,
                ),
                aws_secret_access_key=os.getenv(
                    "AWS_SECRET_ACCESS_KEY",
                    secret_access_key,
                ),
                region_name=os.getenv(
                    "AWS_REGION",
                    region,
                ),
            )
            self.aws_client = session.client(
                "s3",
                endpoint_url=self.endpoint_override,
            )

        self.session: Session = session
        super().__init__(type="aws")

    def read(
        self,
    ):
        pass

    def read_content(
        self,
        bucket: str,
        key: str,
    ) -> str:
        """Reads the contents of a file from cloud storage.
        ---
        bucket: \n
        key:
        """
        ret: str
        uri: str
        if self.type == "aws":
            uri = f"s3://{bucket}/{key}"
        with open(uri, transport_params={"client": self.aws_client}) as f:
            ret = f.read()
        return ret

    def write(
        self,
        bucket: str,
        key: str,
        contents: bytes,
    ):
        """Writes contents to a file in cloud storage."""
        ret: str
        uri: str
        if self.type == "aws":
            uri = f"s3://{bucket}/{key}"
        try:
            with open(uri, "wb", transport_params={"client": self.aws_client}) as f:
                f.write(contents)
            return True
        except Exception as e:
            self.LOGGER.error(e)
            return False

    def get_list_objects(
        self,
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
        """Reads multiple files from storage into a single dataframe.

        Args:
            bucket (str): The name of the S3 bucket containing the files.
            file_list (list): The list of dictionaries containing information about the files, such as key, size, etc.

        Returns:
            polars.DataFrame: A dataframe containing the data from all the files.

        Raises:
            ClientError: If there is an error in communicating with the S3 service.
            ValueError: If the file list is empty or contains invalid files.
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
        """Writes a parquet object to S3 using pyarrow.

        Parameters
        ----------
        bucket : str
            The name of the S3 bucket where the parquet object will be stored.
        key : str
            The S3 key for the parquet object, excluding the extension. The extension will be added based on the compression type.
        df : polars.DataFrame
            The polars DataFrame containing the data to be written as a parquet object.

        Returns
        -------
        bool
            True if the write operation was successful, False otherwise.

        Raises
        ------
        Exception
            If any error occurs during the write operation.

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

    def create_bucket(self, bucket: str):
        return self.aws_client.create_bucket(Bucket=bucket)

    def get_buckets(self) -> list:
        return list(
            map(lambda x: x.get("Name"), self.aws_client.list_buckets()["Buckets"])
        )
