import os
from datetime import datetime

from boto3 import Session
import polars
import pyarrow.parquet as pq
from pyarrow import fs, Table
from smart_open import open

from crowemi_trades.storage.base_storage import BaseStorage
from crowemi_trades.helpers.aws import *


class S3Storage(BaseStorage):
    def __init__(
        self,
        bucket: str,
        access_key: str = None,
        secret_access_key: str = None,
        region: str = None,
        session: Session = None,
        endpoint_override: str = None,
    ) -> None:
        if not session:
            access_key = os.getenv(
                "AWS_ACCESS_KEY_ID",
                access_key,
            )
            secret_access_key = os.getenv(
                "AWS_SECRET_ACCESS_KEY",
                secret_access_key,
            )
            region = os.getenv(
                "AWS_REGION",
                region,
            )
            if access_key and secret_access_key:
                session = aws_session_factory(
                    access_key,
                    secret_access_key,
                    region,
                )
            else:
                raise Exception("Unable to create session, missing AWS credentials.")

        self.aws_client = aws_client_factory(
            session,
            "s3",
            region,
            endpoint_override,
        )
        self.session: Session = session
        self.file_system = self._create_file_system(region, endpoint_override)
        self.bucket = bucket
        super().__init__(type="aws")

    def _create_file_system(self, region: str, endpoint_override: str = None):
        """Creates a pyarrow filesystem object."""
        return fs.S3FileSystem(
            access_key=self.session.get_credentials().access_key,
            secret_key=self.session.get_credentials().secret_key,
            session_token=self.session.get_credentials().token,
            endpoint_override=endpoint_override,
            region=region,
        )

    # TODO: write unittest
    def read(
        self,
        ticker: str,
        interval: str,
        timespan: str,
        start_date: datetime = None,
        end_date: datetime = None,
        results_only: bool = False,
    ) -> polars.DataFrame:
        file_list = get_list_objects(
            self.aws_client,
            self.bucket,
            prefix=self.generate_prefix(ticker, interval, timespan),
            start_date=start_date,
            end_date=end_date,
        )
        ret = self.read_parquet(file_list)
        # this returns the embedded results set only
        if results_only:
            ret = self._get_results_only(ret)
        return ret

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

    def read_parquet(
        self,
        file_list: list,
    ) -> polars.DataFrame:
        """Reads a parquet object from S3."""
        ret = None
        try:
            dataset = pq.ParquetDataset(
                # NOTE: file must contain bucket name
                file_list,
                filesystem=self.file_system,
            )
            ret = polars.from_arrow(dataset.read())
        except Exception as e:
            self.LOGGER.error("Failed to read parquet file to S3.")
            self.LOGGER.exception(e)
            raise e
        return ret

    def write(
        self,
        **kwargs,
    ):
        # TODO: this needs to handle multiple types (single file, parquet, etc.)
        # TODO: key = f"{ticker}/{timespan}/{interval}/{date.year}/{date.month:02}/{date.year}{date.month:02}{date.day:02}"
        """Writes contents to a file in cloud storage."""
        key = kwargs.get("key", None)
        content = kwargs.get("content", None)
        ret: str
        uri = f"s3://{self.bucket}/{key}"
        try:
            with open(uri, "wb", transport_params={"client": self.aws_client}) as f:
                f.write(content)
            return True
        except Exception as e:
            self.LOGGER.error(e)
            return False

    def write_content(self):
        pass

    def write_parquet(
        self,
        key: str,
        df: polars.DataFrame,
    ) -> bool:
        """Writes a parquet object to S3 using pyarrow.

        Parameters
        ----------
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
            table: Table = df.to_arrow()
            pq.ParquetWriter(
                f"{self.bucket}/{key}.parquet.{compression}",
                compression=compression,
                filesystem=self.file_system,
                schema=table.schema,
            ).write_table(table)
            return True
        except Exception as e:
            self.LOGGER.error("Failed to write parquet file to S3.")
            self.LOGGER.exception(e)
            return False

    def _get_results_only(self, df: polars.DataFrame) -> polars.DataFrame:
        """Returns only the results column of a dataframe."""
        ret: list = list()
        # NOTE: need to iterate over rows creating named list
        list(map(lambda x: ret.append(x.results), df.rows(named=True)))
        return polars.DataFrame(ret)

    def generate_key(
        self,
        ticker: str,
        interval: str,
        timespan: str,
    ) -> str:
        return f"{self.bucket}/{self.generate_prefix(ticker, interval, timespan)}"

    @staticmethod
    def generate_prefix(
        ticker: str,
        interval: str,
        timespan: str,
    ) -> str:
        """Generates a prefix for the S3 object.
        :ticker - stock ticker (e.g. C:EURUSD)
        :timespan - minute, hours, etc.
        :interval
        """
        return f"{ticker}/{timespan}/{interval}/"
