import polars as pl

from pyarrow.parquet import ParquetWriter

from crowemi_trades.storage.base_storage import BaseStorage


class S3Storage(BaseStorage):
    def __init__(self) -> None:
        super().__init__(type="aws", region="us-west-2")

    def read(self, key: str) -> pl.DataFrame:
        pass

    def write(self, key: str, df: pl.DataFrame) -> None:
        try:
            compression = "gzip"  # TODO: support multiple compression types
            data, schema = self.create_data_table(df)
            ParquetWriter(
                f"{key}.parquet.{compression}",
                schema,
                compression=compression,
                filesystem=self.file_system,
            ).write_table(data)
        except Exception as e:
            self.LOGGER.error("Failed to write parquet file to S3.")
            self.LOGGER.exception(e)
            raise
