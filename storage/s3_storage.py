from polars import DataFrame
import pyarrow
from pyarrow.parquet import ParquetWriter

from storage.base_storage import BaseStorage

class S3Storage(BaseStorage):
    def __init__(self) -> None:
        super().__init__(type='aws', region='us-west-2')

    def read(self) -> DataFrame:
        pass

    def write(self, key: str, df: DataFrame) -> None:
        try:
            compression = 'gzip' # TODO: support multiple compression types
            table = self.create_data_table(df)
            ParquetWriter(
                f"{key}.parquet.{compression}",
                df.schema,
                compression=compression,
                filesystem=self.file_system,
            ).write_table(df)
            self.file_iterator += 1
        except Exception as e:
            self.logger.error('Failed to write parquet file to S3.')
            raise

    def create_data_table(self, df: DataFrame) -> pyarrow.Table:
        """Creates a pyarrow Table object from the record set."""
        try:
            fields = set()
            for d in df.records:
                fields = fields.union(d.keys())
            dataframe = pyarrow.table({f: [row.get(f) for row in self.records] for f in fields})
        except Exception as e:
            self.logger.error('Failed to create parquet dataframe.')
            self.logger.error(e)
            raise e

        return dataframe