import polars as pl
from storage.base_storage import BaseStorage

class S3Storage(BaseStorage):
    def __init__(self) -> None:
        super().__init__()

    def read():
        pass

    def write(bucket: str, key: str, df):
        pass