from multiprocessing import Process
from abc import ABCMeta, abstractmethod
from polars import DataFrame
from duckdb import sql

from crowemi_trades.helpers.logging import create_logger


class BaseIndicator(metaclass=ABCMeta):
    def __init__(
        self,
    ) -> None:
        self.LOGGER = create_logger(__name__)
        self.LOGGER.debug("BaseIndicator.__init__: enter.")
        self.LOGGER.debug("BaseIndicator.__init__: exit.")

    @abstractmethod
    def run(self, records: DataFrame, **kwargs):
        raise NotImplementedError("BaseIndicator.run: not implemented.")

    @abstractmethod
    def graph():
        raise NotImplementedError("BaseIndicator.graph: not implemented.")

    @staticmethod
    def indicator_factory(indicator, *args, **kwargs) -> object:
        return indicator(*args, **kwargs)

    # TODO: write unittest
    @staticmethod
    def duplicate_check(dataset: DataFrame, key: str = None) -> bool:
        key = BaseIndicator.default_key(dataset, key)
        # validate dataset contains no duplicates
        duplicates = sql(
            f"""
            SELECT
                COUNT(1) AS n_records,
                {key} AS key
            FROM dataset
            GROUP BY {key}
            HAVING n_records > 1
            """
        ).pl()
        return False if len(duplicates) == 0 else True

    # TODO: write unittest
    @staticmethod
    def default_key(dataset: DataFrame, key: str = None):
        # if key not passed or not in columns, default 'timestamp' or 't'
        if (not key) and (key not in dataset.columns):
            if "timestamp_d" in dataset.columns:
                key = "timestamp_d"
            elif "timestamp_s" in dataset.columns:
                key = "timestamp_s"
            else:
                raise ValueError("No key column found (timestamp_d, timestamp_s).")
        return key
