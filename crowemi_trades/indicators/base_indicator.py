from multiprocessing import Process
from abc import ABCMeta, abstractmethod
from polars import DataFrame

from crowemi_trades.helpers.logging import create_logger


class BaseIndicator(metaclass=ABCMeta):
    def __init__(self,) -> None:
        self.LOGGER = create_logger(__name__)
        self.LOGGER.debug("BaseIndicator.__init__: enter.")
        self.LOGGER.debug("BaseIndicator.__init__: exit.")

    @abstractmethod
    def run(self, records: DataFrame, **kwargs):
        raise NotImplementedError("BaseIndicator.run: not implemented.")

    @abstractmethod
    def apply_indicator(self, record: dict, indicator: dict) -> dict:
        try:
            for v in indicator:
                record[v] = indicator[v]
            return record
        except Exception as e:
            print(e)

    @abstractmethod
    def graph():
        raise NotImplementedError("BaseIndicator.graph: not implemented.")

    @staticmethod
    def indicator_factory(indicator, *args, **kwargs) -> object:
        return indicator(*args, **kwargs)
