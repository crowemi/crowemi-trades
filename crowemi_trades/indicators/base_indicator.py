from abc import ABCMeta, abstractmethod

from crowemi_trades.helpers.logging import create_logger


INDICATORS = [
    "SessionIndicator",
]


class BaseIndicator(metaclass=ABCMeta):
    def __init__(self) -> None:
        self.LOGGER = create_logger(__name__)
        self.LOGGER.debug("BaseIndicator enter.")
        self.LOGGER.debug("BaseIndicator exit.")

    @abstractmethod
    def apply_indicator(self, record: dict, indicator: dict) -> dict:
        try:
            for v in indicator:
                record[v] = indicator[v]
            return record
        except Exception as e:
            print(e)

    @staticmethod
    def indicator_factory(indicator, *args, **kwargs) -> object:
        if indicator in INDICATORS:
            return indicator(*args, **kwargs)
