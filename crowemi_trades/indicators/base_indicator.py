from crowemi_trades.helpers.logging import *

from abc import ABCMeta, abstractmethod


class BaseIndicator(metaclass=ABCMeta):
    def __init__(self) -> None:
        self.LOGGER = create_logger(__name__)
        self.LOGGER.debug("BaseIndicator enter.")
        self.LOGGER.debug("BaseIndicator exit.")

    @abstractmethod
    def add_record(self):
        pass
