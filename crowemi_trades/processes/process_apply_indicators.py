from crowemi_trades.processes.process_core import ProcessCore

from crowemi_trades.indicators.enum import INDICATORS
from crowemi_trades.indicators.base_indicator import BaseIndicator


class ProcessApplyIndicators(ProcessCore):
    def __init__(self) -> None:
        super().__init__()

    def run(self, indicator: str):
        if indicator:
            indicator = BaseIndicator.indicator_factory(INDICATORS[indicator])
            indicator.run()
