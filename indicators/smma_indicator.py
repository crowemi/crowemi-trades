from polars import DataFrame

from indicators.base_indicator import BaseIndicator


class SmmaIndicator(BaseIndicator):
    """
    Calculates the smoothed moving average for the dataset for the given periods.
    ---
    Formula: SMMAi = (Sum - SMMAi-1) / N
    SMMAi - is the value of the period being calculated.
    Sum - is the sum of the source prices of all the periods, over which the indicator is calculated.
    SMMAi-1 - is the value of the period immediately preceding the period being calculated.
    Price - is the source (Close or other) price of any period participating in the calculation.
    N - is the number of periods, over which the indicator is calculated.
    """

    def __init__(self, period: int, df: DataFrame) -> None:
        """Instantiate the SMMA Indicator."""
        super.__init__(self)
        self.df = df
        self.period = period

    def calculate_sum():
        pass
