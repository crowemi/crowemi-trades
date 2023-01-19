import pandas

from indicators.base import BaseIndicator

class SmmaIndicator(BaseIndicator):
    '''Calculates the smoothed moving average for the dataset for the given periods.'''
    def __init__(self, df: pandas.DataFrame) -> None:
        super.__init__(self)
        