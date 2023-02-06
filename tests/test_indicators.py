import unittest


from crowemi_trades.indicators.smma_indicator import SmmaIndicator


class TestIndicator(unittest.TestCase):
    def test_smma_indicator(
        self,
    ):

        smma = SmmaIndicator(period=5)
        smma.calculate_sum()
