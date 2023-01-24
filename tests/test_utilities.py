import unittest


from utilities.get_data import *


class TestUtilities(unittest.TestCase):
    def test_get_data(self):
        d = get_daily_data(
            ticker="C:EURUSD",
            interval_period=5,
            interval="minute",
            start_date="2023-01-03",
            end_date="2023-01-03",
            bucket="crowemi-trades"
        )

        assert True



if __name__ == "__main__":
    unittest.main()