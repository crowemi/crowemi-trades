import unittest
import polars as pl

from tests.test_common import configure_mongodb
from crowemi_trades.processes.process_apply_indicators import ProcessApplyIndicators


class TestProcessApplyIndicators(unittest.TestCase):
    def setUp(self) -> None:
        self.process = ProcessApplyIndicators()
        self.stor = configure_mongodb()
        return super().setUp()

    def test_run(self):
        records = self.stor.read(database="data", collection="C:EURUSD/minute/5")
        df = pl.DataFrame(records[1])
        self.process.run("SessionIndicator")


if __name__ == "__main__":
    unittest.main()
