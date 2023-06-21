import duckdb
import polars as pl

from crowemi_trades.indicators.base_indicator import BaseIndicator


class SmmaIndicator(BaseIndicator):
    """
    Calculates the smoothed moving average for the dataset for the given periods.
    ---
    Formula: SMMAi = (Sum - SMMAi-1) / N
    SMMAi - is the value of the period being calculated.
    Sum - is the sum of the source prices (close) of all the periods, over which the indicator is calculated.
    SMMAi-1 - is the value of the period immediately preceding the period being calculated.
    Price - is the source (Close or other) price of any period participating in the calculation.
    N - is the number of periods, over which the indicator is calculated.
    ---
    The first SMMA period is calculated using a simple average over the period
    """

    DEFAULT_PERIODS = [20, 50, 200]

    def __init__(self) -> None:
        """Instantiate the SMMA Indicator."""
        return super().__init__()

    def run(self, dataset: pl.DataFrame, **kwargs) -> pl.DataFrame:
        key = kwargs.get("key", None)
        # Price - is the source (Close or other) price of any period participating in the calculation.
        price = kwargs.get("price", "close")
        period = kwargs.get("period", None)
        periods = [period] if period else self.DEFAULT_PERIODS

        # duplicate check, for now raise error
        if self.duplicate_check(dataset):
            raise ValueError("Duplicate rows found in dataset")

        for period in periods:
            # generate the period sum
            key = BaseIndicator.default_key(dataset, key)
            smma = self.generate_period_sum(dataset, period, key=key, price=price)
            smma = self.calculate(smma, period)
            if not smma.is_empty():
                dataset = dataset.join(smma, left_on=key, right_on="key", how="left")
        return dataset

    def apply_indicator(self, record: dict, indicator: dict) -> dict:
        return super().apply_indicator(record, indicator)

    def graph():
        super().graph()

    @staticmethod
    def calculate(
        dataset: pl.DataFrame,
        period: int,
    ) -> pl.DataFrame:
        ret = list()
        dataset = dataset.filter(pl.col("row_number") >= period)
        for i, r in enumerate(dataset.rows()):
            # The first SMMA period is calculated using a simple average over the period
            if i == 0:
                previous_smma = r[3] / period
                continue
            SMMAi = (r[3] - previous_smma) / period
            ret.append({"key": r[1], f"i_smma_{period}": SMMAi})
            previous_smma = SMMAi
        return pl.DataFrame(ret)

    @staticmethod
    def generate_period_sum(
        dataset: pl.DataFrame,
        period: int,
        **kwargs,
    ) -> pl.DataFrame:
        """
        This method takes a dataset and generates a summation over the period.
        """
        # NOTE: this should be a more generic method, not just for SMMA indicator
        try:
            # validate data elements exist in dataset
            # defaults to default_key
            key = kwargs.get("key", BaseIndicator.default_key(dataset, None))
            # defaults to close (NOTE: not too hott on default to close here)
            value = kwargs.get("price", "close")

            return duckdb.sql(
                f"""
                SELECT
                    ROW_NUMBER() OVER(ORDER BY {key}) AS row_number,
                    {key} AS key,
                    {value} AS value,
                    SUM({value}) OVER(ORDER BY {key} ROWS {period} PRECEDING) AS sum
                FROM dataset
                ORDER BY {key}
            """
            ).pl()
        except Exception as e:
            # TODO: add logging
            print(e)
            raise e
