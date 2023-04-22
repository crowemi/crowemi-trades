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

    def __init__(self) -> None:
        """Instantiate the SMMA Indicator."""
        return super().__init__()

    def run(self, dataset: pl.DataFrame, **kwargs) -> pl.DataFrame:
        period = kwargs.get('period', None)
        assert period, f"smma_indicator.process: Expecting period to be provided. None provided."

        # TODO: attempt to read previous records for calculation
        # generate the period sum
        smma = self.generate_period_sum(dataset, period)

        # check if the current dataset needs a start smma
        if not self.has_start_smma(dataset, period):
            # need to calculate first smma using period average
            # need to cast the row_number Int64
            start_smma = self.generate_period_average(smma, period).with_columns(pl.col('row_number').cast(pl.Int64))
            smma = smma.join(start_smma, on="row_number", how="left")

        smma = self.calculate(smma, period)
        return dataset.join(smma, left_on="ts", right_on="key", how="left")

    def apply_indicator(self, record: dict, indicator: dict) -> dict:
        return super().apply_indicator(record, indicator)

    def graph():
        super().graph()

    @staticmethod
    def calculate(dataset: pl.DataFrame, period: int,) -> pl.DataFrame:
        ret = list()
        dataset = dataset.filter(pl.col("row_number") >= period)
        for i, r in enumerate(dataset.rows()):
            # first element sets SMMAi-1
            if i == 0:
                previous_smma = r[4]
                continue
            SMMAi = (r[3] - previous_smma) / period
            ret.append({ "key": r[1], f"i_smma_{period}": SMMAi})
            previous_smma = SMMAi
        return pl.DataFrame(ret)

    @staticmethod
    def has_start_smma(dataset: pl.DataFrame, period:int) -> bool:
        """ This method checks if the starting SMMMA (SMMAi-1) is present. """
        # ts (timestamp) must be included in the dataset
        assert "ts" in dataset.columns, f"smma_indicator.has_start_smma: Expecting ts in dataset. None provided."
        row = dataset.sort(pl.col("ts")).row(index=period, named=True)._asdict()
        exists = row.get(f"i_smma_{period}", None)
        return True if exists else False

    @staticmethod
    def generate_period_sum(dataset: pl.DataFrame, period: int, **kwargs) -> pl.DataFrame:
        """
        This method takes a dataset and generates a summation over the period.
        """
        try:
            # validate data elements exist in dataset
            key = kwargs.get('key', None)
            value = kwargs.get('value', None)

            if not key and not value:
                # defaults to key=ts (timestamp), value=c (close price)
                key = 'ts'
                value = 'c'
            else:
                assert key, f"smma_indicator.generate_period_sum: No key provided {key}"
                assert value, f"smma_indicator.generate_period_sum: No value provided {value}"

            return duckdb.sql(f"""
                SELECT
                    ROW_NUMBER() OVER(ORDER BY {key}) AS row_number,
                    {key} AS key,
                    {value} AS value,
                    SUM({value}) OVER(ORDER BY {key} ROWS {period} PRECEDING) AS sum
                FROM dataset
                ORDER BY {key}
            """).pl()
        except Exception as e:
            # TODO: add logging
            print(e)

    @staticmethod
    def generate_period_average(dataset: pl.DataFrame, period: int) -> pl.DataFrame:
        """
        Generates the first previous period SMMA (SMMAi-1).
        """
        try:
            return duckdb.sql(f"""
                SELECT
                    {period} AS row_number,
                    AVG(value) AS avg
                FROM dataset
                WHERE row_number <= {period}
            """).pl()
        except Exception as e:
            #TODO: add logging
            print(e)
