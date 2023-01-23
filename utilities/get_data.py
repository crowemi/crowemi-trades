import json
import polars
import pandas

from helpers.polygon_helper import PolygonHelper
from storage.s3_storage import S3Storage

POLYGON_HELPER = PolygonHelper()
S3_STORAGE = S3Storage()


def get_daily_data(ticker: str, interval: str, interval_period: int, start_date: str, end_date: str):
    # we want to be able to get aggs for a series of dates for any given symbol
    # we want te be able to enrich the aggs with other indicators

    agg = POLYGON_HELPER.client.get_aggs(
        ticker,
        interval_period, # five minute
        interval,
        start_date,
        end_date,
        raw=True,
    )
        # "c": close
        # "h": high
        # "l": low
        # "n": number of transactions,
        # "o": open
        # "t": unix ms timestamp,
        # "v": trading volume,
        # "vw": volume weighted average price
    df = polars.DataFrame(data=json.loads(agg.data))
    S3_STORAGE.write("crowemi-trades/EURUSD/5/20230102", df)

    print(agg.data)
