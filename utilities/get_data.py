import json
from datetime import datetime
import polars
import pandas

from helpers.polygon import PolygonHelper
from storage.s3_storage import S3Storage

POLYGON_HELPER = PolygonHelper()
S3_STORAGE = S3Storage()


def get_daily_data(
        ticker: str, 
        interval: str, 
        interval_period: int, 
        start_date: str, 
        end_date: str,
        bucket: str
    ):
    # we want to be able to get aggs for a series of dates for any given symbol

    # agg = POLYGON_HELPER.client.get_aggs(
    #     ticker,
    #     interval_period,
    #     interval,
    #     start_date,
    #     end_date,
    #     raw=True,
    # )

    # d = get_daily_data(
    #         ticker="C:EURUSD",
    #         interval_period=5,
    #         interval="minute",
    #         start_date="2023-01-03",
    #         end_date="2023-01-03",
    #         bucket="crowemi-trades"
    #     )

    for x in range(10):
        ret = POLYGON_HELPER.get_aggs(
            ticker,
            interval_period,
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
    df = polars.DataFrame(data=json.loads(ret.data))
    file_name = datetime.strptime(start_date, '%Y-%m-%d')
    S3_STORAGE.write(f"{bucket}/{ticker}/{interval}/{interval_period}/{file_name.year}{file_name.month:02}{file_name.day:02}", df)

if __name__ == "__main__":
    get_daily_data(
        ticker="C:EURUSD",
        interval_period=5,
        interval="minute",
        start_date="2023-01-03",
        end_date="2023-01-03",
        bucket="crowemi-trades"
    )