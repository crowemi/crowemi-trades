import json
from datetime import datetime
import polars
import pandas

from helpers.polygon_helper import PolygonHelper
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

    for x in range(10):
        POLYGON_HELPER.get_aggs()
        # "c": close
        # "h": high
        # "l": low
        # "n": number of transactions,
        # "o": open
        # "t": unix ms timestamp,
        # "v": trading volume,
        # "vw": volume weighted average price
    df = polars.DataFrame(data=json.loads(agg.data))
    file_name = datetime.strptime(start_date, '%Y-%m-%d')
    S3_STORAGE.write(f"{bucket}/{ticker}/{interval}/{interval_period}/{file_name.year}{file_name.month:02}{file_name.day:02}", df)

    print(agg.data)
