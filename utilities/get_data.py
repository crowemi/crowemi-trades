import json
from datetime import datetime, timedelta
import polars

from helpers.polygon import PolygonHelper
from storage.s3_storage import S3Storage


def get_daily_data(
    ticker: str,
    interval: str,
    interval_period: int,
    start_date: datetime,
    end_date: datetime,
    bucket: str,
):
    # we want to be able to get aggs for a series of dates for any given symbol

    date_range = list()
    while start_date <= end_date:
        date_range.append(start_date)
        start_date = start_date + timedelta(days=1)

    for date in date_range:
        ret = PolygonHelper().get_aggregates(
            ticker,
            interval_period,
            interval,
            f"{date.year}-{date.month:02}-{date.day:02}",
            f"{date.year}-{date.month:02}-{date.day:02}",
            raw=True,
        )
        df = polars.DataFrame(data=json.loads(ret.data))
        file_name = datetime.strptime(start_date, "%Y-%m-%d")
        S3Storage().write(
            f"{bucket}/{ticker}/{interval}/{interval_period}/{file_name.year}{file_name.month:02}{file_name.day:02}",
            df,
        )


if __name__ == "__main__":
    get_daily_data(
        ticker="C:EURUSD",
        interval_period=5,
        interval="minute",
        start_date=datetime(year=2022, month=1, day=1),
        end_date=datetime(year=2023, month=1, day=24),
        bucket="crowemi-trades",
    )
