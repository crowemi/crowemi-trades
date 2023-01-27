import os
import argparse
import json
from datetime import datetime, timedelta
import polars

from helpers.polygon import PolygonHelper
from storage.s3_storage import S3Storage


def get_daily_data(
    ticker: str,
    timespan: str,
    interval: int,
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
            interval,
            timespan,
            f"{date.year}-{date.month:02}-{date.day:02}",
            f"{date.year}-{date.month:02}-{date.day:02}",
            raw=True,
        )
        if ret.status == 200:
            df = polars.DataFrame(data=json.loads(ret.data))
            S3Storage().write(
                f"{bucket}/{ticker}/{timespan}/{interval}/{date.year}{date.month:02}{date.day:02}",
                df,
            )
        else:
            print(f"Failed processing {date.year}-{date.month:02}-{date.day:02}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="Get Daily Data",
        description="This program calls Polygon API to collect and store stock prices for given parameters.",
    )

    parser.add_argument(
        "-t", "--ticker", help="The ticker symbol of the stock/equity.", required=True
    )
    parser.add_argument(
        "-i", "--interval", help="The size of the timespan multiplier.", required=True
    )
    parser.add_argument(
        "-ts",
        "--timespan",
        help="The size of the time window.",
        choices=["minute", "hour", "day", "week", "month", "quarter", "year"],
        required=True,
    )
    parser.add_argument("-sd", "--start-date", required=True)
    parser.add_argument("-ed", "--end-date", required=True)
    parser.add_argument("-b", "--bucket", help="The S3 bucket to store prices")

    args = parser.parse_args()

    # check passed args, then env var
    bucket = args.bucket if args.bucket else os.getenv("bucket", None)
    if not bucket:
        raise Exception("No bucket supplied.")

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    get_daily_data(
        ticker=args.ticker,
        interval=args.interval,
        timespan=args.timespan,
        start_date=start_date,
        end_date=end_date,
        bucket=bucket,
    )