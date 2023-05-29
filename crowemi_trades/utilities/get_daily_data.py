import os
import argparse
from datetime import datetime, timedelta
from polars import DataFrame
import polars as pl

from crowemi_trades.helpers.polygon import PolygonHelper
from crowemi_trades.storage.base_storage import BaseStorage

# FIXME: we shouldn't need this ref
from crowemi_trades.storage.s3_storage import (
    S3Storage,
)
from crowemi_trades.indicators.base_indicator import BaseIndicator
from crowemi_trades.indicators.enum import INDICATORS


def get_daily_data(
    ticker: str,
    timespan: str,
    interval: int,
    start_date: datetime,
    end_date: datetime,
    storage: BaseStorage,
):
    """A process function for getting and storing data. \n
    ---
    ticker: The ticket symbol of the asset request. \n
    timespan: The size of the time window. \n
    interval: The size of the timespan multiplier. \n
    start_date: The start window for data collection. \n
    end_date: The end window for the data collection. \n
    bucket: The bucket to store the data. \n
    storage: The cloud storage object used for storing data.
    """
    date_range = list()
    while start_date <= end_date:
        date_range.append(start_date)
        start_date = start_date + timedelta(days=1)

    failures = list()
    for date in date_range:
        ret = PolygonHelper().get_aggregates(
            ticker,
            interval,
            timespan,
            f"{date.year}-{date.month:02}-{date.day:02}",
            f"{date.year}-{date.month:02}-{date.day:02}",
            raw=False,
        )

        start_date = date - timedelta(
            days=3
        )  # NOTE: what if we need more than 30 days? this needs to be dynamic based on indicator
        end_date = date - timedelta(days=1)
        # TODO: this needs to return `results`
        historical_data = storage.read(
            ticker,
            interval,
            timespan,
            start_date,
            end_date,
            True,
        )
        current_data = DataFrame(ret.get("data", None))
        current_data = current_data.with_columns(pl.lit(1).alias("current"))

        if historical_data:
            historical_data = historical_data.with_columns(pl.lit(0).alias("current"))
            current_data = BaseStorage.combine_data(current_data, historical_data)

        for i in INDICATORS:
            current_indicator = BaseIndicator.indicator_factory(INDICATORS[i])
            current_data = current_indicator.run(
                current_data,
            )

        current_data = current_data.filter(pl.col("current") == 1)
        current_data = current_data.drop("current")

        if ret.get("status", None) == 200:
            if not storage.write(
                ticker=ticker,
                timespan=timespan,
                interval=interval,
                date=date,
                content=current_data,
            ):
                # TODO: log wrror
                print(f"Failed write {date.year}-{date.month:02}-{date.day:02}")
        else:
            print(f"Failed processing {date.year}-{date.month:02}-{date.day:02}")
            failures.append(date)

    # TODO: determine how we handle errors?
    return True


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
    parser.add_argument(
        "-sd",
        "--start-date",
        required=True,
    )
    parser.add_argument(
        "-ed",
        "--end-date",
        required=True,
    )
    parser.add_argument(
        "-b",
        "--bucket",
        help="The S3 bucket to store prices",
    )

    args = parser.parse_args()

    # check passed args, then env var
    bucket = args.bucket if args.bucket else os.getenv("BUCKET", None)
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
        storage=S3Storage(bucket),
    )
