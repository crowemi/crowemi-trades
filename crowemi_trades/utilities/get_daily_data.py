import os
import argparse
from datetime import datetime, timedelta
from polars import DataFrame

from crowemi_trades.helpers.polygon import PolygonHelper
from crowemi_trades.storage.base_storage import BaseStorage
from crowemi_trades.storage.s3_storage import S3Storage # FIXME: we shouldn't need this ref
from crowemi_trades.indicators.base_indicator import BaseIndicator
from crowemi_trades.indicators.enum import INDICATORS


def get_daily_data(
    ticker: str,
    timespan: str,
    interval: int,
    start_date: datetime,
    end_date: datetime,
    bucket: str, # FIXME: remove this, this should be set in the storage object
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
            raw=True,
        )

        # TODO: implement historical data for indicators
        historical_data = storage.get_data(datetime(2023, 3, 1), datetime(2023, 4, 18))
        list(map(lambda x: x.update({"current": 0}), historical_data))

        # apply indicators -- threadpool
        # we should be able to process multiple datasets against multiple indicators
        results = ret.get("data", None).get("results", None)
        list(map(lambda x: x.update({"current": 1}), results))
        for i in INDICATORS:
            current_indicator = BaseIndicator.indicator_factory(INDICATORS[i])
            current_indicator.run(results)

        success_keys = list()
        if ret.get("status", None) == 200:
            # FIXME: this should be set in the storage object
            df = DataFrame(
                data=ret.get("data", None),
            )
            key = f"{ticker}/{timespan}/{interval}/{date.year}/{date.month:02}/{date.year}{date.month:02}{date.day:02}"
            stor_ret = storage.write_parquet(
                bucket,
                key,
                df,
            )
            if stor_ret:
                success_keys.append(key)
        else:
            print(f"Failed processing {date.year}-{date.month:02}-{date.day:02}")
            failures.append(date)

    return True if len(failures) == 0 else False, success_keys


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
        bucket=bucket,
        storage=S3Storage(),
    )
