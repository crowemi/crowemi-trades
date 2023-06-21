import os
import json
import time
from datetime import datetime
import functools

from polygon import RESTClient
from polygon.rest.aggs import Agg


class PolygonHelper:
    """A helper class to abstract common polygon functionality."""

    def __init__(self, polygon_token: str = None) -> None:
        # if token not passed, is it in env
        polygon_token = (
            polygon_token if polygon_token is not None else os.getenv("POLYGON_KEY")
        )
        if not polygon_token:
            raise Exception("No polgygon token found.")
        self._client = RESTClient(api_key=polygon_token)
        self.requests = 0

    def throttle(func):
        """Decorator to throttle requests to Polygon."""

        @functools.wraps(func)
        def wrapper(*args, **kwrags):
            wrapper.time.append(time.time())
            if len(wrapper.time) == 5:
                while (time.time() - wrapper.time[0]) < 60:
                    time.sleep(5)
                wrapper.time.pop(0)
            return func(*args, **kwrags)

        wrapper.time = list()
        return wrapper

    @throttle
    def get_aggregates(
        self,
        ticker: str,
        interval: int,
        timespan: str,
        start_date: str,
        end_date: str,
        raw: bool,
    ) -> list:
        """Polygon get_aggs wrapper
        ---
        "c": close
        "h": high
        "l": low
        "n": number of transactions,
        "o": open
        "t": unix ms timestamp,
        "v": trading volume,
        "vw": volume weighted average price
        """
        data = []
        try:
            ret = self._client.get_aggs(
                ticker,
                interval,
                timespan,
                start_date,
                end_date,
                raw=raw,
            )
            # NOTE: Forex Market is open From 5:00pm ET Sunday through 5:00pm ET on Friday
            # TODO: Add data check for period e.g. 288 periods/day for a 5-min period
            if raw:
                data = json.loads(ret.data)
                if data.get("queryCount", 0) > 0 and data.get("resultsCount") > 0:
                    res = map(lambda x: self.to_list(x), data.get("results"))
                    data["results"] = self.apply_timestamp(data.get("results"))
                else:
                    # TODO: log warning
                    print("No results found")
            else:
                # converts polygon aggregate objects to list of dicts, this is needed to add additional properites.
                res = list(map(lambda x: self.to_dict(x), ret))
                data = self.apply_timestamp(res)

        except Exception as e:
            # TODO: log exception
            print(e)
        return {"status": 200, "data": data}

    # TODO: write unittest
    def to_dict(
        self,
        record: Agg,
    ) -> dict:
        return {i: v for i, v in record.__dict__.items()}

    # TODO: write unittest
    def apply_timestamp(self, records) -> list:
        ret = list()

        def append_timestamp(record):
            try:
                ts = (
                    record.get("timestamp", None)
                    if record.get("timestamp", None)
                    else record.get("t", None)
                )
                if not ts:
                    raise Exception(
                        "PolygonHelper.apply_timestamp: No timestamp found at 't'."
                    )
                ts_d = datetime.utcfromtimestamp(ts / 1000)
                record["timestamp_d"] = ts_d
                record["timestamp_s"] = ts_d.isoformat()
                ret.append(record)
            except Exception as e:
                # TODO: log exception
                print(e)

        list(map(lambda x: append_timestamp(x), records))
        return ret
