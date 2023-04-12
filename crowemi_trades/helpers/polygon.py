import os
import json
import time
from datetime import datetime
import functools

from polygon import RESTClient


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
        ret = self._client.get_aggs(
            ticker,
            interval,
            timespan,
            start_date,
            end_date,
            raw=raw,
        )
        data = json.loads(ret.data)
        data["results"] = self.apply_timestamp(data.get("results"))
        return {"status": ret.status, "data": data}

    def apply_timestamp(self, records: list) -> list:
        def append_timestamp(record: list):
            assert record.get("t", None), "Expecting 't' element, received None"
            record["ts"] = datetime.utcfromtimestamp(record["t"] / 1000).isoformat()

        list(map(lambda x: append_timestamp(x), records))
        return records
