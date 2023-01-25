import os
import time
import functools

from polygon import RESTClient


class PolygonHelper:
    """A helper class to abstract common polygon functionality."""

    def __init__(self, polygon_token: str = None) -> None:
        # if token not passed, is it in env
        polygon_token = (
            polygon_token if polygon_token is not None else os.getenv("polygon_key")
        )
        if not polygon_token:
            raise Exception("No polgygon token found.")
        self._client = RESTClient(api_key=polygon_token)

        self.requests = 0

    def throttle(func):
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
        interval: str,
        interval_period: int,
        start_date: str,
        end_date: str,
    ):
        return self._client.get_aggs(
            ticker,
            interval_period,
            interval,
            start_date,
            end_date,
            raw=True,
        )
