from datetime import datetime
import polars as pl

from crowemi_trades.indicators.base_indicator import BaseIndicator


class SessionIndicator(BaseIndicator):
    """Calculates trading session windows for the given period.
    ---
    Sydney:     2100-0600 UTC
    Tokyo:      0000-0900 UTC
    London:     0700-1600 UTC
    New York:   1200-2100 UTC
    """

    SESSION = [
        {"name": "SYDNEY", "start_utc": 21, "end_utc": 6},
        {"name": "TOKYO", "start_utc": 0, "end_utc": 9},
        {"name": "LONDON", "start_utc": 7, "end_utc": 16},
        {"name": "NEWYORK", "start_utc": 12, "end_utc": 21},
    ]

    def __init__(
        self,
    ) -> None:
        super().__init__()

    def run(self, records: pl.DataFrame, **kwargs) -> pl.DataFrame:
        return records.with_columns(
            pl.col("timestamp_s").apply(self.get_session).alias("i_session")
        )

    def graph():
        super().graph()

    @staticmethod
    def get_session(dt) -> list:
        if type(dt) == str:
            dt = datetime.fromisoformat(dt)

        session = list()
        if (dt.hour >= 21 and dt.hour <= 24) or (dt.hour >= 0 and dt.hour < 6):
            session.append("sydney")
        if dt.hour >= 0 and dt.hour < 9:
            session.append("tokyo")
        if dt.hour >= 7 and dt.hour < 16:
            session.append("london")
        if dt.hour >= 12 and dt.hour < 21:
            session.append("newyork")
        assert len(session) > 0, "Session must not be empty."
        return session
