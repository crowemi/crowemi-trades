import unittest
import os
from datetime import datetime

from crowemi_trades.storage.s3_storage import S3Storage
from crowemi_trades.indicators.session_indicator import SessionIndicator


class TestSessionIndicator(unittest.TestCase):
    def setUp(self) -> None:
        self.stor = S3Storage(
            bucket="crowemi-trades",
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            endpoint_override="http://localhost:4566",
            region="us-east-1",
        )
        self.session = SessionIndicator()
        return super().setUp()

    def test_run(self):
        # TODO: create test for run method
        pass

    def test_session(self):
        today = datetime.now()
        ts = list()
        for t in range(0, 24):
            ts.append(datetime(today.year, today.month, today.hour, t))

        for t in ts:
            session = SessionIndicator.get_session(t)
            if t.hour >= 0 and t.hour < 6:
                self.assertTrue(
                    len(session) == 2,
                    f"Hour {t.hour} does not meet session length. Length {len(session)}",
                )
                self.assertIn(
                    "sydney", session, f"Hour {t.hour} has no sydney session."
                )
                self.assertIn("tokyo", session, f"Hour {t.hour} has no tokyo session.")
            if t.hour == 6:
                assert (
                    len(session) == 1
                ), f"Hour {t.hour} does not meet session length. Length {len(session)}"
                assert "tokyo" in session, f"Hour {t.hour} has no tokyo session."
            if t.hour >= 7 and t.hour < 9:
                assert (
                    len(session) == 2
                ), f"Hour {t.hour} does not meet session length. Length {len(session)}"
                assert "london" in session, f"Hour {t.hour} has no london session."
                assert "tokyo" in session, f"Hour {t.hour} has no tokyo session."
            if t.hour >= 10 and t.hour < 12:
                assert (
                    len(session) == 1
                ), f"Hour {t.hour} does not meet session length. Length {len(session)}"
                assert "london" in session, f"Hour {t.hour} has no london session."
            if t.hour >= 12 and t.hour < 16:
                assert (
                    len(session) == 2
                ), f"Hour {t.hour} does not meet session length. Length {len(session)}"
                assert "london" in session, f"Hour {t.hour} has no london session."
                assert "newyork" in session, f"Hour {t.hour} has no newyork session."
            if t.hour >= 16 and t.hour < 21:
                assert (
                    len(session) == 1
                ), f"Hour {t.hour} does not meet session length. Length {len(session)}"
                assert "newyork" in session, f"Hour {t.hour} has no newyork session."
            if t.hour >= 21:
                assert (
                    len(session) == 1
                ), f"Hour {t.hour} does not meet session length. Length {len(session)}"
                assert "sydney" in session, f"Hour {t.hour} has no sydney session."


if __name__ == "__main__":
    unittest.main()
