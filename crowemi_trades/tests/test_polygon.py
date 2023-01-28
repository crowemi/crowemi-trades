from crowemi_trades.helpers.polygon import PolygonHelper


def test_get_aggs():
    for x in range(10):
        d = PolygonHelper().get_aggregates(
            ticker="C:EURUSD",
            timespan=5,
            interval="minute",
            start_date="2023-01-03",
            end_date="2023-01-03",
        )
    assert d
