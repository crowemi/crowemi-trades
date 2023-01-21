import os
import json
from datetime import datetime, timezone

from storage.s3_storage import S3Storage
from helpers.core import t_to_utc
from helpers.polygon_helper import PolygonHelper

POLYGON_HELPER = PolygonHelper()

def main():
    # we want to be able to get aggs for a series of dates for any given symbol
    # we want te be able to enrich the aggs with other indicators

    agg = POLYGON_HELPER.client.get_aggs(
        "C:EURUSD",
        5, # five minute
        "minute",
        "2023-01-02",
        "2023-01-02",
        raw=True,
    )
        # "c": close
        # "h": high
        # "l": low
        # "n": number of transactions,
        # "o": open
        # "t": unix ms timestamp,
        # "v": trading volume,
        # "vw": volume weighted average price

    ret = json.loads(agg.data)
    list(map(lambda x: t_to_utc(x), ret['results']))

    # list(map(lambda x :dict: x["t_utc"]=datetime.fromtimestamp(x['t']/1000).astimezone(timezone.utc), ret['results']))
    # map(lambda x: )
    for r in ret['results']:
        print(r['t_utc'])

    print(agg)



if __name__ == "__main__":
    main()