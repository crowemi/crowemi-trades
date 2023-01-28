from datetime import datetime, timezone

def t_to_utc(x :dict) -> datetime:
    x["t_utc"] = datetime.fromtimestamp(x['t']/1000).astimezone(timezone.utc)