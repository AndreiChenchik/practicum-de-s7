from datetime import datetime, timedelta
from typing import List

DATE_FORMAT = "%Y-%m-%d"  # yyyy-MM-dd


def path(date: datetime) -> str:
    date_str = date.strftime(DATE_FORMAT)
    return f"/user/username/data/events/date={date_str}/event_type=message"


def input_paths(date_str: str, depth: int) -> List[str]:
    date = datetime.strptime(date_str, DATE_FORMAT)
    return [path(date - timedelta(days=d)) for d in range(depth)]


# Двигайтесь дальше! Ваш код: rK6YrRAkPV
