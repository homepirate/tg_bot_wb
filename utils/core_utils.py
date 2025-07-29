import datetime


def split_into_batches(items: list[dict], batch_size: int) -> list[list[dict]]:
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]


def is_weekend() -> bool:
    """Возвращает True, если сегодня суббота или воскресенье."""
    return datetime.datetime.today().weekday() in (5, 6)