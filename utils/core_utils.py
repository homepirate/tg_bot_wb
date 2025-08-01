import datetime
from typing import Any
from config import config

from services.holiday_service import is_date_in_holidays


def split_into_batches(items: list[dict], batch_size: int) -> list[list[dict]]:
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]


async def is_weekend() -> bool:
    """
    True, если сегодня суббота/воскресенье ИЛИ дата есть в таблице holidays.
    """
    today = datetime.datetime.today()
    if today.weekday() in (5, 6):  # 5=СБ, 6=ВС
        return True
    async with config.AsyncSessionLocal() as session:
        return await is_date_in_holidays(session, today)


ALLOWED_TOP_LEVEL_FIELDS = {
    "nmID",
    "vendorCode",
    "brand",
    "title",
    "description",
    "dimensions",
    "characteristics",
    "sizes",
    "api_key",
    "core", # только для группировки, в payload не уходит
}

def filter_card_top_level(raw_card: dict[str, Any]) -> dict[str, Any]:
    """
    Возвращает новый dict карточки, оставляя ТОЛЬКО допустимые поля верхнего уровня,
    при этом 'api_key' из результата исключается (он нужен только для группировки).

    Не мутирует исходный словарь.
    """
    # 1) Оставляем только разрешённые ключи
    filtered = {k: raw_card[k] for k in ALLOWED_TOP_LEVEL_FIELDS if k in raw_card}
    # 2) Исключаем api_key из payload для API
    # payload_card = {k: v for k, v in filtered.items() if k != "api_key"}
    return filtered