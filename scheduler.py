import asyncio
import pytz
from datetime import datetime, timedelta
from typing import Callable


DAYS_MAPPING = {
    "ПН": 0,
    "ВТ": 1,
    "СР": 2,
    "ЧТ": 3,
    "ПТ": 4,
    "СБ": 5,
    "ВС": 6,
}


def schedule_weekly_task(day_str: str, time_str: str, callback: Callable, *args):
    """
    Запускает задачу каждую неделю в указанный день и время по МСК.

    :param day_str: День недели в формате "ПН", "ВТ", ...
    :param time_str: Время запуска в формате "ЧЧ:ММ"
    :param callback: Функция для запуска
    :param args: Аргументы для callback
    """
    msk = pytz.timezone("Europe/Moscow")

    if day_str not in DAYS_MAPPING:
        raise ValueError(f"Неверный день недели: {day_str}")

    hour, minute = map(int, time_str.split(":"))
    if not (0 <= hour < 24 and 0 <= minute < 60):
        raise ValueError("Неверное время")

    weekday_target = DAYS_MAPPING[day_str]

    async def run_periodically():
        while True:
            now = datetime.now(msk)
            days_ahead = (weekday_target - now.weekday() + 7) % 7
            target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)

            if target_time <= now:
                target_time += timedelta(days=7)

            delay = (target_time - now).total_seconds()
            await asyncio.sleep(delay)

            try:
                await callback(*args)
            except Exception as e:
                print(f"[Ошибка задачи] {e}")

    asyncio.create_task(run_periodically())
