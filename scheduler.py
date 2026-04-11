import asyncio
import traceback
from datetime import datetime, timedelta

import pytz
from aiogram import Bot
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from models import Schedule
from services.schedule_service import is_schedule_still_exists

DAYS_MAPPING_REVERSE = {
    0: "ПН",
    1: "ВТ",
    2: "СР",
    3: "ЧТ",
    4: "ПТ",
    5: "СБ",
    6: "ВС",
}


def schedule_weekly_task(
    session_maker: async_sessionmaker,
    weekday: int,
    hour: int,
    minute: int,
    user_id: int,
    action: str,
    callback,
    bot: Bot,
):
    msk = pytz.timezone("Europe/Moscow")

    async def run_periodically():
        while True:
            now = datetime.now(msk)
            days_ahead = (weekday - now.weekday() + 7) % 7
            target_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)

            if target_time <= now:
                target_time += timedelta(days=7)

            await asyncio.sleep((target_time - now).total_seconds())

            still_exists = await is_schedule_still_exists(
                session_maker=session_maker,
                user_id=user_id,
                action=action,
                weekday=weekday,
                hour=hour,
                minute=minute,
            )

            if still_exists:
                try:
                    await callback(user_id, action, bot=bot)
                except Exception as e:
                    print(f"[Ошибка выполнения задачи] user_id={user_id}, action={action}: {e}")
                    traceback.print_exc()
            else:
                print(f"⛔️ Задача для user_id={user_id} удалена из БД, останавливаем.")
                return

    asyncio.create_task(run_periodically())


async def schedule_all_tasks(session_maker: async_sessionmaker, callback, bot: Bot):
    async with session_maker() as session:
        result = await session.execute(select(Schedule))
        schedules = result.scalars().all()

    for schedule in schedules:
        weekday = schedule.weekday
        hour = schedule.time.hour
        minute = schedule.time.minute
        user_id = schedule.user_id
        action = schedule.action

        schedule_weekly_task(
            session_maker=session_maker,
            weekday=weekday,
            hour=hour,
            minute=minute,
            callback=callback,
            user_id=user_id,
            action=action,
            bot=bot,
        )

        day_str = DAYS_MAPPING_REVERSE.get(weekday, str(weekday))

        try:
            await bot.send_message(
                chat_id=user_id,
                text=f"✅ Задача для '{action}' запланирована на {day_str} {hour:02}:{minute:02} по МСК",
                request_timeout=90,
            )
            print(f"🔁 Задача восстановлена: user_id={user_id}, action={action}, {day_str} {hour:02}:{minute:02}")
        except Exception as e:
            print(f"[schedule_all_tasks] Не удалось отправить уведомление user_id={user_id}: {e}")