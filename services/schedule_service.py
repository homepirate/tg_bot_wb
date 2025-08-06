from sqlalchemy import select, extract
from datetime import time
from models import Schedule

async def save_schedule(session, user_id: int, weekday: int, time_: time, action: str):
    schedule = Schedule(user_id=user_id, weekday=weekday, time=time_, action=action)
    session.add(schedule)
    await session.commit()

async def get_all_schedules(session):
    result = await session.execute(select(Schedule))
    return result.scalars().all()


async def is_schedule_still_exists(session_maker, user_id: int, action: str, weekday: int, hour: int, minute: int) -> bool:
    async with session_maker() as session:
        stmt = select(Schedule).where(
            Schedule.user_id == user_id,
            Schedule.action == action,
            Schedule.weekday == weekday,
            extract('hour', Schedule.time) == hour,
            extract('minute', Schedule.time) == minute
        )
        res = await session.execute(stmt)
        return res.scalar_one_or_none() is not None