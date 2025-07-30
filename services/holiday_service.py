from datetime import date
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Holiday


async def is_date_in_holidays(session: AsyncSession, target_date: date) -> bool:
    """True, если дата есть в таблице holidays."""
    res = await session.execute(
        select(Holiday.date).where(Holiday.date == target_date)
    )
    return res.scalar_one_or_none() is not None
