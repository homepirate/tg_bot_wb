from sqlalchemy import Column, Integer, Date, String, select
from .base import Base
from config import config

class Holiday(Base):
    __tablename__ = "holidays"

    id = Column(Integer, primary_key=True)
    date = Column(Date, unique=True, nullable=False)
    description = Column(String, nullable=True)

    @staticmethod
    async def is_day_off(target_date: date) -> bool:
        if target_date.weekday() >= 5:
            return True

        async with config.AsyncSessionLocal() as session:
            result = await session.execute(
                select(Holiday).where(Holiday.date == target_date)
            )
            return result.scalar_one_or_none() is not None
