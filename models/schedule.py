import datetime

from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

class Schedule(Base):
    __tablename__ = "schedules"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int]
    weekday: Mapped[int]  # 0 = ПН ... 6 = ВС
    time: Mapped[datetime.time]    # '12:00'
    action: Mapped[str]   # all_from / all_to
