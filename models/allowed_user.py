from sqlalchemy import Column, BigInteger
from .base import Base
class AllowedUser(Base):
    __tablename__ = "allowed_users"

    user_id = Column(BigInteger, primary_key=True)
