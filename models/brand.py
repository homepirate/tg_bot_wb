from sqlalchemy import Column, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base

class Brand(Base):
    __tablename__ = "brands"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    is_daytime = Column(Boolean, default=False)
    company_id = Column(
        Integer,
        ForeignKey("companies.id", name="fk_brands_company_id"),
        nullable=False
    )
    wbID = Column(Integer, nullable=False)

    company = relationship(
        "Company",
        back_populates="brands",
        foreign_keys=[company_id]
    )
