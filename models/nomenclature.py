from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base

class Nomenclature(Base):
    __tablename__ = "nomenclature"

    id = Column(Integer, primary_key=True)
    wb_article = Column(String, nullable=False)
    root_id = Column(String, nullable=False)
    original_brand = Column(String, nullable=True)

    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)

    company = relationship("Company", back_populates="nomenclatures")
