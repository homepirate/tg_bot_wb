from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship

from .brand import Brand
from .base import Base

class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    api_key = Column(String, nullable=False)
    expires_at = Column(Date, nullable=False)
    company_id = Column(Integer, unique=True)
    cabinet_order = Column(Integer, unique=True, nullable=True)

    default_brand_id = Column(
        Integer,
        ForeignKey("brands.id", name="fk_companies_default_brand_id"),
        nullable=True
    )

    default_brand = relationship(
        "Brand",
        foreign_keys=[default_brand_id]
    )

    brands = relationship(
        "Brand",
        back_populates="company",
        cascade="all, delete-orphan",
        foreign_keys=[Brand.company_id]
    )

    nomenclatures = relationship(
        "Nomenclature",
        back_populates="company",
        cascade="all, delete-orphan"
    )
