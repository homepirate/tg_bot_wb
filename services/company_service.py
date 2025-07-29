from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from models import Nomenclature
from models.company import Company
from models.brand import Brand


async def get_all_companies(session) -> list[Company]:
    result = await session.execute(select(Company))
    return list(result.scalars().all())

async def get_sorted_companies(session: AsyncSession) -> list[Company]:
    """
    Возвращает компании, отсортированные по убыванию cabinet_order.
    """
    result = await session.execute(
        select(Company).order_by(desc(Company.cabinet_order)).options(
            selectinload(Company.brands)
        )
    )
    return list(result.scalars().all())



async def get_companies_with_nomenclature(session: AsyncSession) -> list[Company]:
    result = await session.execute(
        select(Company)
        .options(selectinload(Company.nomenclatures).selectinload(Nomenclature.company))
        .order_by(desc(Company.cabinet_order))
    )
    return list(result.scalars().all())


async def get_company_by_api_key(session, api_key: str):
    result = await session.execute(select(Company).where(Company.api_key == api_key))
    return result.scalar_one_or_none()