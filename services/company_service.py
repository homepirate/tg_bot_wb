from sqlalchemy import select, desc, asc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, joinedload

from models import Nomenclature
from models.company import Company

from sqlalchemy.exc import InterfaceError

async def get_all_companies(session) -> list[Company]:
    stmt = (
        select(Company)
        .options(
            selectinload(Company.default_brand),     # подгружаем отношение
            # selectinload(Company.nomenclatures),   # ← раскомментируй, если используешь
        )
    )
    result = await session.execute(stmt)
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
        .order_by(asc(Company.cabinet_order))
    )
    return list(result.scalars().all())


async def get_company_by_api_key(session, api_key: str):
    stmt = (
        select(Company)
        .options(
            joinedload(Company.default_brand)  # или selectinload(Company.default_brand)
        )
        .where(Company.api_key == api_key)
    )

    result = await session.execute(stmt)
    return result.scalar_one_or_none()



async def get_company_by_api_key_safe(session_maker, api_key: str):
    try:
        async with session_maker() as session:
            return await get_company_by_api_key(session, api_key)
    except InterfaceError:
        # второй шанс с новым соединением
        async with session_maker() as session:
            return await get_company_by_api_key(session, api_key)