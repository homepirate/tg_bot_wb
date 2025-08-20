from sqlalchemy import select, distinct
from sqlalchemy.ext.asyncio import AsyncSession

from models import Brand


async def get_night_brands(session: AsyncSession) -> list[Brand]:
    """
    Возвращает список брендов, которые работают ночью (is_daytime = False)
    """
    stmt = select(Brand).where(Brand.is_daytime.is_(False))
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_night_brand_wbids(session, company_id: int, default_brand: str) -> list[int]:
    """
    Возвращает WB ID всех ночных брендов компании, кроме default_brand.
    """
    result = await session.execute(
        select(Brand).where(
            Brand.company_id == company_id,
            Brand.is_daytime == False,
            Brand.name != default_brand
        )
    )
    brands = result.scalars().all()
    return [brand.wbID for brand in brands]


async def get_all_brand_wbids_except_default(session, company_id: int, default_brand: str) -> list[int]:
    """
    Возвращает WB ID всех брендов компании, кроме default_brand.
    """
    result = await session.execute(
        select(Brand).where(
            Brand.company_id == company_id,
            Brand.name != default_brand
        )
    )
    brands = result.scalars().all()
    return [brand.wbID for brand in brands]


async def get_all_brand_wbids_except_default(session, default_brand: str) -> list[int]:
    """
    Возвращает уникальные WB ID всех брендов (всех компаний), кроме default_brand.
    """
    result = await session.execute(
        select(distinct(Brand.wbID)).where(Brand.name != default_brand)
    )
    wbids = result.scalars().all()
    return list(wbids)

async def is_night_brand(session: AsyncSession, company_id: int, brand_name: str) -> bool:
    """
    True, если бренд с именем brand_name для компании company_id помечен как ночной (is_daytime = False).
    """
    if not brand_name:
        return False

    stmt = select(Brand).where(
        Brand.company_id == company_id,
        Brand.name == brand_name,
        Brand.is_daytime == False,
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None