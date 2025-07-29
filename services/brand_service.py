from sqlalchemy import select
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