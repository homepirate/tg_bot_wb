from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from models.allowed_user import AllowedUser

async def is_user_allowed(session: AsyncSession, user_id: int) -> bool:
    """
    Проверяет, есть ли пользователь в таблице allowed_users.
    """
    result = await session.execute(
        select(AllowedUser.user_id).where(AllowedUser.user_id == user_id)
    )
    return result.scalar_one_or_none() is not None