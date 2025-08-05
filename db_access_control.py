from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery
from sqlalchemy.ext.asyncio import async_sessionmaker
from typing import Callable, Awaitable, Dict, Any

from services.access_service import is_user_allowed


class DBAccessControlMiddleware(BaseMiddleware):
    def __init__(self, session_factory: async_sessionmaker):
        self.session_factory = session_factory

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user = data.get("event_from_user")
        if user is None:
            return

        async with self.session_factory() as session:
            allowed = await is_user_allowed(session, user.id)

        if not allowed:
            # Универсальный способ ответить пользователю
            if isinstance(event, Message):
                await event.answer("⛔️ У вас нет доступа.")
            elif isinstance(event, CallbackQuery):
                await event.message.answer("⛔️ У вас нет доступа.")
            else:
                print(f"⛔️ Пользователь {user.id} не имеет доступа, но тип события неизвестен: {type(event)}")
            return  # блокируем доступ

        return await handler(event, data)