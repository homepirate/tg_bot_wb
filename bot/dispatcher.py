import os

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode

from config import Config, config
from db_access_control import DBAccessControlMiddleware
from scheduler import schedule_all_tasks
from utils.handlers_utils import run_action
from .handlers import router as handlers_router


async def start_bot():
    proxy = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    session = AiohttpSession(proxy=proxy) if proxy else AiohttpSession()

    bot = Bot(
        token=Config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=session,
    )

    dp = Dispatcher()
    dp.message.outer_middleware(DBAccessControlMiddleware(config.AsyncSessionLocal))
    dp.include_router(handlers_router)

    print("Бот запущен...")

    try:
        await schedule_all_tasks(config.AsyncSessionLocal, run_action, bot)
        await dp.start_polling(bot)
    finally:
        await bot.session.close()