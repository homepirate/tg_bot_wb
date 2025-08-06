from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from config import Config, config
from db_access_control import DBAccessControlMiddleware
from scheduler import schedule_all_tasks
from utils.handlers_utils import run_action
from .handlers import router as handlers_router

async def start_bot():
    bot = Bot(
        token=Config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()
    dp.message.outer_middleware(DBAccessControlMiddleware(config.AsyncSessionLocal))

    dp.include_router(handlers_router)
    print("Бот запущен...")

    await schedule_all_tasks(config.AsyncSessionLocal, run_action)
    await dp.start_polling(bot)

