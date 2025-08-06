from datetime import time

from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command

import re

from config import config
from scheduler import schedule_weekly_task
from services.schedule_service import save_schedule
from utils.handlers_utils import run_action
from .keyboards import main_menu, schedule_menu

router = Router()



user_context = {}  # временное хранилище статуса пользователя

DAYS_MAPPING = {
    "ПН": 0,
    "ВТ": 1,
    "СР": 2,
    "ЧТ": 3,
    "ПТ": 4,
    "СБ": 5,
    "ВС": 6,
}

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Привет! Я бот WB!", reply_markup=main_menu)


@router.message(F.text == "All To")
async def handle_all_to_entry(message: Message):
    user_context[message.from_user.id] = "all_to"
    await message.answer("Выберите действие:", reply_markup=schedule_menu)


@router.message(F.text == "All From")
async def handle_all_from_entry(message: Message):
    user_context[message.from_user.id] = "all_from"
    await message.answer("Выберите действие:", reply_markup=schedule_menu)


@router.message(lambda m: m.text == "Меню")
async def handle_back(message: Message):
    await message.answer("Вы вернулись в главное меню.", reply_markup=main_menu)


@router.message(F.text == "Запустить сейчас")
async def handle_run_now(message: Message):
    action = user_context.get(message.from_user.id)
    await run_action(message, action)
    await message.answer("Меню", reply_markup=main_menu)


@router.message(F.text == "Задать расписание")
async def handle_schedule_request(message: Message):
    await message.answer("Введите время запуска в формате ДД ЧЧ:ММ (по московскому времени):")



@router.message(F.text.regexp(r"^(ПН|ВТ|СР|ЧТ|ПТ|СБ|ВС)\s\d{1,2}:\d{2}$"))
async def handle_schedule_day_time(message: Message):
    action = user_context.get(message.from_user.id)
    match = re.match(r"^(ПН|ВТ|СР|ЧТ|ПТ|СБ|ВС)\s(\d{1,2}):(\d{2})$", message.text.strip())
    if not match:
        await message.answer("Неверный формат. Введите, например: ПН 12:00")
        return

    day_str, hour, minute = match[1], int(match[2]), int(match[3])
    if hour >= 24 or minute >= 60:
        await message.answer("Неверное время. Часы < 24, минуты < 60.")
        return

    weekday = DAYS_MAPPING[day_str]
    time_obj = time(hour=hour, minute=minute)

    async with config.AsyncSessionLocal() as session:
        await save_schedule(session, user_id=message.from_user.id, weekday=weekday, time_=time_obj, action=action)

    try:
        schedule_weekly_task(
            session_maker=config.AsyncSessionLocal,
            weekday=weekday,
            hour=hour,
            minute=minute,
            callback=run_action,
            user_id=message.from_user.id,
            action=action,
        )
        await message.answer(
            f"✅ Задача для '{action}' запланирована на {day_str} {hour:02}:{minute:02} по МСК",
            reply_markup=main_menu
        )
    except Exception as e:
        await message.answer(f"Ошибка при создании задачи: {e}")