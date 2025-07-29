from aiogram.types import Message

from core import run_all_to, run_all_from
from errors import AuthorizationError


async def run_action(message: Message, action: str):
    if action == "all_to":
        await message.answer("Запущен процесс All To...")
        try:
            errors = await run_all_to()
            await message.answer("✅ All To завершено.")
            if errors:
                errors_str = '\n'.join(errors)
                await message.answer(f"Ошибки:\n{errors_str}")
        except AuthorizationError as e:
            await message.answer(f"Ошибка авторизации\n{e}")
    elif action == "all_from":
        await message.answer("Запущен процесс All From...")
        try:
            errors = await run_all_from()
            await message.answer("✅ All From завершено.")
            if errors:
                errors_str = '\n'.join(errors)
                await message.answer(f"Ошибки:\n{errors_str}")
        except AuthorizationError as e:
            await message.answer(f"Ошибка авторизации\n{e}")
    else:
        await message.answer("Неизвестная команда.")
