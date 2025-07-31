from aiogram.types import Message

from core import run_all_to, run_all_from
from errors import AuthorizationError

from typing import List, Iterable

TELEGRAM_LIMIT = 4096

def split_telegram_message(text: str, limit: int = TELEGRAM_LIMIT) -> List[str]:
    """
    Разбивает текст на части длиной ≤ limit.
    Сначала пытается резать по абзацам, затем по строкам, затем — «жёстко» по длине.
    Не добавляет форматирование и не ломает HTML/MDV2, если резать по границам абзацев/строк.
    """
    if not text:
        return [""]

    if len(text) <= limit:
        return [text]

    parts: List[str] = []
    buf = ""

    def flush():
        nonlocal buf
        if buf:
            parts.append(buf)
            buf = ""

    # 1) по абзацам (двойной перенос)
    for para in text.split("\n\n"):
        if len(para) <= limit:
            candidate = f"{buf}\n\n{para}" if buf else para
            if len(candidate) <= limit:
                buf = candidate
            else:
                flush()
                buf = para
            continue

        # 2) абзац длинный — режем по строкам
        for line in para.split("\n"):
            if len(line) <= limit:
                candidate = f"{buf}\n{line}" if buf else line
                if len(candidate) <= limit:
                    buf = candidate
                else:
                    flush()
                    buf = line
                continue

            # 3) строка длинная — жёсткая нарезка
            start = 0
            while start < len(line):
                chunk = line[start:start + limit]
                if buf:
                    flush()
                parts.append(chunk)
                start += limit

        # разделим абзацы пустой строкой, если влезает
        if buf and len(buf) + 1 <= limit:
            buf += "\n"
        else:
            flush()

    flush()
    return [p for p in parts if p]


async def send_long_text(message, text: str, *, parse_mode: str | None = None):
    """
    Отправляет text, разрезая по 4096 символов.
    """
    for part in split_telegram_message(text):
        await message.answer(part, parse_mode=parse_mode, disable_web_page_preview=True)



async def run_action(message: Message, action: str):
    if action == "all_to":
        await message.answer("Запущен процесс All To...")
        try:
            errors = await run_all_to()
            await message.answer("✅ All To завершено.")
            if errors:
                errors_str = "\n".join(errors)
                await send_long_text(message, f"Ошибки:\n{errors_str}")  # <— отправка частями
        except AuthorizationError as e:
            await send_long_text(message, f"Ошибка авторизации\n{e}")
    elif action == "all_from":
        await message.answer("Запущен процесс All From...")
        try:
            errors = await run_all_from()
            await message.answer("✅ All From завершено.")
            if errors:
                errors_str = "\n".join(errors)
                await send_long_text(message, f"{errors_str}")  # <— отправка частями
        except AuthorizationError as e:
            await send_long_text(message, f"Ошибка авторизации\n{e}")
    else:
        await message.answer("Неизвестная команда.")