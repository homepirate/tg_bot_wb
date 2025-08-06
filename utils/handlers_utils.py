from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message

from config import config
from core import run_all_to, run_all_from
from errors import AuthorizationError

from typing import List

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


async def send_long_text(
    target: int | Message,
    text: str,
    *,
    bot: Bot | None = None,
    parse_mode: str | None = None
):
    """
    Отправляет длинный текст, разбивая его на части по 4096 символов.
    Работает как с объектом Message, так и с user_id.
    """
    messages = split_telegram_message(text)

    if isinstance(target, Message):
        for msg in messages:
            await target.answer(msg, parse_mode=parse_mode, disable_web_page_preview=True)
    else:
        if bot is None:
            bot = Bot(token=config.BOT_TOKEN)
        for msg in messages:
            await bot.send_message(
                chat_id=target,
                text=msg,
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )

async def run_action(target: int | Message, action: str):
    """
    Универсальный запуск действия по сообщению ИЛИ user_id.
    """
    if isinstance(target, Message):
        user_id = target.from_user.id
        send = target.answer
    else:
        user_id = target
        bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        send = lambda text: bot.send_message(chat_id=user_id, text=text)

    try:
        if action == "all_to":
            await send("Запущен процесс All To...")
            errors = await run_all_to()
            await send("✅ All To завершено.")
        elif action == "all_from":
            await send("Запущен процесс All From...")
            errors = await run_all_from()
            await send("✅ All From завершено.")
        else:
            await send("Неизвестная команда.")
            return

        if errors:
            errors_str = "\n".join(map(str, errors))
            if isinstance(target, Message):
                await send_long_text(target, f"Ошибки:\n{errors_str}")
            else:
                await bot.send_message(chat_id=user_id, text="⚠️ Были ошибки:")
                await send_long_text(user_id, f"Ошибки:\n{errors_str}", bot=bot)

    except AuthorizationError as e:
        await send(f"Ошибка авторизации:\n{e}")