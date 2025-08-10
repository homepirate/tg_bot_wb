from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="All To")],
        [KeyboardButton(text="All From")],
    ],
    resize_keyboard=True
)


schedule_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Запустить сейчас")],
        [KeyboardButton(text="Задать расписание")],
        [KeyboardButton(text="Меню")],
    ],
    resize_keyboard=True
)

mode_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Режим: выходные"), KeyboardButton(text="Режим: будни")],
        [KeyboardButton(text="Меню")],
    ],
    resize_keyboard=True
)