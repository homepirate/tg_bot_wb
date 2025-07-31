import asyncio
from collections import defaultdict
from typing import Any

from api_client import WBClientAPI
from config import config
from errors import AuthorizationError, RootIDError, UpdateCardsError
from services.company_service import get_sorted_companies, get_companies_with_nomenclature, get_company_by_api_key, \
    get_all_companies
from utils.core_utils import split_into_batches, is_weekend, filter_card_top_level
from services.brand_service import get_night_brands, get_night_brand_wbids, get_all_brand_wbids_except_default

REQUEST_DELAY_ONE_SECOND = 1
REQUEST_DELAY_SIX_SECONDS = 6
BATCH_LIMIT = 3000

async def run_all_from() -> list[str]:
    error_send: list[str] = []

    """Главная функция запуска логики."""
    if await is_weekend():
        async with config.AsyncSessionLocal() as session:
            night_brands = await get_night_brands(session)
            print(f"Получено ночных брендов: {len(night_brands)}")
    else:
        print("Сегодня не выходной — ночные бренды не обрабатываются.")
        night_brands = []

    all_cards = await process_cards()

    # 1) Выставляем бренды
    updated_cards, tg_messages = await process_brands(all_cards, night_brands)
    error_send.append("Неизменившиеся  каточки:")
    error_send.extend(tg_messages)
    print(updated_cards)

    # 2) Готовим карточки к отправке: фильтруем верхний уровень и возвращаем api_key
    prepared_cards: list[dict[str, Any]] = []
    for card in (updated_cards or []):
        api_key = card.get("api_key")
        if not api_key:
            print("run_all_from Пропущена карточка без API-ключа")
            continue
        payload = filter_card_top_level(card)   # убирает лишние поля и api_key из payload
        payload["api_key"] = api_key           # вернуть для группировки внутри send_cards
        prepared_cards.append(payload)

    # 3) Отправляем подготовленные карточки
    error_send_card = await send_cards(prepared_cards)
    if error_send_card:
        error_send.append("Ошибки:")
        error_send.extend(error_send_card)

    # 4) Пауза и повторная попытка для части карточек (как у вас было)
    await asyncio.sleep(10)
    # await asyncio.sleep(600)


    async with config.AsyncSessionLocal() as session:
        companies = await get_all_companies(session)

    client = WBClientAPI()

    for company in companies:
        async with config.AsyncSessionLocal() as session:
            if await is_weekend():
                wb_brand_ids = await get_night_brand_wbids(session, company.id, company.default_brand.name)
            else:
                wb_brand_ids = await get_all_brand_wbids_except_default(session, company.id, company.default_brand.name)

        if not wb_brand_ids:
            print(f"⛔️ Нет брендов для компании {company.name}")
            continue

        products = await client.get_all_data_by_company_id_and_brands(company.id, wb_brand_ids)
        print(f"📦 {len(products)} товаров найдено для компании {company.name}")

        product_root_ids = {p.get("root") for p in products if p.get("root")}

        # Фильтруем карточки, которые нужно попробовать еще раз
        retry_cards = [card for card in (updated_cards or []) if card.get("root") in product_root_ids]
        if not retry_cards:
            continue

        failed_cards: list[dict[str, Any]] = []

        for card in retry_cards:
            root_id = card["root_id"]
            try:
                # Пытаемся выставить бренд повторно (ваша логика)
                updated_cards, tg_messages = await process_brands(all_cards, night_brands)
                print(f"🔁 Повторно установлен бренд для root_id={root_id}")
                if tg_messages:
                    error_send.append("Неизменившиеся  каточки:")
                    error_send.extend(tg_messages)

                retry_prepared = []
                for c in updated_cards or []:
                    api_key = c.get("api_key")
                    if not api_key:
                        continue
                    p = filter_card_top_level(c)
                    p["api_key"] = api_key
                    retry_prepared.append(p)
                error_send = await send_cards(retry_prepared)
                if error_send:
                    error_send.append("Ошибки при повторной отправке:")
                    failed_cards.extend(error_send)

            except Exception as e:
                print(f"❌ Ошибка повторной установки бренда root_id={root_id}: {e}")
                failed_cards.append(card)

        # Отправка ошибок в Telegram, если есть
        if failed_cards:
            messages = [
                f"❗️ Не удалось установить бренд для root_id {card['root_id']} (API ключ: {card.get('api_key', '—')})"
                for card in failed_cards
            ]
            error_send.extend(messages)
    return error_send

async def run_all_to():
    products = await get_all_product_from_catalog()
    root_ids = [product["root"] for product in products]
    print(f"Root_IDS {root_ids}")
    cards_for_update, errors = await get_and_update_brand_in_card(root_ids)

    prepared_cards: list[dict[str, Any]] = []
    for card in cards_for_update or []:
        api_key = card.get("api_key")
        card['root'] = card.get("imtID")
        if not api_key:
            print("run_all_to Пропущена карточка без API-ключа")
            continue
        payload = filter_card_top_level(card)
        prepared_cards.append(payload)
    print(prepared_cards)
    error_send = await send_cards(prepared_cards)
    if error_send:
        errors.extend(error_send)
    return errors


async def process_cards():
    """Обработка компаний и их номенклатуры."""
    client = WBClientAPI()
    all_cards = []

    async with config.AsyncSessionLocal() as session:
        companies = await get_companies_with_nomenclature(session)

    for company in companies:
        print(f"\n🔍 Компания: {company.name}")
        api_key = company.api_key  # или другой способ получить ключ

        seen_root_ids = set()  # уникальность root_id внутри одной компанииf

        for nom in company.nomenclatures:
            # безопасное приведение к int
            try:
                root_id = int(nom.root_id)
            except (TypeError, ValueError):
                print(f"Пропущен некорректный root_id: {nom.root_id}")
                continue

            # пропуск повторов
            if root_id in seen_root_ids:
                print(f"⏩ Пропущен дубликат root_id: {root_id}")
                continue

            seen_root_ids.add(root_id)

            # вызов API (если у тебя api_key — это переменная с токеном)
            cards = await client.get_cards_list(api_key=api_key, root_id=root_id)

            for card in cards:
                card["root"] = card["imtID"]
                card["api_key"] = nom.company.api_key
                all_cards.append(card)
            print(f"📦 Получено карточек для root_id={root_id}: {len(cards)}")
    return all_cards


async def process_brands(all_cards: list[dict], night_brands: list[str]) -> tuple[list[dict], list[str]]:
    """
    Обрабатывает бренды в карточках.

    Возвращает:
    - список карточек, где бренд был изменён на default_brand,
    - список сообщений для Telegram об уже корректных карточках.
    """
    updated_cards = []
    messages_for_telegram = []
    already_added_msgs = set()

    for card in all_cards:
        brand = card.get("brand")
        api_key = card.get("api_key")

        # Получение компании по API ключу
        async with config.AsyncSessionLocal() as session:
            company = await get_company_by_api_key(session, api_key)

        if not company:
            print(f"Компания с ключом {api_key} не найдена.")
            continue

        default_brand = company.default_brand.name
        root_id = card.get("root") or "неизвестен"

        if brand == default_brand:
            msg = f"🔸 RootID {root_id}: бренд остался {default_brand}"
            if msg not in already_added_msgs:
                messages_for_telegram.append(msg)
                already_added_msgs.add(msg)
            continue

        if brand in night_brands:
            # Ночной бренд — не трогаем
            continue

        # Заменить бренд на default_brand
        card["brand"] = default_brand
        updated_cards.append(card)

    return updated_cards, messages_for_telegram


async def get_all_product_from_catalog() -> list[dict]:
    all_products = []
    api = WBClientAPI()
    companies = []

    async with config.AsyncSessionLocal() as session:
        companies = await get_sorted_companies(session)

    for company in companies:
        print(f"Обработка компании: {company.name} (ID: {company.company_id})")

        company_products = await api.get_all_data_by_company_id(company.company_id)
        print(f" Найдено товаров: {len(company_products)}")

        for product in company_products:
            product["company_id"] = company.company_id

        all_products.extend(company_products)

    print(f"Всего собрано товаров: {len(all_products)}")
    return all_products


async def get_and_update_brand_in_card(available_root_ids: list) -> tuple[list[dict], list[str]]:
    client = WBClientAPI()
    errors = []
    updated_cards = []
    companies = []

    async with config.AsyncSessionLocal() as session:
        companies = await get_companies_with_nomenclature(session)

    seen_root_ids = set()

    for company in companies:
        print(f"\n🔍 Компания: {company.name}")

        for nom in company.nomenclatures:
            try:
                root_id = int(nom.root_id)
            except (TypeError, ValueError):
                print(f"⚠️ Пропущен некорректный root_id: {nom.root_id}")
                continue

            if root_id in seen_root_ids:
                print(f"⏩ Пропущен root_id {root_id} — уже обработан ранее")
                continue

            if root_id not in available_root_ids:
                print(f"⛔️ Пропущен root_id {root_id} — нет в каталоге WB")
                continue

            seen_root_ids.add(root_id)  # Запоминаем, что обработали
            print(f"✅ Обрабатываем root_id: {root_id}")

            try:
                cards = await client.get_cards_list(api_key=company.api_key, root_id=root_id)
            except AuthorizationError as e:
                raise e
            except RootIDError as e:
                errors.append(f"Company Name={company.name}, Company ID={company.company_id}: Error={e}")
                continue

            for card in cards:
                if card.get("brand") != nom.original_brand:
                    print(f"бренд: {card.get('brand')} → {nom.original_brand}")
                    card["brand"] = nom.original_brand
                    card["api_key"] = nom.company.api_key
                    updated_cards.append(card)

            await asyncio.sleep(REQUEST_DELAY_ONE_SECOND)

    print(f"\nОбновлено карточек бренда: {len(updated_cards)}")
    return updated_cards, errors


async def send_cards(cards: list[dict]) -> list[str]:

    if not cards:
        print("Нет карточек для отправки.")
        return

    client = WBClientAPI()
    errors = []

    grouped_cards = defaultdict(list)

    for card in cards:
        api_key = card.get("api_key")
        if not api_key:
            print("send_cards Пропущена карточка без API-ключа")
            continue
        grouped_cards[api_key].append(card)

    for api_key, card_list in grouped_cards.items():
        print(f"\nОтправка карточек для api_key: {api_key}, всего: {len(card_list)}")

        batches = split_into_batches(card_list, BATCH_LIMIT)
        for idx, batch in enumerate(batches, start=1):

            print(f"Отправка батча {idx}/{len(batches)} ({len(batch)} карточек)...")

            try:
                success = await client.update_cards(api_key=api_key, cards=batch)
            except AuthorizationError as e:
                raise e
            except UpdateCardsError as e:
                print(f"Ошибка отправки: {e}")
                errors.append(f"{api_key}: {e}")
                continue

            if success:
                print(f"Успешно отправлено {len(batch)} карточек")
            else:
                print(f"Ошибка при отправке батча {idx}")

            if idx < len(batches):
                await asyncio.sleep(REQUEST_DELAY_SIX_SECONDS)

    if errors:
        print(f"\nВсего ошибок обновления карточек: {len(errors)}")

    return errors