import asyncio
from collections import defaultdict

from api_client import WBClientAPI
from config import config
from errors import AuthorizationError, RootIDError, UpdateCardsError
from services.company_service import get_sorted_companies, get_companies_with_nomenclature, get_company_by_api_key, \
    get_all_companies
from utils.core_utils import split_into_batches, is_weekend
from services.brand_service import get_night_brands, get_night_brand_wbids, get_all_brand_wbids_except_default

REQUEST_DELAY_ONE_SECOND = 1
REQUEST_DELAY_SIX_SECONDS = 6
BATCH_LIMIT = 3000

async def run_all_from():
    """Главная функция запуска логики."""
    if is_weekend():
        async with config.AsyncSessionLocal() as session:
            night_brands = await get_night_brands(session)
            print(f"Получено ночных брендов: {len(night_brands)}")
    else:
        print("Сегодня не выходной — ночные бренды не обрабатываются.")
        night_brands = []

    all_cards = await process_cards()
    updated_cards, tg_messages = await process_brands(all_cards, night_brands)
    error_send = await send_cards(updated_cards)

    await asyncio.sleep(600)

    async with config.AsyncSessionLocal() as session:
        companies = await get_all_companies(session)

    client = WBClientAPI()

    for company in companies:
        async with config.AsyncSessionLocal() as session:
            if is_weekend():
                wb_brand_ids = await get_night_brand_wbids(session, company.id, company.default_brand)
            else:
                wb_brand_ids = await get_all_brand_wbids_except_default(session, company.id, company.default_brand)

        if not wb_brand_ids:
            print(f"⛔️ Нет брендов для компании {company.name}")
            continue

        products = await client.get_all_data_by_company_id_and_brands(company.id, wb_brand_ids)
        print(f"📦 {len(products)} товаров найдено для компании {company.name}")


async def run_all_to():
    products = await get_all_product_from_catalog()
    root_ids = [product["root"] for product in products]
    print(f"Root_IDS {root_ids}")
    cards_for_update, errors = await get_and_update_brand_in_card(root_ids)
    print(f"cards_for_update {cards_for_update}")
    # error_send = await send_cards(cards_for_update)
    # errors.extend(error_send)
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

        for nom in company.nomenclatures:
            try:
                root_id = int(nom.root_id)
            except (TypeError, ValueError):
                print(f"Пропущен некорректный root_id: {nom.root_id}")
                continue

            cards = await client.get_cards_list(api_key, root_id)
            for card in cards:
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

    for card in all_cards:
        brand = card.get("brand")
        api_key = card.get("api_key")

        # Получение компании по API ключу
        async with config.AsyncSessionLocal() as session:
            company = await get_company_by_api_key(session, api_key)

        if not company:
            print(f"Компания с ключом {api_key} не найдена.")
            continue

        default_brand = company.default_brand
        root_id = card.get("root_id") or "неизвестен"

        if brand == default_brand:
            messages_for_telegram.append(f"🔸 RootID {root_id}: бренд остался {default_brand}")
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

    for company in companies:
        print(f"\n🔍 Компания: {company.name}")

        for nom in company.nomenclatures:
            try:
                root_id = int(nom.root_id)
            except (TypeError, ValueError):
                print(f"⚠️ Пропущен некорректный root_id: {nom.root_id}")
                continue

            if root_id not in available_root_ids:
                print(f"Пропущен root_id {root_id} — нет в каталоге WB")
                continue

            print(f"Обрабатываем root_id: {root_id}")
            cards = []
            try:
                cards = await client.get_cards_list(api_key=company.api_key, root_id=root_id)
            except AuthorizationError as e:
                raise e
            except RootIDError as e:
                errors.append(f"Company Name={company.name}, Company ID={company.company_id}: Error={e}")

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
            print("Пропущена карточка без API-ключа")
            continue
        grouped_cards[api_key].append(card)

    for api_key, card_list in grouped_cards.items():
        print(f"\nОтправка карточек для api_key: {api_key}, всего: {len(card_list)}")

        batches = split_into_batches(card_list, BATCH_LIMIT)
        for idx, batch in enumerate(batches, start=1):
            for card in batch:
                card.pop("api_key", None)

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