import asyncio
import json
from collections import defaultdict
from typing import Any

from api_client import WBClientAPI
from config import config
from errors import AuthorizationError, RootIDError, UpdateCardsError
from services.company_service import get_sorted_companies, get_companies_with_nomenclature, get_company_by_api_key, \
    get_all_companies, get_company_by_api_key_safe
from utils.core_utils import split_into_batches, is_weekend, filter_card_top_level
from services.brand_service import get_night_brands, get_night_brand_wbids, get_all_brand_wbids_except_default, \
    is_night_brand

REQUEST_DELAY_ONE_SECOND = 1
REQUEST_DELAY_SIX_SECONDS = 6
BATCH_LIMIT = 3000

async def run_all_from(*, weekend_override: bool | None = None) -> list[str]:
    error_send: list[str] = []

    # определяем режим
    if weekend_override is None:
        weekend = await is_weekend()
    else:
        weekend = weekend_override

    if weekend:
        print("Сегодня выходной (или выбран режим выходных) — применяем ночную логику брендов.")
    else:
        print("Сегодня будний (или выбран режим будних) — бренды приводим к default_brand.")

    all_cards = await process_cards()

    updated_cards, tg_messages = await process_brands(all_cards, weekend)
    if tg_messages:
        error_send.append("Неизменившиеся  каточки:")
        error_send.extend(tg_messages)

    prepared_cards: list[dict[str, Any]] = []
    for card in (updated_cards or []):
        api_key = card.get("api_key")
        if not api_key:
            print("run_all_from Пропущена карточка без API-ключа")
            continue
        payload = filter_card_top_level(card)
        payload["api_key"] = api_key
        prepared_cards.append(payload)

    error_send_card = await send_cards(prepared_cards)
    if error_send_card:
        error_send.append("Ошибки:")
        error_send.extend(error_send_card)

    await asyncio.sleep(10)

    async with config.AsyncSessionLocal() as session:
        companies = await get_all_companies(session)


    for company in companies:
        async with config.AsyncSessionLocal() as session:
            if weekend:
                wb_brand_ids = await get_night_brand_wbids(
                    session, company.id, company.default_brand.name
                )
            else:
                # wb_brand_ids = await get_all_brand_wbids_except_default(
                #     session, company.id, company.default_brand.name
                # )
                wb_brand_ids = await get_all_brand_wbids_except_default(
                    session, company.default_brand.name
                )

        if not wb_brand_ids:
            print(f"⛔️ Нет брендов для компании {company.name}")
            continue
        async with WBClientAPI() as api:
            products = await api.get_all_data_by_company_id_and_brands(company.id, wb_brand_ids)
        print(f"📦 {len(products)} товаров найдено для компании {company.name}")

        product_root_ids = {p.get("root") for p in products if p.get("root")}
        retry_cards = [card for card in (updated_cards or []) if card.get("root") in product_root_ids]
        if not retry_cards:
            continue

        failed_cards: list[dict[str, Any]] = []

        for card in retry_cards:
            root_id = card.get("root_id") or card.get("root")
            try:
                updated_cards, tg_messages = await process_brands(all_cards, weekend)  # <-- используем тот же weekend
                print(f"🔁 Повторно обработан бренд для root_id={root_id}")
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

                resend_errors = await send_cards(retry_prepared)
                if resend_errors:
                    error_send.append("Ошибки при повторной отправке:")
                    error_send.extend(resend_errors)

            except Exception as e:
                print(f"❌ Ошибка повторной обработки бренда root_id={root_id}: {e}")
                failed_cards.append(card)

        if failed_cards:
            messages = [
                f"❗️ Не удалось обработать бренд для root_id {card.get('root_id') or card.get('root')} (API ключ: {card.get('api_key', '—')})"
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
    error_send = await send_cards(prepared_cards)
    if error_send:
        errors.extend(error_send)
    return errors


async def process_cards():
    """
    Тянем карточки по компаниям/номенклатурам.
    Записываем в карточку:
      - api_key (для отправки)
      - root
      - company_id
      - original_brand (из номенклатуры — это важно для выходных)
    """
    client = WBClientAPI()
    all_cards: list[dict] = []

    async with config.AsyncSessionLocal() as session:
        companies = await get_companies_with_nomenclature(session)

    for company in companies:
        print(f"\n🔍 Компания: {company.name}")
        api_key = company.api_key

        seen_root_ids = set()

        for nom in company.nomenclatures:
            try:
                root_id = int(nom.root_id)
            except (TypeError, ValueError):
                print(f"Пропущен некорректный root_id: {nom.root_id}")
                continue

            if root_id in seen_root_ids:
                print(f"⏩ Пропущен дубликат root_id: {root_id}")
                continue

            seen_root_ids.add(root_id)

            try:
                async with WBClientAPI() as api:
                    cards = await api.get_cards_list(api_key=api_key, root_id=root_id)
            except Exception as e:
                print(e)
                return []

            for card in cards:
                card["root"] = card.get("imtID")
                card["api_key"] = company.api_key
                card["company_id"] = company.id
                card["original_brand"] = nom.original_brand or ""
                all_cards.append(card)

            print(f"📦 Получено карточек для root_id={root_id}: {len(cards)}")

            await asyncio.sleep(REQUEST_DELAY_ONE_SECOND)

    return all_cards

async def process_brands(all_cards: list[dict], weekend: bool) -> tuple[list[dict], list[str]]:
    """
    Будни (weekend=False): всегда меняем бренд на default_brand, если отличается.
    Выходной (weekend=True): берём текущий бренд карточки; если он ночной для company -> меняем на default_brand,
                              иначе не трогаем.
    """
    updated: list[dict] = []
    msgs: list[str] = []
    seen: set[str] = set()

    # одна сессия на все проверки ночных брендов (быстро и стабильно)
    async with config.AsyncSessionLocal() as session:
        companies_cache: dict[str, Any] = {}
        night_cache: dict[tuple[int, str], bool] = {}

        for card in all_cards:
            api_key = card.get("api_key")
            company_id = card.get("company_id")
            current_brand = (card.get("brand") or "").strip()
            root_id = card.get("root") or "?"

            if not api_key or not company_id:
                continue

            # безопасная обёртка: берём компанию через фабрику сессий (а не через открытую сессию!)
            company = companies_cache.get(api_key)
            if company is None:
                company = await get_company_by_api_key_safe(config.AsyncSessionLocal, api_key)
                companies_cache[api_key] = company

            if not company or not company.default_brand:
                continue

            default_brand = company.default_brand.name

            if not weekend:
                # Будний день — всегда приводим к базовому бренду
                if current_brand != default_brand:
                    card["brand"] = default_brand
                    updated.append(card)
                else:
                    m = f"🔸 RootID {root_id}: бренд уже {default_brand}"
                    if m not in seen:
                        msgs.append(m); seen.add(m)
            else:
                # Выходной — меняем только если текущий бренд ночной для этой компании
                key = (company_id, current_brand)
                is_night = night_cache.get(key)
                if is_night is None:
                    is_night = await is_night_brand(session, company_id, current_brand)
                    night_cache[key] = is_night

                if is_night and current_brand != default_brand:
                    card["brand"] = default_brand
                    updated.append(card)
                elif not is_night:
                    m = f"🔸 RootID {root_id}: '{current_brand}' не ночной — без изменений"
                    if m not in seen:
                        msgs.append(m); seen.add(m)

    return updated, msgs

async def get_all_product_from_catalog() -> list[dict]:
    all_products = []
    companies = []

    async with config.AsyncSessionLocal() as session:
        companies = await get_sorted_companies(session)

    for company in companies:
        print(f"Обработка компании: {company.name} (ID: {company.company_id})")

        async with WBClientAPI() as api:
            company_products = await api.get_all_data_by_company_id(company.company_id)
        print(f" Найдено товаров: {len(company_products)}")

        for product in company_products:
            product["company_id"] = company.company_id

        all_products.extend(company_products)

    print(f"Всего собрано товаров: {len(all_products)}")
    return all_products


async def get_and_update_brand_in_card(available_root_ids: list) -> tuple[list[dict], list[str]]:
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
                async with WBClientAPI() as api:
                    cards = await api.get_cards_list(api_key=company.api_key, root_id=root_id)
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

    print(f"Карточки для отправки: {len(grouped_cards)}")

    for api_key, card_list in grouped_cards.items():
        print(f"\nОтправка карточек для api_key: {api_key}, всего: {len(card_list)}")

        batches = split_into_batches(card_list, BATCH_LIMIT)
        for idx, batch in enumerate(batches, start=1):

            print(f"Отправка батча {idx}/{len(batches)} ({len(batch)} карточек)...")

            try:
                async with WBClientAPI() as api:
                    success, response = await api.update_cards(api_key=api_key, cards=batch)
                errors.append("Ответ от сервера WB:")
                errors.append(json.dumps(response, ensure_ascii=False, indent=2))
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