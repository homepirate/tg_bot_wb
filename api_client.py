import asyncio
from typing import Any, Coroutine

from aiohttp import ClientTimeout, ClientConnectionError, ClientResponse
from aiohttp.web_response import Response

from config import Config
import aiohttp

from errors import AuthorizationError, RootIDError, UpdateCardsError


class WBClientAPI:
    def __init__(self):
        self.api_base_url = Config.API_URL
        self.catalog_base_url = Config.CATALOG_URL

        self.timeout = ClientTimeout(total=30)
        self.max_retries = 5
        self.retry_delay = 2

    async def get_all_data_by_company_id(self, company_id: int) -> list[dict]:
        all_products = []
        page = 1

        async with aiohttp.ClientSession() as session:
            while True:
                url = f"{self.catalog_base_url}/sellers/v4/catalog?ab_testing=false&appType=1&curr=rub&dest=-1257786&hide_dtype=13;14&lang=ru&page={page}&sort=popular&spp=30&supplier={company_id}"
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"⚠️ Ошибка запроса: {response.status}")
                        break

                    data = await response.json()
                    products = data.get("products", [])

                    if not products:
                        break

                    all_products.extend(products)
                    page += 1

        return all_products

    async def get_cards_list(self, api_key: str, root_id: int) -> list[dict]:
        """
        Получает список карточек по API-ключу и root_id.
        """
        url = f"{self.api_base_url}/content/v2/get/cards/list"
        headers = {
            "Authorization": api_key,
            "Content-Type": "application/json"
        }

        payload = {
            "settings": {
                "cursor": {
                    "limit": 100
                },
                "filter": {
                    "withPhoto": -1,
                    "imtID": root_id
                }
            }
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.post(url, headers=headers, json=payload) as response:
                        if response.status == 200:
                            data = await response.json()
                            return data.get("cards", [])

                        elif response.status == 401:
                            print("❌ Ошибка авторизации (401): Неверный или просроченный токен.")
                            raise AuthorizationError("Неверный токен (401)")

                        elif response.status == 429:
                            print(f"⏳ Превышен лимит запросов (429). Попытка {attempt}/{self.max_retries}")
                            await asyncio.sleep(self.retry_delay * attempt)

                        elif response.status >= 500:
                            # WB серверная ошибка — логируем, пропускаем root_id
                            text = await response.text()
                            print(f"❌ root_id={root_id} — ошибка {response.status}: {text.strip()}")
                            return []

                        else:
                            text = await response.text()
                            msg = f"root_id={root_id} ошибка {response.status}: {text.strip()}"
                            raise RootIDError(msg)

            except (asyncio.TimeoutError, ClientConnectionError) as e:
                print(f"⏱️ Попытка {attempt}/{self.max_retries} — таймаут: {e}")
                if attempt == self.max_retries:
                    print(f"❌ root_id={root_id}: превышено число повторов. Пропускаем.")
                    return []
                await asyncio.sleep(self.retry_delay * attempt)

        return []

    async def update_cards(self, api_key: str, cards: list[dict]) -> tuple[bool, dict]:
        url = f"{self.api_base_url}/content/v2/cards/update"
        headers = {
            "Authorization": f"{api_key}",
            "Content-Type": "application/json"
        }
        response = None
        payload = cards

        for attempt in range(1, self.max_retries + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json=payload) as response:
                        if response.status == 200:
                            print(f"Карточки успешно обновлены. Кол-во: {len(cards)}")
                            return True, await response.json()
                        elif response.status == 401:
                            print("Ошибка авторизации (401): Неверный или просроченный токен.")
                            raise AuthorizationError("Неверный токен (401)")
                        elif response.status == 429:
                            print(f"Превышен лимит запросов (429). Попытка {attempt}/{self.max_retries}.")
                            await asyncio.sleep(self.retry_delay * attempt)
                        else:
                            text = await response.text()
                            msg = f"Ошибка отправки карточек {response.status}: {text}"
                            raise UpdateCardsError(msg)
            except (asyncio.TimeoutError, ClientConnectionError) as e:
                print(f"Попытка {attempt}/{self.max_retries} — ошибка соединения: {e}")
                if attempt == self.max_retries:
                    raise UpdateCardsError("Превышено число попыток отправки карточек")
                await asyncio.sleep(self.retry_delay * attempt)

        return False, await response.json()


    async def get_filters_by_supplier(self, supplier_id: int) -> dict:
        """
        Запрашивает настройки фильтров каталога WB для указанного поставщика.

        :param supplier_id: идентификатор поставщика
        :return: словарь с данными фильтров (как вернул API)
        """
        # Собираем URL с подстановкой supplier_id
        url = (
            f"{self.catalog_base_url}/sellers/v8/filters"
            f"?ab_testing=false"
            f"&appType=1"
            f"&curr=rub"
            f"&dest=-1257786"
            f"&fbrand=21;310421867"
            f"&hide_dtype=13;14"
            f"&lang=ru"
            f"&spp=30"
            f"&supplier={supplier_id}"
        )

        for attempt in range(1, self.max_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            return await response.json()
                        else:
                            text = await response.text()
                            print(f"⚠️ Ошибка {response.status} при запросе фильтров: {text}")
                            # Можно настроить более точную обработку ошибок по коду
                            break
            except (asyncio.TimeoutError, ClientConnectionError) as e:
                print(f"⏱️ Попытка {attempt}/{self.max_retries} — ошибка соединения: {e}")
                if attempt == self.max_retries:
                    print("❌ Превышено число попыток. Возвращаем пустой словарь.")
                    return {}
                await asyncio.sleep(self.retry_delay * attempt)

        return {}

    async def get_all_data_by_company_id_and_brands(self, company_id: int, wb_brand_ids: list[int]) -> list[dict]:
        """
        Получает все товары компании с заданными брендами из WB API.
        """
        all_products = []
        page = 1
        fbrand = ';'.join(map(str, wb_brand_ids))

        async with aiohttp.ClientSession() as session:
            while True:
                url = (
                    f"{self.catalog_base_url}/sellers/v4/catalog"
                    f"?ab_testing=false&appType=1&curr=rub&dest=-1257786"
                    f"&hide_dtype=13;14&lang=ru&page={page}&sort=popular&spp=30"
                    f"&supplier={company_id}&fbrand={fbrand}"
                )
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"⚠️ Ошибка запроса: {response.status}")
                        break

                    data = await response.json()
                    products = data.get("products", [])
                    if not products:
                        break

                    all_products.extend(products)
                    page += 1

        return all_products