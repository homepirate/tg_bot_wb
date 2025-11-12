import asyncio
import random

from aiohttp import ClientTimeout, ClientConnectionError

from config import Config
import aiohttp

from errors import AuthorizationError, RootIDError, UpdateCardsError
from utils.helpers_rate import HostRateLimiter, parse_retry_after


class WBClientAPI:
    def __init__(self):
        self.api_base_url = Config.API_URL
        self.catalog_base_url = Config.CATALOG_URL
        self.timeout = ClientTimeout(total=40, connect=10, sock_read=30)
        self.max_retries = 15
        self.retry_delay = 2  # базовый backoff

        # общий session и лимитер на весь класс
        self._connector = aiohttp.TCPConnector(limit=10, limit_per_host=2, ttl_dns_cache=300)
        self._session: aiohttp.ClientSession | None = None
        self._limiter = HostRateLimiter(max_concurrent=2, base_min_interval=0.5, max_min_interval=2.5)

        self._default_headers = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://www.wildberries.ru",
            "Connection": "keep-alive",
        }

    async def __aenter__(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                connector=self._connector,
                headers=self._default_headers,
            )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session:
            await self._session.close()
            self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if not self._session:
            # на случай прямого вызова без контекста
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                connector=self._connector,
                headers=self._default_headers,
            )
        return self._session

    async def _get_with_retries(self, url: str, *, referer: str | None = None) -> dict | None:
        session = self.session
        headers = {}
        if referer:
            headers["Referer"] = referer

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = await session.get(url, headers=headers)
                async with resp:
                    ct = resp.headers.get("Content-Type", "")
                    # 498 или HTML-заглушка → долгий сон
                    if resp.status == 498:
                        text = (await resp.text())[:200]
                        print(f"🛑 498 anti-bot for {url}: {text[:80]}...")
                        delay = min(15 * attempt, 90) + random.random()
                        await asyncio.sleep(delay)
                        continue

                    if resp.status == 200:
                        # если это HTML — антибот
                        peek = await resp.text()
                        if self._is_html_block(peek, ct):
                            print(f"🧱 Anti-bot HTML for {url} (CT={ct or 'n/a'})")
                            delay = min(15 * attempt, 90) + random.random()
                            await asyncio.sleep(delay)
                            continue
                        # это JSON или текст JSON
                        try:
                            return await resp.json()
                        except Exception:
                            # вдруг отдали текст JSON без header
                            import json
                            return json.loads(peek)

                    if resp.status == 429:
                        ra = resp.headers.get("Retry-After")
                        if ra and ra.isdigit():
                            delay = float(ra)
                        else:
                            delay = min(5 * attempt, 90) + random.random()
                        print(f"⏳ 429 for {url} → sleep {delay:.1f}s")
                        await asyncio.sleep(delay)
                        continue

                    if resp.status in (408, 425, 500, 502, 503, 504):
                        delay = min(5 * attempt, 60) + random.random()
                        print(f"⚠️ {resp.status} for {url} → retry in {delay:.1f}s")
                        await asyncio.sleep(delay)
                        continue

                    text = (await resp.text())[:300]
                    print(f"❌ {resp.status} for {url}: {text}")
                    return None

            except (asyncio.TimeoutError, ClientConnectionError) as e:
                if attempt == self.max_retries:
                    print(f"❌ exhausted for {url}: {e}")
                    return None
                delay = min(5 * attempt, 60) + random.random()
                print(f"⏱️ {e} → retry in {delay:.1f}s")
                await asyncio.sleep(delay)

        return None

    async def get_all_data_by_company_id(self, company_id: int) -> list[dict]:
        """
        Пагинация по каталогу WB с ретраями и паузами при 429/5xx.
        """
        all_products: list[dict] = []
        page = 1

        while True:
            url = (
                f"{self.catalog_base_url}/sellers/v4/catalog"
                f"?ab_testing=false&appType=1&curr=rub&dest=-1257786"
                f"&hide_dtype=13;14&lang=ru&page={page}&sort=popular&spp=30"
                f"&supplier={company_id}"
            )

            data = await self._get_with_retries(url)
            if not data:  # ошибка после ретраев
                break

            products = data.get("products", [])
            if not products:
                break

            all_products.extend(products)
            page += 1

            await asyncio.sleep(0.2)

        if not all_products:
            print(f"🔁 Фолбэк на https://www.wildberries.ru/__internal/u-catalog для company_id={company_id}")
            page = 1
            while True:
                url = (
                    f"https://www.wildberries.ru/__internal/u-catalog/sellers/v4/catalog"
                    f"?ab_testing=false&appType=1&curr=rub&dest=-1257786"
                    f"&hide_dtype=11&lang=ru&page={page}&sort=popular&spp=30"
                    f"&supplier={company_id}"
                )

                data = await self._get_with_retries(url)
                if not data:
                    break

                products = data.get("products", [])
                if not products:
                    break

                all_products.extend(products)
                page += 1
                await asyncio.sleep(0.2)

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
        fbrand = ";".join(map(str, wb_brand_ids)) if wb_brand_ids else None

        async with aiohttp.ClientSession() as session:
            while True:
                url = (
                    f"{self.catalog_base_url}/sellers/v4/catalog"
                    f"?ab_testing=false&appType=1&curr=rub&dest=-1257786"
                    f"&hide_dtype=13;14&lang=ru&page={page}&sort=popular&spp=30"
                    f"&supplier={company_id}"
                )
                if fbrand:  # добавляем только если есть бренды
                    url += f"&fbrand={fbrand}"

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

    def _is_html_block(self, text: str, content_type: str | None) -> bool:
        if content_type and "application/json" in content_type.lower():
            return False
        t = text.strip().lower()
        return t.startswith("<!doctype html") or t.startswith("<html")