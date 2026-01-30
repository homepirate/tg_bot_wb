import asyncio
import random
import json

import aiohttp
from aiohttp import ClientTimeout, ClientConnectionError

from config import Config
from errors import AuthorizationError, RootIDError, UpdateCardsError
from utils.helpers_rate import HostRateLimiter


class WBClientAPI:
    def __init__(self):
        self.api_base_url = Config.API_URL
        self.catalog_base_url = Config.CATALOG_URL

        self.timeout = ClientTimeout(total=40, connect=10, sock_read=30)
        self.max_retries = 15
        self.retry_delay = 2  # базовый backoff

        # ОДНА сессия на весь класс + один коннектор (и нормальное закрытие)
        self._connector: aiohttp.TCPConnector | None = None
        self._session: aiohttp.ClientSession | None = None

        # лимитер на весь класс
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
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        """
        Гарантированно закрывает session/connector, чтобы не было:
        Unclosed client session / Unclosed connector
        """
        if self._session is not None:
            await self._session.close()
            self._session = None

        if self._connector is not None and not self._connector.closed:
            await self._connector.close()
            self._connector = None

    async def _ensure_session(self):
        """
        Инициализирует session/connector один раз.
        """
        if self._session is None or self._session.closed:
            if self._connector is None or self._connector.closed:
                self._connector = aiohttp.TCPConnector(limit=10, limit_per_host=2, ttl_dns_cache=300)

            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                connector=self._connector,
                headers=self._default_headers,
            )

    @property
    def session(self) -> aiohttp.ClientSession:
        """
        Не создаём session лениво без await — иначе её легко забыть закрыть.
        Требуем использования: async with WBClientAPI() as api: ...
        """
        if not self._session or self._session.closed:
            raise RuntimeError(
                "ClientSession is not initialized. Use 'async with WBClientAPI()' "
                "or call 'await api._ensure_session()' then 'await api.close()'."
            )
        return self._session

    async def _get_with_retries(self, url: str, *, referer: str | None = None) -> dict | None:
        await self._ensure_session()

        headers = {}
        if referer:
            headers["Referer"] = referer

        for attempt in range(1, self.max_retries + 1):
            try:
                # Если HostRateLimiter поддерживает async context manager — норм.
                # Если у тебя другая реализация, убери блок async with и просто делай get.
                try:
                    async with self._limiter.limit(url):
                        resp = await self.session.get(url, headers=headers)
                except AttributeError:
                    resp = await self.session.get(url, headers=headers)

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
                        # иногда отдают HTML антибот
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
        await self._ensure_session()

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
            if not data:
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
        await self._ensure_session()

        url = f"{self.api_base_url}/content/v2/get/cards/list"
        headers = {"Authorization": api_key, "Content-Type": "application/json"}

        payload = {
            "settings": {
                "cursor": {"limit": 100},
                "filter": {"withPhoto": -1, "imtID": root_id},
            }
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                async with self.session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("cards", [])

                    if response.status == 401:
                        print("❌ Ошибка авторизации (401): Неверный или просроченный токен.")
                        raise AuthorizationError("Неверный токен (401)")

                    if response.status == 429:
                        print(f"⏳ Превышен лимит запросов (429). Попытка {attempt}/{self.max_retries}")
                        await asyncio.sleep(self.retry_delay * attempt)
                        continue

                    if response.status >= 500:
                        text = await response.text()
                        print(f"❌ root_id={root_id} — ошибка {response.status}: {text.strip()}")
                        return []

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
        await self._ensure_session()

        url = f"{self.api_base_url}/content/v2/cards/update"
        headers = {"Authorization": f"{api_key}", "Content-Type": "application/json"}

        payload = cards
        last_response_json: dict = {}

        for attempt in range(1, self.max_retries + 1):
            try:
                async with self.session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        print(f"Карточки успешно обновлены. Кол-во: {len(cards)}")
                        return True, await response.json()

                    if response.status == 401:
                        print("Ошибка авторизации (401): Неверный или просроченный токен.")
                        raise AuthorizationError("Неверный токен (401)")

                    if response.status == 429:
                        print(f"Превышен лимит запросов (429). Попытка {attempt}/{self.max_retries}.")
                        await asyncio.sleep(self.retry_delay * attempt)
                        continue

                    text = await response.text()
                    msg = f"Ошибка отправки карточек {response.status}: {text}"
                    raise UpdateCardsError(msg)

            except (asyncio.TimeoutError, ClientConnectionError) as e:
                print(f"Попытка {attempt}/{self.max_retries} — ошибка соединения: {e}")
                if attempt == self.max_retries:
                    raise UpdateCardsError("Превышено число попыток отправки карточек")
                await asyncio.sleep(self.retry_delay * attempt)

        return False, last_response_json

    async def get_filters_by_supplier(self, supplier_id: int) -> dict:
        """
        Запрашивает настройки фильтров каталога WB для указанного поставщика.
        """
        await self._ensure_session()

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
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.json()

                    text = await response.text()
                    print(f"⚠️ Ошибка {response.status} при запросе фильтров: {text}")

                    if response.status in (408, 425, 429, 500, 502, 503, 504):
                        await asyncio.sleep(self.retry_delay * attempt)
                        continue

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
        await self._ensure_session()

        all_products: list[dict] = []
        page = 1
        fbrand = ";".join(map(str, wb_brand_ids)) if wb_brand_ids else None

        while True:
            url = (
                f"{self.catalog_base_url}/sellers/v4/catalog"
                f"?ab_testing=false&appType=1&curr=rub&dest=-1257786"
                f"&hide_dtype=13;14&lang=ru&page={page}&sort=popular&spp=30"
                f"&supplier={company_id}"
            )
            if fbrand:
                url += f"&fbrand={fbrand}"

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

    def _is_html_block(self, text: str, content_type: str | None) -> bool:
        if content_type and "application/json" in (content_type or "").lower():
            return False
        t = text.strip().lower()
        return t.startswith("<!doctype html") or t.startswith("<html")
