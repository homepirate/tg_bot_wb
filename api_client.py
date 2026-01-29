import asyncio
import random
import json
from typing import Any

import aiohttp
from aiohttp import ClientTimeout, ClientConnectionError

from config import Config
from errors import AuthorizationError, RootIDError, UpdateCardsError
from utils.helpers_rate import parse_retry_after


class WBClientAPI:
    def __init__(self):
        self.api_base_url = Config.API_URL
        self.catalog_base_url = Config.CATALOG_URL

        self.timeout = ClientTimeout(total=40, connect=10, sock_read=30)
        self.max_retries = 15
        self.retry_delay = 2  # базовый backoff

        self._default_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://www.wildberries.ru",
            "Connection": "keep-alive",
        }

    # ----------------------------
    # Core request helper (NEW SESSION each call)
    # ----------------------------
    async def _request_json_with_retries(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: Any | None = None,
        referer: str | None = None,
        expect_json: bool = True,
        allow_html_antibot: bool = False,
    ) -> Any | None:
        """
        Делает HTTP-запрос с ретраями. На каждый запрос создаётся новая ClientSession (как ты хочешь).
        Возвращает распарсенный JSON (или None при неуспехе/необрабатываемых кодах).
        """
        req_headers: dict[str, str] = dict(self._default_headers)
        if headers:
            req_headers.update(headers)
        if referer:
            req_headers["Referer"] = referer

        for attempt in range(1, self.max_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout, headers=req_headers) as session:
                    async with session.request(method, url, json=json_body) as resp:
                        ct = resp.headers.get("Content-Type", "")
                        status = resp.status

                        # 498 / антибот
                        if status == 498:
                            text = (await resp.text())[:200]
                            print(f"🛑 498 anti-bot for {url}: {text[:80]}...")
                            delay = min(15 * attempt, 90) + random.random()
                            await asyncio.sleep(delay)
                            continue

                        # OK
                        if status == 200:
                            if not expect_json:
                                return await resp.text()

                            # иногда WB шлёт HTML заглушку
                            raw = await resp.text()
                            if self._is_html_block(raw, ct) and not allow_html_antibot:
                                print(f"🧱 Anti-bot HTML for {url} (CT={ct or 'n/a'})")
                                delay = min(15 * attempt, 90) + random.random()
                                await asyncio.sleep(delay)
                                continue

                            # JSON
                            try:
                                return json.loads(raw)
                            except Exception:
                                # если всё-таки корректный json по header — попробуем стандартный парсер
                                try:
                                    return await resp.json()
                                except Exception as e:
                                    print(f"❌ JSON parse error for {url}: {e}")
                                    return None

                        # Rate limit
                        if status == 429:
                            ra = resp.headers.get("Retry-After")
                            delay = None
                            if ra:
                                # если у тебя parse_retry_after умеет "1.5" или даты — ок
                                try:
                                    delay = float(parse_retry_after(ra))  # может вернуть float/int
                                except Exception:
                                    delay = float(ra) if ra.isdigit() else None

                            if delay is None:
                                delay = min(5 * attempt, 90) + random.random()

                            print(f"⏳ 429 for {url} → sleep {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue

                        # временные/серверные
                        if status in (408, 425, 500, 502, 503, 504):
                            delay = min(5 * attempt, 60) + random.random()
                            print(f"⚠️ {status} for {url} → retry in {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue

                        # остальное — читаем текст и выходим
                        text = (await resp.text())[:300]
                        print(f"❌ {status} for {url}: {text}")
                        return None

            except (asyncio.TimeoutError, ClientConnectionError) as e:
                if attempt == self.max_retries:
                    print(f"❌ exhausted for {url}: {e}")
                    return None
                delay = min(5 * attempt, 60) + random.random()
                print(f"⏱️ {e} → retry in {delay:.1f}s")
                await asyncio.sleep(delay)

        return None

    # ----------------------------
    # Catalog methods
    # ----------------------------
    async def get_all_data_by_company_id(self, company_id: int) -> list[dict]:
        all_products: list[dict] = []
        page = 1

        while True:
            url = (
                f"{self.catalog_base_url}/sellers/v4/catalog"
                f"?ab_testing=false&appType=1&curr=rub&dest=-1257786"
                f"&hide_dtype=13;14&lang=ru&page={page}&sort=popular&spp=30"
                f"&supplier={company_id}"
            )

            data = await self._request_json_with_retries("GET", url)
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
                    "https://www.wildberries.ru/__internal/u-catalog/sellers/v4/catalog"
                    f"?ab_testing=false&appType=1&curr=rub&dest=-1257786"
                    f"&hide_dtype=11&lang=ru&page={page}&sort=popular&spp=30"
                    f"&supplier={company_id}"
                )

                data = await self._request_json_with_retries("GET", url)
                if not data:
                    break

                products = data.get("products", [])
                if not products:
                    break

                all_products.extend(products)
                page += 1
                await asyncio.sleep(0.2)

        return all_products

    async def get_all_data_by_company_id_and_brands(self, company_id: int, wb_brand_ids: list[int]) -> list[dict]:
        all_products: list[dict] = []
        page = 1
        fbrand = ";".join(map(str, wb_brand_ids)) if wb_brand_ids else ""

        while True:
            url = (
                f"{self.catalog_base_url}/sellers/v4/catalog"
                f"?ab_testing=false&appType=1&curr=rub&dest=-1257786"
                f"&hide_dtype=13;14&lang=ru&page={page}&sort=popular&spp=30"
                f"&supplier={company_id}"
            )
            if fbrand:
                url += f"&fbrand={fbrand}"

            data = await self._request_json_with_retries("GET", url)
            if not data:
                break

            products = data.get("products", [])
            if not products:
                break

            all_products.extend(products)
            page += 1
            await asyncio.sleep(0.2)

        return all_products

    async def get_filters_by_supplier(self, supplier_id: int) -> dict:
        url = (
            f"{self.catalog_base_url}/sellers/v8/filters"
            f"?ab_testing=false&appType=1&curr=rub&dest=-1257786"
            f"&fbrand=21;310421867&hide_dtype=13;14&lang=ru&spp=30"
            f"&supplier={supplier_id}"
        )

        data = await self._request_json_with_retries("GET", url)
        return data or {}

    # ----------------------------
    # Content API methods
    # ----------------------------
    async def get_cards_list(self, api_key: str, root_id: int) -> list[dict]:
        url = f"{self.api_base_url}/content/v2/get/cards/list"
        headers = {
            "Authorization": api_key,
            "Content-Type": "application/json",
        }
        payload = {
            "settings": {
                "cursor": {"limit": 100},
                "filter": {"withPhoto": -1, "imtID": root_id},
            }
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout, headers=headers) as session:
                    async with session.post(url, json=payload) as response:
                        if response.status == 200:
                            data = await response.json()
                            return data.get("cards", [])

                        if response.status == 401:
                            raise AuthorizationError("Неверный токен (401)")

                        if response.status == 429:
                            ra = response.headers.get("Retry-After")
                            delay = float(parse_retry_after(ra)) if ra else (self.retry_delay * attempt)
                            print(f"⏳ 429 (get_cards_list) root_id={root_id} → sleep {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue

                        if response.status >= 500:
                            text = await response.text()
                            print(f"❌ root_id={root_id} — ошибка {response.status}: {text.strip()}")
                            return []

                        text = await response.text()
                        raise RootIDError(f"root_id={root_id} ошибка {response.status}: {text.strip()}")

            except (asyncio.TimeoutError, ClientConnectionError) as e:
                print(f"⏱️ Попытка {attempt}/{self.max_retries} — таймаут/соединение: {e}")
                if attempt == self.max_retries:
                    print(f"❌ root_id={root_id}: превышено число повторов. Пропускаем.")
                    return []
                await asyncio.sleep(self.retry_delay * attempt)

        return []

    async def update_cards(self, api_key: str, cards: list[dict]) -> tuple[bool, dict]:
        url = f"{self.api_base_url}/content/v2/cards/update"
        headers = {
            "Authorization": api_key,
            "Content-Type": "application/json",
        }

        last_payload: dict = {}

        for attempt in range(1, self.max_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout, headers=headers) as session:
                    async with session.post(url, json=cards) as response:
                        if response.status == 200:
                            data = await response.json()
                            print(f"Карточки успешно обновлены. Кол-во: {len(cards)}")
                            return True, data

                        if response.status == 401:
                            raise AuthorizationError("Неверный токен (401)")

                        if response.status == 429:
                            ra = response.headers.get("Retry-After")
                            delay = float(parse_retry_after(ra)) if ra else (self.retry_delay * attempt)
                            print(f"⏳ 429 (update_cards) → sleep {delay:.1f}s")
                            await asyncio.sleep(delay)
                            continue

                        text = await response.text()
                        raise UpdateCardsError(f"Ошибка отправки карточек {response.status}: {text}")

            except (asyncio.TimeoutError, ClientConnectionError) as e:
                print(f"Попытка {attempt}/{self.max_retries} — ошибка соединения: {e}")
                if attempt == self.max_retries:
                    raise UpdateCardsError(f"Превышено число попыток отправки карточек: {e}")
                await asyncio.sleep(self.retry_delay * attempt)

            except Exception as e:
                # чтобы last_payload не был пустым при отладке
                last_payload = {"error": str(e)}
                raise

        return False, last_payload

    # ----------------------------
    # Helpers
    # ----------------------------
    def _is_html_block(self, text: str, content_type: str | None) -> bool:
        if content_type and "application/json" in (content_type or "").lower():
            return False
        t = (text or "").strip().lower()
        return t.startswith("<!doctype html") or t.startswith("<html")
