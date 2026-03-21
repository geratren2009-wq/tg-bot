import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import List, Dict, Set
from urllib.parse import urljoin, quote

import aiohttp
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ============================================================
# НАСТРОЙКИ
# ============================================================
BOT_TOKEN = "8435777682:AAESaC5frCxhbIECaSBPEMUQhzn7LYXCULo"
TARGET_CHAT_ID = ""
CHECK_EVERY_MINUTES = 20
MAX_NEW_PER_STORE = 5
STATE_FILE = "seen_deals.json"

# Партнерське посилання Telegram / Stars
TELEGRAM_PARTNER_URL = "https://t.me/BuyVPN_Global_bot?start=_tgr_1lp79ZIwODli"

# ============================================================
# АВТОВСТАВКА ПАРТНЁРКИ В КАЖДУЮ АКЦИЮ
# ============================================================
# Вариант 1: один общий редирект для всех магазинов
# Пример:
# GLOBAL_REDIRECT = "https://yourdomain.com/go?url={url}"
GLOBAL_REDIRECT = ""

# Вариант 2: отдельная ссылка/шаблон под каждый магазин
# {url} будет автоматически заменяться на ссылку акции
#
# Примеры:
# "eva": "https://yourdomain.com/go?store=eva&url={url}"
# "rozetka": "https://ad.admitad.com/g/xxxx/?ulp={url}"
# "prostor": "https://yourdomain.com/go?store=prostor&url={url}"
STORE_AFFILIATE_TEMPLATES = {
    "eva": "",
    "prostor": "",
    "rozetka": "",
}

# Добавлять user_id как subid в редирект
ADD_SUBID = True

# ============================================================
# ИСТОЧНИКИ
# ============================================================
STORES = {
    "eva": {
        "title": "EVA",
        "url": "https://eva.ua/ua/promotion/",
    },
    "prostor": {
        "title": "PROSTOR",
        "url": "https://prostor.ua/ua/aktsii/",
    },
    "rozetka": {
        "title": "ROZETKA",
        "url": "https://rozetka.com.ua/ua/news-articles-promotions/promotions/",
    },
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


@dataclass
class Deal:
    store_key: str
    store_name: str
    title: str
    url: str
    image: str = ""


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def absolute_url(base: str, href: str) -> str:
    return urljoin(base, href)


def extract_image_from_tag(tag, base_url: str) -> str:
    if not tag:
        return ""

    img = tag.find("img")
    if img:
        for attr in ("src", "data-src", "data-lazy", "data-original"):
            value = img.get(attr)
            if value:
                return absolute_url(base_url, value)

        srcset = img.get("srcset")
        if srcset:
            first = srcset.split(",")[0].strip().split(" ")[0]
            return absolute_url(base_url, first)

    style = tag.get("style", "")
    m = re.search(r'url\(["\']?(.*?)["\']?\)', style)
    if m:
        return absolute_url(base_url, m.group(1))

    return ""


def unique_deals(items: List[Deal]) -> List[Deal]:
    seen = set()
    out = []
    for item in items:
        key = (item.title.strip().lower(), item.url.strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def load_seen() -> Dict[str, List[str]]:
    if not os.path.exists(STATE_FILE):
        return {key: [] for key in STORES.keys()}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for key in STORES.keys():
            data.setdefault(key, [])
        return data
    except Exception:
        return {key: [] for key in STORES.keys()}


def save_seen(data: Dict[str, List[str]]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def build_affiliate_url(store_key: str, raw_url: str, user_id: int | None = None) -> str:
    """
    Автоматически вставляет партнёрскую ссылку в каждую акцию.
    Логика:
    1. Если для магазина есть STORE_AFFILIATE_TEMPLATES[store_key] -> используем его
    2. Иначе если задан GLOBAL_REDIRECT -> используем его
    3. Иначе отдаём обычную ссылку
    """
    if not raw_url:
        return raw_url

    encoded_url = quote(raw_url, safe="")

    template = STORE_AFFILIATE_TEMPLATES.get(store_key, "").strip()
    if template:
        final_url = template.replace("{url}", encoded_url)
    elif GLOBAL_REDIRECT.strip():
        final_url = GLOBAL_REDIRECT.strip().replace("{url}", encoded_url)
    else:
        final_url = raw_url

    if ADD_SUBID and user_id is not None:
        separator = "&" if "?" in final_url else "?"
        final_url = f"{final_url}{separator}subid={user_id}"

    return final_url


def deal_keyboard(deal: Deal, user_id: int | None = None) -> InlineKeyboardMarkup:
    final_url = build_affiliate_url(deal.store_key, deal.url, user_id=user_id)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛒 Перейти до акції", url=final_url)],
            [InlineKeyboardButton(text="🔐 Включити VPN", url=TELEGRAM_PARTNER_URL)]
        ]
    )


async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        )
    }
    timeout = aiohttp.ClientTimeout(total=25)
    async with session.get(url, headers=headers, timeout=timeout, ssl=False) as resp:
        resp.raise_for_status()
        return await resp.text()


def parse_eva(html_text: str, base_url: str) -> List[Deal]:
    soup = BeautifulSoup(html_text, "html.parser")
    items: List[Deal] = []
    keywords = ["знижк", "акці", "промокод", "до -", "%", "sale", "розпрод"]

    for a in soup.find_all("a", href=True):
        text = normalize_spaces(a.get_text(" ", strip=True))
        href = absolute_url(base_url, a["href"])

        if len(text) < 10:
            continue
        if not any(k in text.lower() for k in keywords):
            continue

        card = a.parent
        img = extract_image_from_tag(card, base_url) or extract_image_from_tag(a, base_url)

        items.append(Deal(
            store_key="eva",
            store_name="EVA",
            title=text,
            url=href,
            image=img
        ))

    return unique_deals(items)


def parse_prostor(html_text: str, base_url: str) -> List[Deal]:
    soup = BeautifulSoup(html_text, "html.parser")
    items: List[Deal] = []
    keywords = ["знижк", "акці", "1+1", "2=3", "до -", "%", "sale", "вигода"]

    for a in soup.find_all("a", href=True):
        text = normalize_spaces(a.get_text(" ", strip=True))
        href = absolute_url(base_url, a["href"])

        if len(text) < 10:
            continue
        if not any(k in text.lower() for k in keywords):
            continue

        card = a.parent
        img = extract_image_from_tag(card, base_url) or extract_image_from_tag(a, base_url)

        items.append(Deal(
            store_key="prostor",
            store_name="PROSTOR",
            title=text,
            url=href,
            image=img
        ))

    return unique_deals(items)


def parse_rozetka(html_text: str, base_url: str) -> List[Deal]:
    soup = BeautifulSoup(html_text, "html.parser")
    items: List[Deal] = []
    keywords = ["знижк", "акці", "до -", "%", "вигід", "sale", "розпрод", "ціни"]

    for a in soup.find_all("a", href=True):
        text = normalize_spaces(a.get_text(" ", strip=True))
        href = absolute_url(base_url, a["href"])

        if len(text) < 10:
            continue
        if not any(k in text.lower() for k in keywords):
            continue

        card = a.parent
        img = extract_image_from_tag(card, base_url) or extract_image_from_tag(a, base_url)

        items.append(Deal(
            store_key="rozetka",
            store_name="ROZETKA",
            title=text,
            url=href,
            image=img
        ))

    return unique_deals(items)


async def load_store_deals(store_key: str) -> List[Deal]:
    store = STORES[store_key]
    async with aiohttp.ClientSession() as session:
        html_text = await fetch_html(session, store["url"])

    if store_key == "eva":
        return parse_eva(html_text, store["url"])
    if store_key == "prostor":
        return parse_prostor(html_text, store["url"])
    if store_key == "rozetka":
        return parse_rozetka(html_text, store["url"])
    return []


bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


async def send_deal(chat_id, deal: Deal, user_id: int | None = None) -> None:
    text = (
        f"🔥 <b>{deal.store_name}</b>\n\n"
        f"<b>{deal.title}</b>\n\n"
        f"Нова знайдена акція.\n\n"
        f"Нижче є кнопка переходу до акції та окрема кнопка зі Stars."
    )

    kb = deal_keyboard(deal, user_id=user_id)

    try:
        if deal.image:
            await bot.send_photo(
                chat_id=chat_id,
                photo=deal.image,
                caption=text,
                reply_markup=kb,
            )
        else:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
    except Exception:
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💄 EVA", callback_data="store:eva"),
             InlineKeyboardButton(text="🧴 PROSTOR", callback_data="store:prostor")],
            [InlineKeyboardButton(text="🛒 ROZETKA", callback_data="store:rozetka")],
            [InlineKeyboardButton(text="🔥 Перевірити все зараз", callback_data="check:all")]
        ]
    )


@dp.message(CommandStart())
async def start_handler(message: Message):
    text = (
        "Привіт! 🇺🇦\n\n"
        "Я шукаю нові акції в EVA, PROSTOR і ROZETKA.\n"
        f"Автоперевірка: кожні <b>{CHECK_EVERY_MINUTES}</b> хв.\n\n"
        "Команди:\n"
        "/check — перевірити все зараз\n"
        "/setchat — зробити цей чат ціллю для автопостингу\n"
        "/status — показати статус\n"
    )
    await message.answer(text, reply_markup=main_menu())


@dp.message(Command("setchat"))
async def setchat_handler(message: Message):
    global TARGET_CHAT_ID
    TARGET_CHAT_ID = str(message.chat.id)
    await message.answer(f"✅ Тепер нові акції будуть надсилатися сюди:\n<code>{TARGET_CHAT_ID}</code>")


@dp.message(Command("status"))
async def status_handler(message: Message):
    mode = "звичайні посилання"
    if any(v.strip() for v in STORE_AFFILIATE_TEMPLATES.values()) or GLOBAL_REDIRECT.strip():
        mode = "партнёрка увімкнена"

    await message.answer(
        f"🤖 Статус бота\n\n"
        f"Перевірка кожні: <b>{CHECK_EVERY_MINUTES}</b> хв\n"
        f"Цільовий чат: <code>{TARGET_CHAT_ID or 'не задано'}</code>\n"
        f"Магазини: EVA, PROSTOR, ROZETKA\n"
        f"Режим посилань: <b>{mode}</b>"
    )


@dp.message(Command("check"))
async def manual_check_handler(message: Message):
    await message.answer("⏳ Перевіряю нові акції...")
    summary = await check_new_deals_and_send(chat_id=message.chat.id, user_id=message.from_user.id, send_only_new=False)
    await message.answer(summary)


@dp.callback_query(F.data.startswith("store:"))
async def store_callback(callback):
    store_key = callback.data.split(":", 1)[1]
    await callback.answer("Шукаю...")
    try:
        deals = await load_store_deals(store_key)
    except Exception as e:
        logging.exception("Store error: %s", e)
        await callback.message.answer("Не вдалося завантажити акції.")
        return

    if not deals:
        await callback.message.answer("Поки що нічого не знайдено.")
        return

    await callback.message.answer(f"✅ {STORES[store_key]['title']}: знайдено {min(len(deals), 5)} акцій")
    for deal in deals[:5]:
        await send_deal(callback.message.chat.id, deal, user_id=callback.from_user.id)


@dp.callback_query(F.data == "check:all")
async def check_all_callback(callback):
    await callback.answer("Перевіряю...")
    summary = await check_new_deals_and_send(
        chat_id=callback.message.chat.id,
        user_id=callback.from_user.id,
        send_only_new=False
    )
    await callback.message.answer(summary)


async def check_new_deals_and_send(chat_id=None, user_id: int | None = None, send_only_new=True) -> str:
    seen = load_seen()
    report_lines = []
    total_sent = 0

    for store_key, store_info in STORES.items():
        try:
            deals = await load_store_deals(store_key)
            if not deals:
                report_lines.append(f"• {store_info['title']}: нічого не знайдено")
                continue

            if send_only_new:
                known_urls: Set[str] = set(seen.get(store_key, []))
                new_deals = [d for d in deals if d.url not in known_urls]
            else:
                new_deals = deals[:MAX_NEW_PER_STORE]

            new_deals = new_deals[:MAX_NEW_PER_STORE]

            if new_deals:
                target = chat_id or TARGET_CHAT_ID
                if target:
                    for deal in new_deals:
                        await send_deal(target, deal, user_id=user_id)
                        total_sent += 1
                report_lines.append(f"• {store_info['title']}: {len(new_deals)} нових")
            else:
                report_lines.append(f"• {store_info['title']}: без нових")

            seen[store_key] = [d.url for d in deals[:100]]

        except Exception as e:
            logging.exception("Check error in %s: %s", store_key, e)
            report_lines.append(f"• {store_info['title']}: помилка")

    save_seen(seen)
    return "📊 Результат перевірки\n\n" + "\n".join(report_lines) + f"\n\nНадіслано: <b>{total_sent}</b>"


async def background_checker():
    await asyncio.sleep(5)
    while True:
        try:
            if TARGET_CHAT_ID:
                logging.info("Background check started")
                summary = await check_new_deals_and_send(send_only_new=True)
                logging.info(summary.replace("\n", " | "))
            else:
                logging.info("TARGET_CHAT_ID is empty, skip background check")
        except Exception as e:
            logging.exception("Background loop error: %s", e)

        await asyncio.sleep(CHECK_EVERY_MINUTES * 60)


async def main():
    if not BOT_TOKEN.strip() or "PASTE_YOUR_BOT_TOKEN_HERE" in BOT_TOKEN:
        raise RuntimeError("Встав свій BOT TOKEN у BOT_TOKEN")

    asyncio.create_task(background_checker())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
