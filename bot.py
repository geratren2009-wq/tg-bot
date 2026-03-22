import asyncio
import json
import logging
import os
import re
import sqlite3
import hashlib
from dataclasses import dataclass
from typing import Dict, List, Set
from urllib.parse import quote, urljoin

import aiohttp
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = 939148059
TARGET_CHAT_ID = ""
CHECK_EVERY_MINUTES = 20
MAX_NEW_PER_STORE = 5
STATE_FILE = "seen_deals.json"
DB_FILE = "clicks.db"

SITE_URL = "https://znizhkyakciiua.netlify.app/"
TELEGRAM_PARTNER_URL = "https://t.me/BuyVPN_Global_bot?start=_tgr_1lp79ZIwODli"

GLOBAL_REDIRECT = ""
STORE_AFFILIATE_TEMPLATES = {"eva": "", "prostor": "", "rozetka": ""}
ADD_SUBID = True

STORES = {
    "eva": {"title": "EVA", "url": "https://eva.ua/ua/promotion/"},
    "prostor": {"title": "PROSTOR", "url": "https://prostor.ua/ua/aktsii/"},
    "rozetka": {"title": "ROZETKA", "url": "https://rozetka.com.ua/ua/news-articles-promotions/promotions/"},
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


@dataclass
class Deal:
    store_key: str
    store_name: str
    title: str
    url: str
    image: str = ""


DEALS_CACHE: Dict[str, Deal] = {}


def is_admin(user_id: int | None) -> bool:
    return user_id == ADMIN_ID


def init_db() -> None:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clicks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            deal_id TEXT,
            store_key TEXT,
            store_name TEXT,
            title TEXT,
            original_url TEXT,
            final_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def save_click(user_id, username, deal_id: str, deal: Deal, final_url: str) -> None:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO clicks (user_id, username, deal_id, store_key, store_name, title, original_url, final_url)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, username, deal_id, deal.store_key, deal.store_name, deal.title, deal.url, final_url))
    conn.commit()
    conn.close()


def get_stats(limit: int = 10):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT store_name, title, COUNT(*) AS cnt
        FROM clicks
        GROUP BY store_name, title
        ORDER BY cnt DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_total_clicks() -> int:
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM clicks")
    total = cur.fetchone()[0]
    conn.close()
    return total


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


def make_deal_id(deal: Deal) -> str:
    raw = f"{deal.store_key}|{deal.url}|{deal.title}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def cache_deal(deal: Deal) -> str:
    deal_id = make_deal_id(deal)
    DEALS_CACHE[deal_id] = deal
    return deal_id


def build_affiliate_url(store_key: str, raw_url: str, user_id=None) -> str:
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


def deal_keyboard(deal: Deal) -> InlineKeyboardMarkup:
    deal_id = cache_deal(deal)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Перейти до акції", callback_data=f"go:{deal_id}")],
        [InlineKeyboardButton(text="🌐 Відкрити сайт", url=SITE_URL)],
        [InlineKeyboardButton(text="🔐 Включити VPN", url=TELEGRAM_PARTNER_URL)],
    ])


def final_open_keyboard(final_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Відкрити акцію", url=final_url)],
        [InlineKeyboardButton(text="🌐 Відкрити сайт", url=SITE_URL)],
        [InlineKeyboardButton(text="🔐 Включити VPN", url=TELEGRAM_PARTNER_URL)],
    ])


async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    timeout = aiohttp.ClientTimeout(total=25)
    async with session.get(url, headers=headers, timeout=timeout, ssl=False) as resp:
        resp.raise_for_status()
        return await resp.text()


def parse_eva(html_text: str, base_url: str) -> List[Deal]:
    soup = BeautifulSoup(html_text, "html.parser")
    items = []
    keywords = ["знижк", "акці", "промокод", "до -", "%", "sale", "розпрод"]
    for a in soup.find_all("a", href=True):
        text = normalize_spaces(a.get_text(" ", strip=True))
        href = absolute_url(base_url, a["href"])
        if len(text) < 10 or not any(k in text.lower() for k in keywords):
            continue
        card = a.parent
        img = extract_image_from_tag(card, base_url) or extract_image_from_tag(a, base_url)
        items.append(Deal("eva", "EVA", text, href, img))
    return unique_deals(items)


def parse_prostor(html_text: str, base_url: str) -> List[Deal]:
    soup = BeautifulSoup(html_text, "html.parser")
    items = []
    keywords = ["знижк", "акці", "1+1", "2=3", "до -", "%", "sale", "вигода"]
    for a in soup.find_all("a", href=True):
        text = normalize_spaces(a.get_text(" ", strip=True))
        href = absolute_url(base_url, a["href"])
        if len(text) < 10 or not any(k in text.lower() for k in keywords):
            continue
        card = a.parent
        img = extract_image_from_tag(card, base_url) or extract_image_from_tag(a, base_url)
        items.append(Deal("prostor", "PROSTOR", text, href, img))
    return unique_deals(items)


def parse_rozetka(html_text: str, base_url: str) -> List[Deal]:
    soup = BeautifulSoup(html_text, "html.parser")
    items = []
    keywords = ["знижк", "акці", "до -", "%", "вигід", "sale", "розпрод", "ціни"]
    for a in soup.find_all("a", href=True):
        text = normalize_spaces(a.get_text(" ", strip=True))
        href = absolute_url(base_url, a["href"])
        if len(text) < 10 or not any(k in text.lower() for k in keywords):
            continue
        card = a.parent
        img = extract_image_from_tag(card, base_url) or extract_image_from_tag(a, base_url)
        items.append(Deal("rozetka", "ROZETKA", text, href, img))
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


async def send_deal(chat_id, deal: Deal) -> None:
    text = f"🔥 <b>{deal.store_name}</b>\n\n<b>{deal.title}</b>\n\nНатисни кнопку нижче, щоб відкрити акцію."
    kb = deal_keyboard(deal)
    try:
        if deal.image:
            await bot.send_photo(chat_id=chat_id, photo=deal.image, caption=text, reply_markup=kb)
        else:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)
    except Exception:
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💄 EVA", callback_data="store:eva"),
         InlineKeyboardButton(text="🧴 PROSTOR", callback_data="store:prostor")],
        [InlineKeyboardButton(text="🛒 ROZETKA", callback_data="store:rozetka")],
        [InlineKeyboardButton(text="🔥 Перевірити все зараз", callback_data="check:all")]
    ])


@dp.message(CommandStart())
async def start_handler(message: Message):
    text = (
        "Привіт! 🇺🇦\n\n"
        "Я шукаю нові акції в EVA, PROSTOR і ROZETKA.\n"
        f"Автоперевірка: кожні <b>{CHECK_EVERY_MINUTES}</b> хв."
    )
    if is_admin(message.from_user.id if message.from_user else None):
        text += (
            "\n\nКоманди адміністратора:\n"
            "/check — перевірити все зараз\n"
            "/setchat — зробити цей чат ціллю для автопостингу\n"
            "/status — показати статус\n"
            "/stats — статистика кліків"
        )
    await message.answer(text, reply_markup=main_menu())


@dp.message(Command("setchat"))
async def setchat_handler(message: Message):
    if not is_admin(message.from_user.id if message.from_user else None):
        return
    global TARGET_CHAT_ID
    TARGET_CHAT_ID = str(message.chat.id)
    await message.answer(f"✅ Тепер нові акції будуть надсилатися сюди:\n<code>{TARGET_CHAT_ID}</code>")


@dp.message(Command("status"))
async def status_handler(message: Message):
    if not is_admin(message.from_user.id if message.from_user else None):
        return
    mode = "звичайні посилання"
    if any(v.strip() for v in STORE_AFFILIATE_TEMPLATES.values()) or GLOBAL_REDIRECT.strip():
        mode = "партнерські / редирект-посилання"
    await message.answer(
        f"🤖 Статус бота\n\n"
        f"Перевірка кожні: <b>{CHECK_EVERY_MINUTES}</b> хв\n"
        f"Цільовий чат: <code>{TARGET_CHAT_ID or 'не задано'}</code>\n"
        f"Магазини: EVA, PROSTOR, ROZETKA\n"
        f"Режим посилань: <b>{mode}</b>\n"
        f"Усього кліків: <b>{get_total_clicks()}</b>"
    )


@dp.message(Command("stats"))
async def stats_handler(message: Message):
    if not is_admin(message.from_user.id if message.from_user else None):
        return
    rows = get_stats(10)
    total = get_total_clicks()
    if not rows:
        await message.answer("Поки що кліків немає.")
        return
    lines = [f"📊 <b>Статистика кліків</b>\n", f"Усього кліків: <b>{total}</b>\n"]
    for idx, (store_name, title, cnt) in enumerate(rows, start=1):
        lines.append(f"{idx}. <b>{store_name}</b> — {title} <b>({cnt})</b>")
    await message.answer("\n".join(lines))


@dp.message(Command("check"))
async def manual_check_handler(message: Message):
    if not is_admin(message.from_user.id if message.from_user else None):
        return
    await message.answer("⏳ Перевіряю нові акції...")
    summary = await check_new_deals_and_send(chat_id=message.chat.id, send_only_new=False)
    await message.answer(summary)


@dp.callback_query(F.data.startswith("store:"))
async def store_callback(callback: CallbackQuery):
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
        await send_deal(callback.message.chat.id, deal)


@dp.callback_query(F.data == "check:all")
async def check_all_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id if callback.from_user else None):
        await callback.answer("Недоступно", show_alert=False)
        return
    await callback.answer("Перевіряю...")
    summary = await check_new_deals_and_send(chat_id=callback.message.chat.id, send_only_new=False)
    await callback.message.answer(summary)


@dp.callback_query(F.data.startswith("go:"))
async def go_callback(callback: CallbackQuery):
    deal_id = callback.data.split(":", 1)[1]
    deal = DEALS_CACHE.get(deal_id)
    if not deal:
        await callback.answer("Посилання застаріло. Відкрий новий пост.", show_alert=True)
        return
    user_id = callback.from_user.id if callback.from_user else None
    username = callback.from_user.username if callback.from_user else None
    final_url = build_affiliate_url(deal.store_key, deal.url, user_id=user_id)
    save_click(user_id, username, deal_id, deal, final_url)
    await callback.answer()
    await callback.message.answer("Відкрий акцію нижче 👇", reply_markup=final_open_keyboard(final_url))


async def check_new_deals_and_send(chat_id=None, send_only_new=True) -> str:
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
                        await send_deal(target, deal)
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
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не знайдено в змінних середовища")
    init_db()
    asyncio.create_task(background_checker())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
