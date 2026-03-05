#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import random
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None


# =======================
# ENV / SETTINGS
# =======================
BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
CHANNEL = os.getenv("TG_CHANNEL")  # для приватного канала: "-100xxxxxxxxxx"

DB_PATH = os.getenv("DB_PATH", "posted.sqlite3")

WINDOW_HOURS = int(os.getenv("WINDOW_HOURS", "48"))          # постим только за последние N часов
MAX_POSTS_PER_RUN = int(os.getenv("MAX_POSTS_PER_RUN", "2")) # максимум постов за один запуск
NEWS_PAGES = int(os.getenv("NEWS_PAGES", "2"))               # /ru/news/ + /ru/news/p2/ (и т.д.)
LISTING_FETCH_LIMIT = int(os.getenv("LISTING_FETCH_LIMIT", "80"))

SLEEP_BETWEEN_POSTS_SEC = float(os.getenv("SLEEP_BETWEEN_POSTS_SEC", "1.5"))

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_BACKOFF_BASE = float(os.getenv("HTTP_BACKOFF_BASE", "1.7"))

NEWS_INDEX_URL = os.getenv("NEWS_INDEX_URL", "https://worldoftanks.eu/ru/news/")

# ссылки вида /ru/news/<category>/<slug>/
LINK_RE = re.compile(r"^/ru/news/[^/]+/[^/]+/?$")

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]


@dataclass(frozen=True)
class ListingItem:
    url: str
    title: str
    tag: str  # /ru/news/<tag>/...


# =======================
# DB
# =======================
def init_db() -> None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS posted(
            url TEXT PRIMARY KEY,
            title TEXT,
            tag TEXT,
            posted_at TEXT
        )
        """
    )
    con.commit()
    con.close()


def already_posted(url: str) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM posted WHERE url=?", (url,))
    row = cur.fetchone()
    con.close()
    return row is not None


def mark_posted(url: str, title: str, tag: str) -> None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO posted(url, title, tag, posted_at)
        VALUES(?, ?, ?, ?)
        """,
        (url, title, tag, datetime.now(timezone.utc).isoformat()),
    )
    con.commit()
    con.close()


# =======================
# UTIL
# =======================
def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def clamp_text(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    if max_len <= 1:
        return s[:max_len]
    return s[: max_len - 1] + "…"


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": random.choice(UA_POOL),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en;q=0.8,de;q=0.6",
            "Connection": "keep-alive",
        }
    )
    return s


def fetch(session: requests.Session, url: str) -> Optional[requests.Response]:
    """
    Robust GET with exponential backoff.
    Returns Response on success, or None on final failure.
    """
    last_err: Optional[Exception] = None

    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT)

            if r.status_code == 200:
                return r

            # временные ошибки — ретраим
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {r.status_code} for {url}", response=r)

            # прочие (403/404 и т.п.) — не ретраим
            log(f"HTTP {r.status_code} (no-retry) {url}")
            return None

        except Exception as e:
            last_err = e
            wait = (HTTP_BACKOFF_BASE ** (attempt - 1)) + random.random()
            log(f"Fetch failed ({attempt}/{HTTP_RETRIES}) {url} -> {e}. Sleep {wait:.1f}s")
            time.sleep(wait)

    log(f"Fetch окончательно не удалось: {url} -> {last_err}")
    return None


def parse_iso_datetime(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def extract_from_jsonld(soup: BeautifulSoup) -> Optional[datetime]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            if not isinstance(obj, dict):
                continue
            dp = obj.get("datePublished") or obj.get("dateCreated")
            if isinstance(dp, str):
                dt = parse_iso_datetime(dp)
                if dt:
                    return dt
    return None


def normalize_abs_url(base: str, maybe_url: Optional[str]) -> Optional[str]:
    if not maybe_url:
        return None
    maybe_url = maybe_url.strip()
    if not maybe_url:
        return None
    return urljoin(base, maybe_url)


def url_to_tag(article_url: str) -> str:
    p = urlparse(article_url).path.strip("/").split("/")
    # ["ru", "news", "<tag>", "<slug>"]
    if len(p) >= 3 and p[0] == "ru" and p[1] == "news":
        return p[2]
    return "news"


# =======================
# TELEGRAM
# =======================
def tg_api_post(method: str, data: dict) -> requests.Response:
    api = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    r = requests.post(api, data=data, timeout=HTTP_TIMEOUT)
    if not r.ok:
        try:
            payload = r.json()
        except Exception:
            payload = {"raw": r.text}
        raise RuntimeError(
            "Telegram API error\n"
            f"HTTP: {r.status_code}\n"
            f"Method: {method}\n"
            f"Response: {payload}"
        )
    return r


def make_button(url: str) -> str:
    return json.dumps(
        {"inline_keyboard": [[{"text": "🔗 Читать новость", "url": url}]]},
        ensure_ascii=False,
    )


def tg_send_photo(photo_url: str, caption_html: str, button_url: str) -> None:
    caption_html = clamp_text(caption_html, 1024)  # лимит Telegram caption
    tg_api_post(
        "sendPhoto",
        {
            "chat_id": CHANNEL,
            "photo": photo_url,
            "caption": caption_html,
            "parse_mode": "HTML",
            "disable_notification": "false",
            "reply_markup": make_button(button_url),
        },
    )


def tg_send_message(text_html: str, button_url: str) -> None:
    text_html = clamp_text(text_html, 4096)
    tg_api_post(
        "sendMessage",
        {
            "chat_id": CHANNEL,
            "text": text_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": "false",
            "reply_markup": make_button(button_url),
        },
    )


# =======================
# PARSING
# =======================
def iter_index_pages(base_url: str, pages: int) -> Iterable[str]:
    yield base_url.rstrip("/") + "/"
    for i in range(2, pages + 1):
        yield urljoin(base_url.rstrip("/") + "/", f"p{i}/")


def parse_news_index_page(session: requests.Session, page_url: str) -> list[ListingItem]:
    r = fetch(session, page_url)
    if not r:
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    found: list[ListingItem] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not LINK_RE.match(href):
            continue

        url = urljoin(page_url, href)
        title = a.get_text(" ", strip=True)

        if not title or len(title) < 6:
            continue

        tag = url_to_tag(url)
        found.append(ListingItem(url=url, title=title, tag=tag))

    # dedupe keep order
    seen: set[str] = set()
    uniq: list[ListingItem] = []
    for it in found:
        if it.url in seen:
            continue
        seen.add(it.url)
        uniq.append(it)

    return uniq[:LISTING_FETCH_LIMIT]


def fetch_article_meta(session: requests.Session, article_url: str) -> tuple[Optional[datetime], Optional[str]]:
    r = fetch(session, article_url)
    if not r:
        return None, None

    soup = BeautifulSoup(r.text, "html.parser")

    published_dt: Optional[datetime] = None

    meta = soup.find("meta", attrs={"property": "article:published_time"})
    if meta and meta.get("content"):
        published_dt = parse_iso_datetime(meta["content"])

    if not published_dt:
        t = soup.find("time")
        if t and t.get("datetime"):
            published_dt = parse_iso_datetime(t["datetime"])

    if not published_dt:
        meta2 = soup.find("meta", attrs={"itemprop": "datePublished"})
        if meta2 and meta2.get("content"):
            published_dt = parse_iso_datetime(meta2["content"])

    if not published_dt:
        published_dt = extract_from_jsonld(soup)

    image_url: Optional[str] = None

    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        image_url = normalize_abs_url(article_url, og["content"])

    if not image_url:
        tw = soup.find("meta", attrs={"name": "twitter:image"})
        if tw and tw.get("content"):
            image_url = normalize_abs_url(article_url, tw["content"])

    if not image_url:
        ip = soup.find("meta", attrs={"itemprop": "image"})
        if ip and ip.get("content"):
            image_url = normalize_abs_url(article_url, ip["content"])

    return published_dt, image_url


# =======================
# STYLING (Style 2)
# =======================
def tag_to_label(tag: str) -> str:
    mapping = {
        "updates": "Обновления",
        "specials": "Акции",
        "general-news": "Новости",
        "events": "События",
        "tournaments": "Турниры",
        "merchandise": "Мерч",
        "guides": "Руководства",
        "clans": "Кланы",
    }
    return mapping.get(tag, tag.replace("-", " ").title())


def tag_to_icon(tag: str) -> str:
    mapping = {
        "updates": "🛠️",
        "specials": "🎁",
        "general-news": "📢",
        "events": "🎮",
        "tournaments": "🏆",
        "merchandise": "🛒",
        "guides": "📘",
        "clans": "🛡️",
    }
    return mapping.get(tag, "📰")


def extra_hashtags_by_title(title: str) -> list[str]:
    t = title.lower()
    mapping = [
        (["тест", "test", "ct", "common test", "sandbox"], "#test"),
        (["патч", "обновлен", "микропатч", "update"], "#patch"),
        (["акц", "скид", "распрод", "sale", "%"], "#sale"),
        (["ивент", "событ", "event", "мисси", "задач"], "#event"),
        (["турнир", "tournament"], "#tournament"),
        (["прем", "premium"], "#premium"),
        (["танк", "машин", "техника", "vehicle"], "#tanks"),
        (["карта", "map"], "#maps"),
    ]
    tags: list[str] = []
    for keys, tag in mapping:
        if any(k in t for k in keys):
            tags.append(tag)

    # unique keep order
    seen: set[str] = set()
    out: list[str] = []
    for x in tags:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def format_dt(dt_utc: Optional[datetime]) -> str:
    if not dt_utc:
        return "—"
    if ZoneInfo:
        try:
            dt_local = dt_utc.astimezone(ZoneInfo("Europe/Berlin"))
        except Exception:
            dt_local = dt_utc
    else:
        dt_local = dt_utc
    return dt_local.strftime("%d.%m.%Y %H:%M")


def format_caption_style2(title: str, tag: str, published_dt_utc: Optional[datetime]) -> str:
    safe_title = html_escape(title)
    safe_label = html_escape(tag_to_label(tag))
    safe_tag = html_escape(tag)

    icon = tag_to_icon(tag)
    dt_str = html_escape(format_dt(published_dt_utc))

    extra_tags = extra_hashtags_by_title(title)
    extra_tags_str = " ".join(extra_tags)

    hashtags_line = f"🏷️ #{safe_tag}"
    if extra_tags_str:
        hashtags_line += f"  {html_escape(extra_tags_str)}"

    return (
        "🚜 <b>Hard_kh • WoT EU</b>\n"
        "🪖 <i>Официальные новости</i>\n\n"
        f"{icon} <b>{safe_title}</b>\n"
        f"📅 <b>{dt_str}</b>\n"
        f"📌 Раздел: <b>{safe_label}</b>\n"
        f"{hashtags_line}"
    )


# =======================
# MAIN
# =======================
def collect_new_links_all(session: requests.Session) -> list[ListingItem]:
    candidates: list[ListingItem] = []

    for page_url in iter_index_pages(NEWS_INDEX_URL, NEWS_PAGES):
        items = parse_news_index_page(session, page_url)
        if not items:
            log(f"Пусто/ошибка на странице: {page_url}")
            continue

        for it in items:
            if not already_posted(it.url):
                candidates.append(it)

    # dedupe across pages
    seen: set[str] = set()
    uniq: list[ListingItem] = []
    for it in candidates:
        if it.url in seen:
            continue
        seen.add(it.url)
        uniq.append(it)
    return uniq


def run_once() -> int:
    # ВАЖНО: не валим workflow — просто выходим, чтобы следующая попытка сработала.
    if not BOT_TOKEN or not CHANNEL:
        log("Нужно задать TG_BOT_TOKEN и TG_CHANNEL.")
        return 0

    init_db()

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=WINDOW_HOURS)

    session = build_session()

    raw_candidates = collect_new_links_all(session)
    if not raw_candidates:
        log("Новых ссылок не найдено.")
        return 0

    candidates = list(reversed(raw_candidates))  # старые -> новые

    posted_count = 0
    for it in candidates:
        if posted_count >= MAX_POSTS_PER_RUN:
            break

        pub_dt, img = fetch_article_meta(session, it.url)

        if not pub_dt:
            log(f"Нет даты публикации -> skip: {it.url}")
            mark_posted(it.url, it.title, it.tag)
            continue

        if pub_dt < cutoff:
            mark_posted(it.url, it.title, it.tag)
            continue

        caption = format_caption_style2(it.title, it.tag, pub_dt)

        try:
            if img:
                tg_send_photo(img, caption, button_url=it.url)
            else:
                open_link = f"\n\n<a href=\"{html_escape(it.url)}\">Открыть</a>"
                tg_send_message(caption + open_link, button_url=it.url)
        except Exception as e:
            # если телега отвалилась — НЕ помечаем posted, чтобы повторить на следующем запуске
            log(f"Ошибка Telegram при отправке: {e}")
            return 0

        mark_posted(it.url, it.title, it.tag)
        posted_count += 1
        log(f"Опубликовано: {it.title} ({it.tag})")
        time.sleep(SLEEP_BETWEEN_POSTS_SEC)

    log(f"Готово. Опубликовано за запуск: {posted_count}")
    return 0


if __name__ == "__main__":
    sys.exit(run_once())
