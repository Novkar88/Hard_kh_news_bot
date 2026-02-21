import os
import re
import time
import sqlite3
import json
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
CHANNEL = os.getenv("TG_CHANNEL")

# Парсим ВСЕ новости из общей ленты
NEWS_INDEX_URL = "https://worldoftanks.eu/ru/news/"

DB_PATH = "posted.sqlite3"
UA = "Mozilla/5.0 (WoTEUNewsBot/2.0)"
HTTP_TIMEOUT = 30

# /ru/news/<category>/<slug>/
ARTICLE_PATH_RE = re.compile(r"^/ru/news/([^/]+)/([^/]+)/?$")

# ===== Настройки =====
MAX_POSTS_PER_RUN = 2            # максимум постов за запуск
WINDOW_HOURS = 48                # публикуем ТОЛЬКО за последние 48 часов
PAGES_TO_SCAN = 6                # сколько страниц ленты максимум сканировать за запуск
ARTICLES_PER_PAGE_HINT = 48      # подсказка/лимит на сбор ссылок (защита)
SLEEP_BETWEEN_POSTS_SEC = 2
# =====================


# ---------- DB ----------
def init_db():
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


def mark_posted(url: str, title: str, tag: str):
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


# ---------- Helpers ----------
def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def tg_api_post(method: str, data: dict):
    api = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    r = requests.post(api, data=data, timeout=HTTP_TIMEOUT)
    if not r.ok:
        try:
            payload = r.json()
        except Exception:
            payload = {"raw": r.text}
        raise SystemExit(
            "Telegram API error\n"
            f"HTTP: {r.status_code}\n"
            f"Method: {method}\n"
            f"Response: {payload}"
        )
    return r


def make_button(url: str):
    return json.dumps(
        {"inline_keyboard": [[{"text": "🔗 Читать новость", "url": url}]]},
        ensure_ascii=False,
    )


def tg_send_photo(photo_url: str, caption_html: str, button_url: str):
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


def tg_send_message(text_html: str, button_url: str):
    # Превью оставляем включённым — Telegram подтянет картинку по ссылке, если сможет
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


def parse_iso_datetime(s: str):
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


def extract_from_jsonld(soup: BeautifulSoup):
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


def fetch_article_meta(article_url: str):
    """
    Один запрос к статье: достаём
    - published_dt (UTC)
    - image_url (og/twitter/itemprop)
    - title (если надо)
    """
    r = requests.get(article_url, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # ---- date ----
    published_dt = None

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

    # ---- image ----
    image_url = None

    og = soup.find("meta", attrs={"property": "og:image"})
    if og and og.get("content"):
        image_url = og["content"].strip()

    if not image_url:
        tw = soup.find("meta", attrs={"name": "twitter:image"})
        if tw and tw.get("content"):
            image_url = tw["content"].strip()

    if not image_url:
        ip = soup.find("meta", attrs={"itemprop": "image"})
        if ip and ip.get("content"):
            image_url = ip["content"].strip()

    # ---- title fallback ----
    title = None
    ot = soup.find("meta", attrs={"property": "og:title"})
    if ot and ot.get("content"):
        title = ot["content"].strip()

    if not title:
        if soup.title and soup.title.get_text(strip=True):
            title = soup.title.get_text(strip=True)

    return published_dt, image_url, title


def normalize_url(u: str) -> str:
    # Убираем якоря/параметры, чтобы URL в базе был стабильный
    p = urlparse(u)
    clean = p._replace(query="", fragment="")
    return clean.geturl()


def tag_from_article_url(article_url: str) -> str:
    p = urlparse(article_url)
    m = ARTICLE_PATH_RE.match(p.path)
    if not m:
        return "news"
    return m.group(1)  # category segment


# ---------- Styling ----------
def tag_to_icon(tag: str) -> str:
    # Небольшая “семантика”, остальное будет 📰
    mapping = {
        "updates": "🛠️",
        "specials": "🎁",
        "general-news": "📢",
        "merchandise": "🛍️",
        "clan": "🛡️",
        "tournaments": "🏆",
        "competitive-gaming": "🏆",
        "community": "👥",
        "live-streams": "📺",
        "guides": "📘",
        "ranked": "🎖️",
        "frontline": "🚚",
        "battle-pass": "🎟️",
        "common-test": "🧪",
        "test": "🧪",
    }
    return mapping.get(tag, "📰")


def tag_to_label(tag: str) -> str:
    # Покажем “как есть”, но чуть приукрасим основные
    mapping = {
        "updates": "Обновления",
        "specials": "Акции",
        "general-news": "Новости",
        "merchandise": "Мерч",
        "common-test": "Тест",
        "test": "Тест",
    }
    return mapping.get(tag, tag)


def extra_hashtags_by_title(title: str):
    t = (title or "").lower()
    mapping = [
        (["патч", "обновлен", "микропатч", "update"], "#patch"),
        (["акц", "скид", "распрод", "sale", "%"], "#sale"),
        (["ивент", "событ", "event", "мисси", "задач"], "#event"),
        (["турнир", "tournament"], "#tournament"),
        (["прем", "premium"], "#premium"),
        (["тест", "common test", "общем тест"], "#test"),
        (["карта", "map"], "#maps"),
        (["танк", "ветк", "branch"], "#tanks"),
    ]
    tags = []
    for keys, tag in mapping:
        if any(k in t for k in keys):
            tags.append(tag)
    # unique keep order
    seen = set()
    out = []
    for x in tags:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def format_dt(dt_utc: datetime | None) -> str:
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


def format_caption_style2(title: str, tag: str, published_dt_utc: datetime | None):
    safe_title = html_escape(title or "Без названия")
    safe_label = html_escape(tag_to_label(tag))
    safe_tag = html_escape(tag)

    icon = tag_to_icon(tag)
    dt_str = html_escape(format_dt(published_dt_utc))

    extra_tags = extra_hashtags_by_title(title or "")
    extra_tags_str = " ".join(extra_tags)

    hashtags_line = f"🏷️ #{safe_tag}"
    if extra_tags_str:
        hashtags_line += f"  {html_escape(extra_tags_str)}"

    return (
        f"📰 <b>WoT EU • НОВОСТИ</b>\n"
        f"{icon} <b>{safe_title}</b>\n"
        f"📅 Дата: <b>{dt_str}</b>\n"
        f"📌 Раздел: <b>{safe_label}</b>\n"
        f"{hashtags_line}"
    )


# ---------- Parsing index (ALL news) ----------
def parse_news_index_page(page_url: str):
    """
    Возвращает список ссылок на статьи (url, rough_title).
    Здесь мы берём только ссылки формата /ru/news/<cat>/<slug>/.
    """
    r = requests.get(page_url, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    items = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        m = ARTICLE_PATH_RE.match(href)
        if not m:
            continue

        url = normalize_url(urljoin(page_url, href))
        title = a.get_text(" ", strip=True) or ""
        # title на странице может быть шумный — нормально, мы уточним по статье
        items.append((url, title))

    # unique keep order
    seen = set()
    uniq = []
    for url, title in items:
        if url in seen:
            continue
        seen.add(url)
        uniq.append((url, title))

    return uniq


def collect_new_links_all():
    """
    Собираем кандидатов из /ru/news/ + pagination.
    Возвращаем список (url, title_guess).
    """
    all_items = []
    for page in range(1, PAGES_TO_SCAN + 1):
        page_url = NEWS_INDEX_URL if page == 1 else urljoin(NEWS_INDEX_URL, f"p{page}/")
        items = parse_news_index_page(page_url)
        all_items.extend(items)

        # страховка: не собираем бесконечно
        if len(all_items) >= (ARTICLES_PER_PAGE_HINT * PAGES_TO_SCAN):
            break

    return all_items


# ---------- Main ----------
def run_once():
    if not BOT_TOKEN or not CHANNEL:
        raise SystemExit("Нужно задать TG_BOT_TOKEN и TG_CHANNEL (в run.bat).")

    init_db()

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=WINDOW_HOURS)

    # кандидаты из общей ленты
    raw_candidates = collect_new_links_all()
    if not raw_candidates:
        return

    # фильтруем те, которых ещё нет в базе
    candidates = []
    for url, title_guess in raw_candidates:
        if not already_posted(url):
            candidates.append((url, title_guess))

    if not candidates:
        return

    # Публикуем "от старых к новым": сначала перевернём список
    candidates_ordered = list(reversed(candidates))

    posted_count = 0
    for url, title_guess in candidates_ordered:
        if posted_count >= MAX_POSTS_PER_RUN:
            break

        tag = tag_from_article_url(url)

        try:
            pub_dt, img, title_real = fetch_article_meta(url)
        except Exception:
            # если статья не открылась — пометим как seen, чтобы не зациклиться
            mark_posted(url, title_guess or url, tag)
            continue

        # если даты нет — не постим (иначе улетит старьё), но помечаем
        if not pub_dt:
            mark_posted(url, title_real or title_guess or url, tag)
            continue

        # если старее 48ч — не постим, но помечаем
        if pub_dt < cutoff:
            mark_posted(url, title_real or title_guess or url, tag)
            continue

        title_to_use = title_real or title_guess or "Новость"
        caption = format_caption_style2(title_to_use, tag, pub_dt)

        if img:
            tg_send_photo(img, caption, button_url=url)
        else:
            # fallback: ссылка в тексте для превью
            safe_url = html_escape(url)
            tg_send_message(
                caption + f"\n\n<a href=\"{safe_url}\">Открыть</a>",
                button_url=url,
            )

        mark_posted(url, title_to_use, tag)
        posted_count += 1
        time.sleep(SLEEP_BETWEEN_POSTS_SEC)


if __name__ == "__main__":
    run_once()