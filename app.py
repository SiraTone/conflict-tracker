#!/usr/bin/env python3
"""
WIRE // Global Conflict Monitor — advanced OSINT dashboard (standalone app).

Legal, public-data-only OSINT aggregator. Everything here pulls from open
RSS feeds, public subreddit RSS, public Telegram channel preview pages,
public prediction markets, public ADS-B data, public USGS seismic data,
and free public market-price data. No private/individual tracking, no
image geolocation, no targeting features of any kind.

Panels:
  - Wire feed: keyword-filtered headlines from a large set of international
    + regional news RSS feeds, Gulf/Levant subreddits, and public Telegram
    channels, each polling independently.
  - Intensity graph: rolling event-volume-per-hour chart, split by source
    type, so spikes in reporting are visible at a glance.
  - Geo panel: lightweight gazetteer match against headline text, plotting
    country/city-level mentions on a simple world map. Country/city level
    only — this is a "where is activity clustering" view, not a
    geolocation tool for people or individual images.
  - Confidence scoring: alert-tier items are cross-referenced against how
    many distinct sources reported something matching the same keyword
    cluster within a rolling window, giving a rough corroboration score
    instead of a single binary "alert" flag.
  - Markets strip: live Polymarket odds for conflict-relevant questions.
  - Commodities/FX strip: public price data (oil, gold, key regional FX)
    since markets often price in escalation risk before headlines catch up.
  - Seismic cross-reference: public USGS feed, flagged only as "worth a
    look" near conflict zones — many quakes are just quakes.
  - Flights strip: best-effort CENTCOM-AOR ADS-B (civilian-visible only).
  - Timeline/history log: persistent (session-lifetime) event log you can
    scroll back through, separate from the live-scrolling wire.

Run it:
    pip install flask feedparser tzdata requests beautifulsoup4
    python app.py
"""

import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from collections import deque, defaultdict
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import feedparser
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, render_template_string

EASTERN = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Configuration — edit freely
# ---------------------------------------------------------------------------

NEWS_FEEDS = {
    # -- International wires / world desks --
    "BBC":               "http://feeds.bbci.co.uk/news/world/rss.xml",
    "Al Jazeera":        "https://www.aljazeera.com/xml/rss/all.xml",
    "Guardian":          "https://www.theguardian.com/world/rss",
    "NYT":               "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "NPR":               "https://feeds.npr.org/1004/rss.xml",
    "DW":                "https://rss.dw.com/rdf/rss-en-world",
    "CNN":               "http://rss.cnn.com/rss/cnn_world.rss",
    "Sky News":          "https://feeds.skynews.com/feeds/rss/world.xml",
    "France24":          "https://www.france24.com/en/rss",
    "Euronews":          "https://www.euronews.com/rss?level=theme&name=news",
    "CBC":               "https://www.cbc.ca/cmlink/rss-world",
    "Independent":       "https://www.independent.co.uk/news/world/rss",
    "Kyiv Independent":  "https://kyivindependent.com/rss/",
    "Straits Times":     "https://www.straitstimes.com/news/world/rss.xml",
    "Reuters World":     "https://www.reutersagency.com/feed/?best-topics=world&post_type=best",
    "AP World":          "https://apnews.com/hub/world-news.rss",
    "Politico":          "https://www.politico.com/rss/politicopicks.xml",
    "Foreign Policy":    "https://foreignpolicy.com/feed/",
    "Foreign Affairs":   "https://www.foreignaffairs.com/rss.xml",
    "The Diplomat":      "https://thediplomat.com/feed/",
    "War on the Rocks":  "https://warontherocks.com/feed/",
    "Defense News":      "https://www.defensenews.com/arc/outboundfeeds/rss/",
    "Janes":             "https://www.janes.com/feeds/news",
    "ISW":               "https://www.understandingwar.org/rss.xml",

    # -- Middle East / regional outlets --
    "Times of Israel":      "https://www.timesofisrael.com/feed/",
    "Middle East Eye":      "https://www.middleeasteye.net/rss",
    "Middle East Monitor":  "https://www.middleeastmonitor.com/feed/",
    "Jerusalem Post":       "https://www.jpost.com/Rss/RssFeedsFrontPage.aspx",
    "Ynetnews":              "https://www.ynetnews.com/Integration/StoryRss3082.xml",
    "Arutz Sheva":           "https://www.israelnationalnews.com/Rss.aspx",
    "i24NEWS":               "https://www.i24news.tv/en/rss",
    "Al Arabiya English":    "https://english.alarabiya.net/rss.xml",
    "Arab News":             "https://www.arabnews.com/rss.xml",
    "Al-Monitor":            "https://www.al-monitor.com/rss",
    "The National (UAE)":    "https://www.thenationalnews.com/rss/latest.xml",
    "Iran International":    "https://www.iranintl.com/en/rss",
    "Rudaw":                 "https://www.rudaw.net/rss/english",
    "Daily Sabah":           "https://www.dailysabah.com/rssFeed/10000",
    "Anadolu Agency":        "https://www.aa.com.tr/en/rss/default?cat=live",
    "L'Orient Today":        "https://today.lorientlejour.com/rss.xml",

    # -- Other conflict-relevant regions --
    "Kyiv Post":             "https://www.kyivpost.com/rss",
    "Moscow Times":          "https://www.themoscowtimes.com/rss/news",
    "Taiwan News":           "https://www.taiwannews.com.tw/en/rss",
    "NK News":               "https://www.nknews.org/feed/",
    "Africa Confidential":   "https://www.africa-confidential.com/rss/news",
    "AllAfrica Conflict":    "https://allafrica.com/tools/headlines/rdf/peaceafrica/headlines.rdf",
}

REDDIT_FEEDS = {
    "r/Kuwait":          "https://www.reddit.com/r/Kuwait/new/.rss",
    "r/UAE":             "https://www.reddit.com/r/UAE/new/.rss",
    "r/dubai":           "https://www.reddit.com/r/dubai/new/.rss",
    "r/qatar":           "https://www.reddit.com/r/qatar/new/.rss",
    "r/saudiarabia":     "https://www.reddit.com/r/saudiarabia/new/.rss",
    "r/bahrain":         "https://www.reddit.com/r/bahrain/new/.rss",
    "r/lebanon":         "https://www.reddit.com/r/lebanon/new/.rss",
    "r/syria":           "https://www.reddit.com/r/syria/new/.rss",
    "r/iraq":            "https://www.reddit.com/r/iraq/new/.rss",
    "r/Iran":            "https://www.reddit.com/r/Iran/new/.rss",
    "r/Israel":          "https://www.reddit.com/r/Israel/new/.rss",
    "r/IsraelPalestine": "https://www.reddit.com/r/IsraelPalestine/new/.rss",
    "r/ukraine":         "https://www.reddit.com/r/ukraine/new/.rss",
    "r/CredibleDefense": "https://www.reddit.com/r/CredibleDefense/new/.rss",
    "r/LessCredibleDefence": "https://www.reddit.com/r/LessCredibleDefence/new/.rss",
    "r/worldnews":       "https://www.reddit.com/r/worldnews/new/.rss",
    "r/geopolitics":     "https://www.reddit.com/r/geopolitics/new/.rss",
    "r/Taiwan":          "https://www.reddit.com/r/Taiwan/new/.rss",
}

# Public Telegram channels, read via the read-only t.me/s/<name> preview page
# (no login/API key needed, but only shows recent posts and no historical scroll).
TELEGRAM_CHANNELS = {
    "FarsNA":     "farsna",
    "Kofia News": "Kofia_News",
    "PMTV NEWS":  "pm_afshaa",
    "NAYA":       "naya_foriraq",
    "ABUA":       "abualiexpress",
    "ALIB":       "Alibk3",
    "r3ado138e":  "rasedal3ado138e",
    "IRIBNEWS":  "iribnews",
}

NEWS_KEYWORDS = [
    "war", "conflict", "ceasefire", "cease-fire", "invasion", "invade",
    "military", "troops", "airstrike", "air strike", "missile",
    "casualties", "insurgency", "insurgent", "rebel", "coup",
    "sanctions", "clashes", "killed", "wounded", "offensive",
    "front line", "frontline", "peace talks", "occupation", "occupied",
    "militant", "terrorist", "attack", "shelling", "drone strike",
    "hostage", "genocide", "displaced", "refugee", "annexation",
    "airspace", "border clash", "unrest", "junta", "blockade", "clash",
    "explosions", "targeted", "targeting", "mobilization", "mobilisation",
    "deterrence", "escalation", "de-escalation", "proxy war",
]

REDDIT_KEYWORDS = [
    "siren", "sirens", "boom", "booms", "explosion", "explosions",
    "blast", "blasts", "rocket", "rockets", "missile", "missiles",
    "intercept", "interception", "loud bang", "loud noise", "shaking",
    "shook", "air raid", "red alert", "iron dome", "mushroom cloud",
    "gunfire", "shelling", "smoke rising", "drone", "strike",
]

# Any headline/post matching one of these (checked across ALL sources —
# news, reddit, telegram) gets flagged as a high-priority "alert" item.
ALERT_KEYWORDS = [
    "nuclear", "nuke", "wmd", "chemical weapon", "biological weapon",
    "mass casualty", "mass casualties", "declares war", "declaration of war",
    "martial law", "evacuation order", "evacuate immediately",
    "iron dome activated", "red alert", "incoming missile", "ballistic missile",
    "strike on capital", "assassination", "coup underway", "regime collapse",
    "hostage rescue", "chemical attack", "mushroom cloud", "full scale invasion",
    "full-scale invasion", "state of emergency",
]

# Lightweight public gazetteer: country/city name -> approx lat/lon, used
# only to plot where wire activity is clustering. Country/city resolution
# only — nothing here resolves to street level or individual people/devices.
GAZETTEER = {
    "israel": (31.5, 34.8), "gaza": (31.5, 34.45), "tel aviv": (32.08, 34.78),
    "jerusalem": (31.78, 35.22), "lebanon": (33.85, 35.86), "beirut": (33.89, 35.5),
    "syria": (34.8, 38.99), "damascus": (33.51, 36.28), "iraq": (33.22, 43.68),
    "baghdad": (33.31, 44.36), "iran": (32.43, 53.69), "tehran": (35.69, 51.39),
    "yemen": (15.55, 48.52), "saudi arabia": (23.89, 45.08), "riyadh": (24.71, 46.68),
    "qatar": (25.35, 51.18), "kuwait": (29.31, 47.48), "uae": (23.42, 53.85),
    "dubai": (25.2, 55.27), "bahrain": (26.07, 50.55), "jordan": (30.59, 36.24),
    "egypt": (26.82, 30.8), "cairo": (30.04, 31.24), "turkey": (38.96, 35.24),
    "ukraine": (48.38, 31.17), "kyiv": (50.45, 30.52), "kharkiv": (49.99, 36.23),
    "russia": (61.52, 105.32), "moscow": (55.75, 37.62), "crimea": (45.34, 34.4),
    "taiwan": (23.7, 121.0), "china": (35.86, 104.2), "beijing": (39.9, 116.4),
    "north korea": (40.34, 127.51), "south korea": (35.9, 127.77),
    "venezuela": (6.42, -66.59), "sudan": (12.86, 30.22), "somalia": (5.15, 46.2),
    "ethiopia": (9.15, 40.49), "libya": (26.34, 17.23), "mali": (17.57, -4.0),
    "pakistan": (30.38, 69.35), "afghanistan": (33.94, 67.71), "myanmar": (21.91, 95.96),
    "poland": (51.92, 19.15), "belarus": (53.71, 27.95), "armenia": (40.07, 45.04),
    "azerbaijan": (40.14, 47.58), "nagorno-karabakh": (39.83, 46.75),
}
GAZETTEER_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in sorted(GAZETTEER, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)

POLY_QUERIES = [
    "Iran war", "Israel Gaza", "Russia Ukraine", "China Taiwan",
    "North Korea", "Venezuela", "Houthi", "Hezbollah", "nuclear strike",
    "Lebanon Israel",
]

# Public price data via Yahoo Finance's public chart endpoint (free, no key).
# Used only as a rough "is risk being priced in" signal alongside the wire.
# Falls back to Stooq's CSV endpoint if Yahoo is unreachable.
FINANCE_SYMBOLS = {
    "Brent Crude": {"yahoo": "BZ=F",     "stooq": "cb.f"},
    "WTI Crude":   {"yahoo": "CL=F",     "stooq": "cl.f"},
    "Gold":        {"yahoo": "GC=F",     "stooq": "xauusd"},
    "USD/ILS":     {"yahoo": "ILS=X",    "stooq": "usdils"},
    "USD/TRY":     {"yahoo": "TRY=X",    "stooq": "usdtry"},
    "USD/RUB":     {"yahoo": "RUB=X",    "stooq": "usdrub"},
}

NEWS_POLL_SECONDS = 20
REDDIT_POLL_SECONDS = 20
TELEGRAM_POLL_SECONDS = 15
POLY_POLL_SECONDS = 300
FLIGHT_POLL_SECONDS = 30
FINANCE_POLL_SECONDS = 300
SEISMIC_POLL_SECONDS = 120
MAX_ITEMS = 400
MAX_TIMELINE = 1500
MAX_MARKETS = 24
MAX_FLIGHTS = 200
MAX_QUAKES = 40
PORT = 5000

POLL_SECONDS = min(NEWS_POLL_SECONDS, REDDIT_POLL_SECONDS, TELEGRAM_POLL_SECONDS)

NEWS_KEYWORD_RE = re.compile(r"\b(" + "|".join(re.escape(k) for k in NEWS_KEYWORDS) + r")\b", re.IGNORECASE)
REDDIT_KEYWORD_RE = re.compile(r"\b(" + "|".join(re.escape(k) for k in REDDIT_KEYWORDS) + r")\b", re.IGNORECASE)
ALERT_KEYWORD_RE = re.compile(r"\b(" + "|".join(re.escape(k) for k in ALERT_KEYWORDS) + r")\b", re.IGNORECASE)

# Rough CENTCOM area of responsibility bounding box.
CENTCOM_BBOX = {"lamin": -2, "lomin": 21, "lamax": 48, "lomax": 75}

MILITARY_CALLSIGN_HINTS = ("RCH", "NATO", "CFC", "IAM", "FAF", "RRR", "ASCOT", "NAF")

# Rough conflict-zone boxes used only to flag USGS quakes as "near an active
# reporting cluster" — informational cross-reference, not a claim of cause.
SEISMIC_WATCH_BOXES = {
    "Israel/Lebanon/Syria": (29, 37, 33, 43),
    "Iran":                 (25, 40, 44, 63),
    "Ukraine/Russia border": (44, 53, 22, 41),
    "Taiwan Strait":        (21, 26, 118, 123),
}

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

app = Flask(__name__)

feed_lock = threading.Lock()
headlines = deque(maxlen=MAX_ITEMS)
timeline = deque(maxlen=MAX_TIMELINE)   # persistent history, separate from live feed
seen_ids = set()
feed_status = {
    "last_poll": None, "feeds_ok": 0,
    "feeds_total": len(NEWS_FEEDS) + len(REDDIT_FEEDS) + len(TELEGRAM_CHANNELS),
    "feeds_failed": [], "total_seen": 0, "total_alerts": 0,
}
category_status = {
    "news": {"last_poll": None, "ok": 0, "total": len(NEWS_FEEDS), "failed": []},
    "reddit": {"last_poll": None, "ok": 0, "total": len(REDDIT_FEEDS), "failed": []},
    "telegram": {"last_poll": None, "ok": 0, "total": len(TELEGRAM_CHANNELS), "failed": []},
}

# Rolling per-hour-bucket counts per type, for the intensity graph.
INTENSITY_HOURS = 24
intensity_lock = threading.Lock()
intensity_buckets = defaultdict(lambda: defaultdict(int))  # {bucket_ts: {type: count}}

market_lock = threading.Lock()
markets = []
market_status = {"last_poll": None, "queries_ok": 0, "queries_total": len(POLY_QUERIES), "queries_failed": []}

flight_lock = threading.Lock()
flights = []
flight_status = {"last_poll": None, "ok": False, "count": 0, "error": None, "source": None}

finance_lock = threading.Lock()
finance_data = {}
finance_status = {"last_poll": None, "ok": 0, "total": len(FINANCE_SYMBOLS), "failed": []}

seismic_lock = threading.Lock()
quakes = []
seismic_status = {"last_poll": None, "ok": False, "error": None}

geo_lock = threading.Lock()
geo_counts = defaultdict(int)  # {place_name: mention_count} — rolling
geo_items = defaultdict(list)  # {place_name: [item, ...]} most-recent-first, capped
MAX_GEO_ITEMS_PER_PLACE = 12

# corroboration tracking: {alert_keyword: [(ts, source), ...]} for confidence scoring
corrob_lock = threading.Lock()
corroboration = defaultdict(list)
CORROB_WINDOW_SECONDS = 3600


def parse_time(entry):
    for field in ("published", "updated"):
        val = entry.get(field)
        if val:
            try:
                dt = parsedate_to_datetime(val)
                return dt.astimezone(EASTERN).strftime("%H:%M:%S"), dt.timestamp()
            except Exception:
                pass
    now = datetime.now(EASTERN)
    return now.strftime("%H:%M:%S"), now.timestamp()


def is_alert_text(text: str):
    match = ALERT_KEYWORD_RE.search(text)
    return bool(match), (match.group(0).lower() if match else None)


def _update_feed_status():
    ok_total = sum(c["ok"] for c in category_status.values())
    failed_total = []
    for c in category_status.values():
        failed_total.extend(c["failed"])
    latest_poll = max(
        (c["last_poll"] for c in category_status.values() if c["last_poll"]), default=None,
    )
    feed_status["last_poll"] = latest_poll
    feed_status["feeds_ok"] = ok_total
    feed_status["feeds_failed"] = failed_total


def _bucket_ts(ts):
    return int(ts // 3600) * 3600


def record_intensity(kind, ts):
    with intensity_lock:
        b = _bucket_ts(ts)
        intensity_buckets[b][kind] += 1
        cutoff = time.time() - INTENSITY_HOURS * 3600
        for k in [k for k in intensity_buckets if k < cutoff]:
            del intensity_buckets[k]


def record_geo(text, item=None):
    matches = set(m.lower() for m in GAZETTEER_RE.findall(text))
    if not matches:
        return
    with geo_lock:
        for m in matches:
            geo_counts[m] += 1
            if item is not None:
                geo_items[m].insert(0, {
                    "title": item["title"], "source": item["source"],
                    "link": item["link"], "time": item["time"], "type": item["type"],
                })
                del geo_items[m][MAX_GEO_ITEMS_PER_PLACE:]


def record_corroboration(alert_kw, source, ts):
    """Returns a rough confidence score (1..N distinct sources) for this alert
    keyword within the rolling corroboration window."""
    with corrob_lock:
        lst = corroboration[alert_kw]
        lst.append((ts, source))
        cutoff = ts - CORROB_WINDOW_SECONDS
        corroboration[alert_kw] = [x for x in lst if x[0] >= cutoff]
        distinct_sources = len(set(s for _, s in corroboration[alert_kw]))
        return distinct_sources


def make_item(kind, source, title, link, ts, time_str, keyword):
    alert, alert_kw = is_alert_text(title)
    confidence = record_corroboration(alert_kw, source, ts) if alert else None
    item = {
        "id": link, "type": kind, "source": source, "time": time_str, "ts": ts,
        "title": title, "keyword": keyword, "link": link,
        "alert": alert, "alert_keyword": alert_kw, "confidence": confidence,
    }
    record_intensity(kind, ts)
    record_geo(title, item)
    return item


def poll_group(feeds: dict, keyword_re: re.Pattern, kind: str):
    ok, failed = 0, []
    for source, url in feeds.items():
        try:
            parsed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})
            if parsed.bozo and not parsed.entries:
                failed.append(source)
                continue
            ok += 1
            for entry in parsed.entries[:30]:
                title = entry.get("title", "").strip()
                link = entry.get("link", "")
                if not title or not link or link in seen_ids:
                    continue
                match = keyword_re.search(title)
                if not match:
                    continue
                seen_ids.add(link)
                time_str, ts = parse_time(entry)
                item = make_item(kind, source, title, link, ts, time_str, match.group(0).lower())
                with feed_lock:
                    headlines.appendleft(item)
                    timeline.appendleft(item)
                    feed_status["total_seen"] += 1
                    if item["alert"]:
                        feed_status["total_alerts"] += 1
        except Exception:
            failed.append(source)
    return ok, failed


def fetch_json(url: str, timeout: float = 10.0, headers: dict = None):
    req_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_telegram_posts(channels: dict):
    posts, ok, failed = [], 0, []
    headers = {"User-Agent": "Mozilla/5.0"}
    for label, handle in channels.items():
        url = f"https://telegram.dog/s/{handle}"
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            ok += 1
            for msg in soup.select(".tgme_widget_message"):
                text_el = msg.select_one(".tgme_widget_message_text")
                link_el = msg.select_one("a.tgme_widget_message_date")
                if not link_el:
                    continue
                text = text_el.get_text(" ", strip=True) if text_el else "[MEDIA POST]"
                link = link_el["href"]
                time_el = msg.select_one("time")
                ts, time_str = None, None
                raw_dt = time_el.get("datetime") if time_el else None
                if raw_dt:
                    try:
                        normalized = raw_dt.strip()
                        if normalized.endswith("Z"):
                            normalized = normalized[:-1] + "+00:00"
                        dt = datetime.fromisoformat(normalized)
                        time_str = dt.astimezone(EASTERN).strftime("%H:%M:%S")
                        ts = dt.timestamp()
                    except Exception as e:
                        print(f"Telegram timestamp parse failed for {label} ({raw_dt!r}):", e)
                if ts is None:
                    ts = time.time()
                item = make_item("telegram", label, text[:280], link, ts,
                                  time_str or datetime.now(EASTERN).strftime("%H:%M:%S"), "telegram")
                posts.append(item)
        except Exception as e:
            failed.append(label)
            print(f"Telegram fetch error ({label}):", e)
    return posts, ok, failed


def news_loop():
    while True:
        ok, failed = poll_group(NEWS_FEEDS, NEWS_KEYWORD_RE, "news")
        with feed_lock:
            category_status["news"]["last_poll"] = datetime.now(EASTERN).strftime("%H:%M:%S")
            category_status["news"]["ok"] = ok
            category_status["news"]["failed"] = failed
            _update_feed_status()
        time.sleep(NEWS_POLL_SECONDS)


def reddit_loop():
    while True:
        ok, failed = poll_group(REDDIT_FEEDS, REDDIT_KEYWORD_RE, "reddit")
        with feed_lock:
            category_status["reddit"]["last_poll"] = datetime.now(EASTERN).strftime("%H:%M:%S")
            category_status["reddit"]["ok"] = ok
            category_status["reddit"]["failed"] = failed
            _update_feed_status()
        time.sleep(REDDIT_POLL_SECONDS)


def telegram_loop():
    while True:
        tg_posts, ok, failed = fetch_telegram_posts(TELEGRAM_CHANNELS)
        with feed_lock:
            for p in tg_posts:
                if p["id"] not in seen_ids:
                    seen_ids.add(p["id"])
                    headlines.appendleft(p)
                    timeline.appendleft(p)
                    feed_status["total_seen"] += 1
                    if p["alert"]:
                        feed_status["total_alerts"] += 1
            category_status["telegram"]["last_poll"] = datetime.now(EASTERN).strftime("%H:%M:%S")
            category_status["telegram"]["ok"] = ok
            category_status["telegram"]["failed"] = failed
            _update_feed_status()
        time.sleep(TELEGRAM_POLL_SECONDS)


def poly_loop():
    while True:
        ok, failed = 0, []
        collected = {}
        for query in POLY_QUERIES:
            try:
                url = ("https://gamma-api.polymarket.com/public-search?q="
                       + urllib.parse.quote(query) + "&events_status=active&limit_per_type=8")
                data = fetch_json(url)
                ok += 1
                for event in (data.get("events") or []):
                    event_slug = event.get("slug") or ""
                    for m in (event.get("markets") or []):
                        mid = m.get("id") or m.get("conditionId")
                        if not mid or mid in collected:
                            continue
                        try:
                            outcomes = json.loads(m.get("outcomes") or "[]")
                            prices = [float(p) for p in json.loads(m.get("outcomePrices") or "[]")]
                        except Exception:
                            outcomes, prices = [], []
                        if not outcomes or not prices or len(outcomes) != len(prices):
                            continue
                        best_idx = max(range(len(prices)), key=lambda i: prices[i])
                        volume = m.get("volumeNum") or 0
                        try:
                            volume = float(volume)
                        except Exception:
                            volume = 0.0
                        collected[mid] = {
                            "id": mid,
                            "question": m.get("question") or event.get("title") or "Untitled market",
                            "leading_outcome": outcomes[best_idx],
                            "leading_price": round(prices[best_idx] * 100, 1),
                            "volume": volume,
                            "link": f"https://polymarket.com/event/{event_slug}" if event_slug else "https://polymarket.com",
                            "matched_query": query,
                        }
            except Exception:
                failed.append(query)
        ranked = sorted(collected.values(), key=lambda m: m["volume"], reverse=True)[:MAX_MARKETS]
        with market_lock:
            markets.clear()
            markets.extend(ranked)
            market_status["last_poll"] = datetime.now(EASTERN).strftime("%H:%M:%S")
            market_status["queries_ok"] = ok
            market_status["queries_failed"] = failed
        time.sleep(POLY_POLL_SECONDS)


FLIGHT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# Grid of center points (lat, lon) covering the CENTCOM AOR bbox, queried
# with airplanes.live's free point+radius endpoint (no key/signup needed).
# Radius is capped at 250nm by that API, so one point can't cover the whole
# AOR — several overlapping points approximate it instead.
_bb = CENTCOM_BBOX
FLIGHT_GRID_POINTS = [
    (_bb["lamax"] - 6, (_bb["lomin"] + _bb["lomax"]) / 2),   # north
    (_bb["lamin"] + 6, (_bb["lomin"] + _bb["lomax"]) / 2),   # south
    ((_bb["lamin"] + _bb["lamax"]) / 2, _bb["lomin"] + 6),   # west
    ((_bb["lamin"] + _bb["lamax"]) / 2, _bb["lomax"] - 6),   # east
    ((_bb["lamin"] + _bb["lamax"]) / 2, (_bb["lomin"] + _bb["lomax"]) / 2),  # center
    (31.5, 45),   # Gulf/Levant
    (25, 55),     # Gulf states
]


def _fetch_airplanes_live_point(lat, lon, radius_nm=250):
    """airplanes.live public API — no key or account required."""
    url = f"https://api.airplanes.live/v2/point/{lat}/{lon}/{radius_nm}"
    r = requests.get(url, timeout=10, headers=FLIGHT_HEADERS)
    r.raise_for_status()
    data = r.json()
    return data.get("ac") or []


def _parse_airplanes_live_aircraft(ac_list):
    parsed = {}
    for ac in ac_list:
        icao24 = ac.get("hex")
        lat, lon = ac.get("lat"), ac.get("lon")
        if not icao24 or lat is None or lon is None:
            continue
        if ac.get("alt_baro") in ("ground", None) and not ac.get("alt_geom"):
            continue
        callsign = (ac.get("flight") or "").strip()
        is_military_hint = bool(ac.get("dbFlags", 0) & 1) or (
            bool(callsign) and callsign.upper().startswith(MILITARY_CALLSIGN_HINTS)
        )
        alt = ac.get("alt_baro")
        alt_ft = alt if isinstance(alt, (int, float)) else None
        parsed[icao24] = {
            "icao24": icao24, "callsign": callsign or "—",
            "origin_country": ac.get("r") or ac.get("t") or "unknown",
            "lat": lat, "lon": lon,
            "alt_ft": round(alt_ft) if alt_ft is not None else None,
            "speed_kt": round(ac.get("gs")) if ac.get("gs") is not None else None,
            "heading": round(ac.get("track")) if ac.get("track") is not None else None,
            "squawk": ac.get("squawk"), "military_hint": is_military_hint,
        }
    return parsed


def _fetch_opensky_bbox():
    """Fallback: OpenSky's anonymous states endpoint (no key, but rate-limited)."""
    url = ("https://opensky-network.org/api/states/all"
           f"?lamin={CENTCOM_BBOX['lamin']}&lomin={CENTCOM_BBOX['lomin']}"
           f"&lamax={CENTCOM_BBOX['lamax']}&lomax={CENTCOM_BBOX['lomax']}")
    r = requests.get(url, timeout=15.0, headers=FLIGHT_HEADERS)
    r.raise_for_status()
    data = r.json()
    states = data.get("states") or []
    parsed = {}
    for s in states:
        icao24, callsign, origin_country = s[0], (s[1] or "").strip(), s[2]
        lon, lat, baro_alt, on_ground = s[5], s[6], s[7], s[8]
        velocity, true_track, squawk = s[9], s[10], s[14]
        if on_ground or lat is None or lon is None:
            continue
        is_military_hint = bool(callsign) and callsign.upper().startswith(MILITARY_CALLSIGN_HINTS)
        parsed[icao24] = {
            "icao24": icao24, "callsign": callsign or "—", "origin_country": origin_country,
            "lat": lat, "lon": lon,
            "alt_ft": round(baro_alt * 3.28084) if baro_alt else None,
            "speed_kt": round(velocity * 1.94384) if velocity else None,
            "heading": round(true_track) if true_track is not None else None,
            "squawk": squawk, "military_hint": is_military_hint,
        }
    return parsed


def poll_flights():
    """Best-effort CENTCOM-AOR ADS-B, entirely keyless.

    Primary source: airplanes.live's free point+radius API (no account or
    key needed at all) — queried across several grid points to approximate
    the AOR bbox. Falls back to OpenSky's anonymous endpoint (also keyless,
    but more heavily rate-limited) if airplanes.live is unreachable.
    """
    consecutive_failures = 0
    while True:
        combined = {}
        source_used = None
        errors = []
        try:
            for lat, lon in FLIGHT_GRID_POINTS:
                try:
                    ac_list = _fetch_airplanes_live_point(lat, lon)
                    combined.update(_parse_airplanes_live_aircraft(ac_list))
                except Exception as e:
                    errors.append(f"airplanes.live @({lat},{lon}): {e}")
            if combined:
                source_used = "airplanes.live"
        except Exception as e:
            errors.append(f"airplanes.live grid: {e}")

        if not combined:
            try:
                combined = _fetch_opensky_bbox()
                if combined:
                    source_used = "opensky"
            except Exception as e:
                errors.append(f"opensky: {e}")

        if combined:
            parsed = list(combined.values())
            parsed.sort(key=lambda f: (not f["military_hint"], f["callsign"]))
            parsed = parsed[:MAX_FLIGHTS]
            with flight_lock:
                flights.clear()
                flights.extend(parsed)
                flight_status["last_poll"] = datetime.now(EASTERN).strftime("%H:%M:%S")
                flight_status["ok"] = True
                flight_status["count"] = len(parsed)
                flight_status["error"] = None
                flight_status["source"] = source_used
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            err_msg = "; ".join(errors) or "no data returned"
            print(f"Flight fetch failed ({consecutive_failures}x): {err_msg}")
            with flight_lock:
                flight_status["ok"] = False
                flight_status["error"] = err_msg

        sleep_for = FLIGHT_POLL_SECONDS * min(6, 1 + consecutive_failures)
        time.sleep(sleep_for)


FINANCE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json,text/csv,*/*",
}


def _fetch_yahoo_quote(yahoo_symbol: str):
    """Yahoo Finance's public chart endpoint — free, no key, widely used.
    Returns (price, date_str) or raises on failure."""
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
           "?interval=1d&range=1d")
    r = requests.get(url, timeout=10, headers=FINANCE_HEADERS)
    r.raise_for_status()
    data = r.json()
    result = (data.get("chart") or {}).get("result") or []
    if not result:
        raise ValueError("empty chart result")
    meta = result[0].get("meta", {})
    price = meta.get("regularMarketPrice")
    if price is None:
        raise ValueError("no regularMarketPrice in response")
    ts = meta.get("regularMarketTime")
    date_str = datetime.fromtimestamp(ts, EASTERN).strftime("%H:%M ET") if ts else None
    return float(price), date_str


def _fetch_stooq_quote(stooq_symbol: str):
    """Fallback: Stooq's CSV quote endpoint."""
    url = f"https://stooq.com/q/l/?s={stooq_symbol}&f=sd2t2ohlcv&h&e=csv"
    r = requests.get(url, timeout=10, headers=FINANCE_HEADERS)
    r.raise_for_status()
    lines = r.text.strip().splitlines()
    if len(lines) < 2:
        raise ValueError("no data rows returned")
    row = lines[1].split(",")
    if len(row) <= 6 or row[6] in ("N/D", ""):
        raise ValueError("close price unavailable (N/D)")
    return float(row[6]), (row[1] if len(row) > 1 else None)


def finance_loop():
    """Yahoo Finance primary, Stooq fallback. Used as a rough risk-pricing signal."""
    while True:
        ok, failed, results = 0, [], {}
        for label, symbols in FINANCE_SYMBOLS.items():
            price, date_str, err = None, None, None
            try:
                price, date_str = _fetch_yahoo_quote(symbols["yahoo"])
            except Exception as e:
                err = f"yahoo: {e}"
                try:
                    price, date_str = _fetch_stooq_quote(symbols["stooq"])
                    err = None
                except Exception as e2:
                    err = f"{err} / stooq: {e2}"
            if price is not None:
                results[label] = {"label": label, "price": round(price, 4), "date": date_str}
                ok += 1
            else:
                failed.append(label)
                print(f"Finance fetch failed for {label}: {err}")
        with finance_lock:
            finance_data.clear()
            finance_data.update(results)
            finance_status["last_poll"] = datetime.now(EASTERN).strftime("%H:%M:%S")
            finance_status["ok"] = ok
            finance_status["failed"] = failed
        time.sleep(FINANCE_POLL_SECONDS)


def _in_box(lat, lon, box):
    lat_min, lat_max, lon_min, lon_max = box
    return lat_min <= lat <= lat_max and lon_min <= lon <= lon_max


def seismic_loop():
    """Public USGS feed — magnitude 3.5+, last day. Informational cross-reference only."""
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/3.5_day.geojson"
    while True:
        try:
            data = fetch_json(url, timeout=15.0)
            parsed = []
            for feat in (data.get("features") or [])[:MAX_QUAKES]:
                props = feat.get("properties", {})
                coords = feat.get("geometry", {}).get("coordinates", [None, None, None])
                lon, lat = coords[0], coords[1]
                if lat is None or lon is None:
                    continue
                near_watch = None
                for zone, box in SEISMIC_WATCH_BOXES.items():
                    if _in_box(lat, lon, box):
                        near_watch = zone
                        break
                ts = (props.get("time") or 0) / 1000
                parsed.append({
                    "place": props.get("place", "unknown"),
                    "mag": props.get("mag"),
                    "lat": lat, "lon": lon, "ts": ts,
                    "time": datetime.fromtimestamp(ts, EASTERN).strftime("%H:%M:%S") if ts else "—",
                    "near_watch_zone": near_watch,
                    "url": props.get("url"),
                })
            with seismic_lock:
                quakes.clear()
                quakes.extend(parsed)
                seismic_status["last_poll"] = datetime.now(EASTERN).strftime("%H:%M:%S")
                seismic_status["ok"] = True
                seismic_status["error"] = None
        except Exception as e:
            with seismic_lock:
                seismic_status["ok"] = False
                seismic_status["error"] = str(e)
        time.sleep(SEISMIC_POLL_SECONDS)


@app.route("/api/data")
def api_data():
    with feed_lock:
        items = sorted(headlines, key=lambda h: h["ts"], reverse=True)
        status_copy = dict(feed_status)
        category_status_copy = {k: dict(v) for k, v in category_status.items()}
    with market_lock:
        markets_copy, mstatus_copy = list(markets), dict(market_status)
    with flight_lock:
        flights_copy, fstatus_copy = list(flights), dict(flight_status)
    with finance_lock:
        finance_copy, finstatus_copy = dict(finance_data), dict(finance_status)
    with seismic_lock:
        quakes_copy, sstatus_copy = list(quakes), dict(seismic_status)
    with geo_lock:
        geo_copy = [
            {
                "place": p, "lat": GAZETTEER[p][0], "lon": GAZETTEER[p][1], "count": c,
                "items": list(geo_items.get(p, [])),
            }
            for p, c in geo_counts.items() if p in GAZETTEER
        ]
    with intensity_lock:
        now_bucket = _bucket_ts(time.time())
        buckets = []
        for i in range(INTENSITY_HOURS - 1, -1, -1):
            b = now_bucket - i * 3600
            counts = intensity_buckets.get(b, {})
            buckets.append({
                "bucket": datetime.fromtimestamp(b, EASTERN).strftime("%H:00"),
                "news": counts.get("news", 0),
                "reddit": counts.get("reddit", 0),
                "telegram": counts.get("telegram", 0),
            })
    return jsonify({
        "headlines": items,
        "status": status_copy,
        "category_status": category_status_copy,
        "markets": markets_copy,
        "market_status": mstatus_copy,
        "flights": flights_copy,
        "flight_status": fstatus_copy,
        "finance": finance_copy,
        "finance_status": finstatus_copy,
        "quakes": quakes_copy,
        "seismic_status": sstatus_copy,
        "geo": sorted(geo_copy, key=lambda g: g["count"], reverse=True)[:30],
        "intensity": buckets,
        "poll_seconds": POLL_SECONDS,
    })


@app.route("/api/timeline")
def api_timeline():
    with feed_lock:
        items = list(timeline)
    return jsonify({"timeline": items})


# ---------------------------------------------------------------------------
# Frontend
# ---------------------------------------------------------------------------

PAGE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>WIRE // Global Conflict Monitor</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0b0a08; --panel: #14110c; --hairline: #2a2318;
    --amber: #ffb627; --amber-bright: #ffd580; --amber-dim: #8a6d3b; --amber-faint: #4a3c22;
    --red: #ff4d3d; --green: #6bbf6b; --blue: #6bb3ff; --purple: #b98bff;
  }
  * { box-sizing: border-box; }
  html, body { margin:0; padding:0; height:100%; background:var(--bg); color:var(--amber);
    font-family:'IBM Plex Mono', monospace; overflow:hidden; }
  body { display:flex; flex-direction:column;
    background-image: repeating-linear-gradient(0deg, rgba(255,182,39,0.018) 0px, rgba(255,182,39,0.018) 1px, transparent 1px, transparent 3px); }

  header { padding:14px 20px 10px; border-bottom:1px solid var(--hairline); flex-shrink:0; }
  .title-row { display:flex; align-items:baseline; justify-content:space-between; }
  .title { font-size:20px; font-weight:700; letter-spacing:0.12em; color:var(--amber-bright);
    text-shadow:0 0 12px rgba(255,182,39,0.35); }
  .title .cursor { display:inline-block; width:9px; height:16px; background:var(--amber-bright);
    margin-left:4px; vertical-align:-3px; animation:blink 1.1s steps(1) infinite; }
  @keyframes blink { 50% { opacity:0; } }
  .subtitle { margin-top:4px; font-size:11px; color:var(--amber-faint); letter-spacing:0.06em; }
  .nav { display:flex; gap:4px; }
  .nav button { background:transparent; border:1px solid var(--hairline); color:var(--amber-dim);
    font-family:inherit; font-size:10.5px; padding:5px 10px; cursor:pointer; letter-spacing:0.05em; }
  .nav button.active { background:var(--amber-faint); color:#1a1608; }

  .ticker-wrap { border-bottom:1px solid var(--hairline); background:var(--panel); overflow:hidden;
    white-space:nowrap; flex-shrink:0; position:relative; }
  .ticker-wrap::before { content:"BREAKING"; position:absolute; left:0; top:0; bottom:0;
    display:flex; align-items:center; padding:0 12px; background:var(--red); color:#1a0a08;
    font-size:11px; font-weight:700; letter-spacing:0.1em; z-index:2; }
  .ticker-track { display:inline-block; animation:scroll-left var(--ticker-duration,40s) linear infinite;
    font-size:12.5px; color:var(--amber); padding-top:8px; padding-bottom:8px; padding-left:calc(100% + 100px); }
  .ticker-track span.sep { color:var(--amber-faint); margin:0 22px; }
  .ticker-track span.alert-tick { color:var(--red); font-weight:700; }
  @keyframes scroll-left { from{transform:translateX(0);} to{transform:translateX(-100%);} }

  .panels-row { display:flex; border-bottom:1px solid var(--hairline); background:var(--panel); flex-shrink:0; flex-wrap:wrap; }
  .panel-col { flex:1; min-width:260px; border-right:1px solid var(--hairline); padding:6px 14px 10px; }
  .panel-col:last-child { border-right:none; }
  .panel-label { font-size:10px; letter-spacing:0.1em; color:var(--amber-faint); margin-bottom:4px;
    display:flex; justify-content:space-between; }
  .panel-label .note { font-size:9px; font-weight:400; }

  .strip { display:flex; gap:8px; overflow-x:auto; padding-bottom:2px; }
  .strip::-webkit-scrollbar { height:5px; }
  .strip::-webkit-scrollbar-thumb { background:var(--amber-faint); }
  .mini-card { flex:0 0 150px; border:1px solid var(--hairline); padding:6px 8px; background:rgba(255,182,39,0.02); text-decoration:none; color:inherit; }
  .mini-card:hover { border-color:var(--amber-dim); }
  .mini-card .q { font-size:10.5px; color:var(--amber-bright); line-height:1.25; display:-webkit-box;
    -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; min-height:26px; }
  .mini-card .row { display:flex; justify-content:space-between; align-items:baseline; margin-top:5px; }
  .mini-card .pct { font-size:14px; font-weight:700; }
  .mini-card .pct.high { color:var(--green); } .mini-card .pct.low { color:var(--red); } .mini-card .pct.mid { color:var(--amber); }
  .mini-card .sub { font-size:9px; color:var(--amber-faint); }
  .mini-card.mil { border-color:var(--blue); background:rgba(107,179,255,0.06); }
  .mini-card.mil .q { color:var(--blue); }
  .mini-card.quake { border-color:var(--purple); }
  .mini-card.quake.watch { border-color:var(--red); background:rgba(255,77,61,0.06); }

  canvas#intensity-canvas { width:100%; height:70px; display:block; }

  #geo-map { position:relative; width:100%; height:220px; overflow:hidden; border:1px solid var(--hairline);
    background:#05070a; cursor:grab; touch-action:none; }
  #geo-map.grabbing { cursor:grabbing; }
  #geo-map.fullscreen { position:fixed; inset:24px; width:auto; height:auto; z-index:1000;
    border:1px solid var(--amber-dim); box-shadow:0 0 0 2000px rgba(0,0,0,0.82), 0 0 40px rgba(0,0,0,0.8); }
  #geo-map-backdrop { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.7); z-index:999; }
  #geo-map-backdrop.show { display:block; }
  #geo-map-inner { position:absolute; left:0; top:0; width:100%; height:100%; transform-origin:0 0; will-change:transform; }
  #geo-map-inner img { width:100%; height:100%; object-fit:fill; display:block; pointer-events:none;
    filter:grayscale(1) brightness(0.55) contrast(1.15) sepia(0.55) hue-rotate(-15deg) saturate(2.2); }
  #geo-zoom-controls { position:absolute; right:6px; bottom:6px; z-index:5; display:flex; flex-direction:column; gap:2px; }
  #geo-zoom-controls button { width:22px; height:22px; background:var(--panel); border:1px solid var(--amber-dim);
    color:var(--amber); font-family:inherit; font-size:13px; line-height:1; cursor:pointer; padding:0; }
  #geo-zoom-controls button:hover { background:var(--amber-faint); color:#1a1608; }
  .geo-arrow { position:absolute; transform:translate(-50%,-100%); cursor:pointer; z-index:3; }
  .geo-arrow .pin { width:0; height:0; border-left:6px solid transparent; border-right:6px solid transparent;
    border-top:12px solid var(--red); filter:drop-shadow(0 0 3px rgba(255,77,61,0.9)); margin:0 auto; }
  .geo-arrow .pin-dot { width:6px; height:6px; border-radius:50%; background:var(--red); margin:0 auto -1px;
    box-shadow:0 0 6px rgba(255,77,61,0.9); }
  .geo-arrow .count-badge { position:absolute; top:-8px; left:50%; transform:translateX(-50%);
    font-size:8px; color:#1a0a08; background:var(--amber-bright); border-radius:6px; padding:0 3px; font-weight:700; line-height:1.3; }
  .geo-arrow:hover .pin { border-top-color:var(--amber-bright); }
  .geo-arrow.active .pin { border-top-color:var(--amber-bright); }
  .geo-label { position:absolute; top:2px; left:50%; transform:translateX(-50%); font-size:8px; color:var(--amber-bright);
    white-space:nowrap; text-shadow:0 0 3px #000, 0 0 3px #000; pointer-events:none; }

  #geo-popup { display:none; position:fixed; z-index:1001; max-width:280px; background:var(--panel);
    border:1px solid var(--amber-dim); padding:8px 10px; font-size:11px; box-shadow:0 4px 16px rgba(0,0,0,0.6); }
  #geo-popup .gp-title { color:var(--amber-bright); font-weight:700; letter-spacing:0.05em; margin-bottom:6px;
    display:flex; justify-content:space-between; }
  #geo-popup .gp-close { cursor:pointer; color:var(--amber-faint); }
  #geo-popup .gp-story { padding:4px 0; border-top:1px solid var(--hairline); }
  #geo-popup .gp-story:first-of-type { border-top:none; }
  #geo-popup .gp-story a { color:var(--amber); text-decoration:none; line-height:1.35; display:block; }
  #geo-popup .gp-story a:hover { text-decoration:underline; }
  #geo-popup .gp-story .gp-src { color:var(--amber-faint); font-size:9.5px; }
  #geo-popup .gp-tweet { margin-top:5px; display:flex; gap:5px; align-items:flex-start; }
  #geo-popup .gp-tweet textarea { flex:1; resize:vertical; min-height:36px; background:rgba(255,182,39,0.03);
    border:1px dashed var(--hairline); color:var(--amber-bright); font-family:inherit; font-size:10px; padding:3px 5px; }
  #geo-popup .gp-tweet button { flex-shrink:0; background:var(--panel); border:1px solid var(--amber-dim);
    color:var(--amber); font-family:inherit; font-size:9px; padding:3px 6px; cursor:pointer; }
  #geo-popup .gp-tweet button.copied { background:var(--green); color:#0a1a0a; border-color:var(--green); }

  .toolbar { display:flex; flex-wrap:wrap; align-items:center; gap:8px; padding:10px 20px;
    border-bottom:1px solid var(--hairline); flex-shrink:0; }
  .toolbar input { background:var(--panel); border:1px solid var(--hairline); color:var(--amber);
    font-family:inherit; font-size:12px; padding:7px 10px; letter-spacing:0.02em; outline:none; }
  .toolbar input#search { flex:1.4; min-width:140px; }
  .toolbar input#exclude { flex:1; min-width:140px; }
  .toolbar input::placeholder { color:var(--amber-faint); }
  .toolbar input:focus { border-color:var(--amber-dim); }

  .type-toggle { display:flex; border:1px solid var(--hairline); }
  .type-toggle button { background:transparent; border:none; color:var(--amber-faint); font-family:inherit;
    font-size:11px; padding:7px 10px; cursor:pointer; letter-spacing:0.05em; border-right:1px solid var(--hairline); }
  .type-toggle button:last-child { border-right:none; }
  .type-toggle button.active { background:var(--amber-faint); color:#1a1608; }
  .type-toggle button[data-type="alerts"].active { background:var(--red); color:#1a0a08; }

  .quick-filters { display:flex; gap:6px; flex-wrap:wrap; padding:0 20px 10px; }
  .chip { border:1px solid var(--hairline); color:var(--amber-dim); font-size:10.5px; padding:4px 9px;
    cursor:pointer; background:transparent; font-family:inherit; letter-spacing:0.03em; }
  .chip:hover { border-color:var(--amber-dim); color:var(--amber); }
  .chip.clear { color:var(--red); border-color:var(--red); opacity:0.7; }
  #clear-toggle.active { background:var(--amber); color:#1a1608; border-color:var(--amber); opacity:1; font-weight:700; }

  main { flex:1; overflow-y:auto; padding:0 0 20px; }
  main::-webkit-scrollbar { width:10px; }
  main::-webkit-scrollbar-thumb { background:var(--amber-faint); }
  main::-webkit-scrollbar-track { background:var(--bg); }

  .entry { display:grid; grid-template-columns:72px 140px 1fr 70px 90px; gap:14px; padding:9px 20px 9px 16px;
    border-bottom:1px solid var(--hairline); border-left:3px solid var(--type-color,var(--amber-faint));
    font-size:13px; align-items:baseline; }
  .entry-tweet { grid-column:1 / -1; margin-top:6px; padding:6px 8px; background:rgba(255,182,39,0.03);
    border:1px dashed var(--hairline); display:flex; gap:8px; align-items:flex-start; }
  .entry-tweet .tweet-label { flex-shrink:0; font-size:9.5px; color:var(--amber-faint); letter-spacing:0.05em;
    padding-top:3px; white-space:nowrap; }
  .entry-tweet textarea.tweet-text { flex:1; resize:vertical; min-height:44px; background:transparent;
    border:none; outline:none; color:var(--amber-bright); font-family:inherit; font-size:11px; line-height:1.4; }
  .entry-tweet .tweet-copy-btn { flex-shrink:0; background:var(--panel); border:1px solid var(--amber-dim);
    color:var(--amber); font-family:inherit; font-size:10px; padding:4px 9px; cursor:pointer; align-self:flex-start; }
  .entry-tweet .tweet-copy-btn:hover { background:var(--amber-faint); color:#1a1608; }
  .entry-tweet .tweet-copy-btn.copied { background:var(--green); color:#0a1a0a; border-color:var(--green); }
  .entry:hover { background:rgba(255,182,39,0.04); }
  .entry .time { color:var(--amber-faint); font-variant-numeric:tabular-nums; }
  .entry .source { color:var(--src-color,var(--amber-dim)); font-weight:600; letter-spacing:0.03em; }
  .entry .headline { color:var(--amber-bright); line-height:1.4; }
  .entry .headline a { color:inherit; text-decoration:none; }
  .entry .headline a:hover { text-decoration:underline; }
  .entry .tag { justify-self:end; font-size:10px; color:var(--amber-dim); border:1px solid var(--amber-faint);
    padding:2px 6px; letter-spacing:0.05em; height:fit-content; }
  .entry .confidence { justify-self:end; font-size:10px; height:fit-content; }

  .entry.alert { border-left-color:var(--red); background:rgba(255,77,61,0.08); }
  .entry.alert .time { color:#ff9a90; } .entry.alert .source { color:var(--red); }
  .entry.alert .headline { color:#ffb3ab; font-weight:600; }
  .entry.alert .tag { color:var(--red); border-color:var(--red); }

  .entry.new { animation:fadein 500ms ease-out; }
  @keyframes fadein { from{background:rgba(255,182,39,0.16);opacity:0;transform:translateY(-3px);} to{background:transparent;opacity:1;transform:translateY(0);} }
  .entry.alert.new { animation:fadein-alert 700ms ease-out; }
  @keyframes fadein-alert { from{background:rgba(255,77,61,0.35);opacity:0;transform:translateY(-3px);} to{background:rgba(255,77,61,0.08);opacity:1;transform:translateY(0);} }

  .empty { padding:60px 20px; text-align:center; color:var(--amber-faint); font-size:13px; }

  footer { display:flex; justify-content:space-between; align-items:center; padding:8px 20px;
    border-top:1px solid var(--hairline); background:var(--panel); font-size:11px; color:var(--amber-faint);
    flex-shrink:0; letter-spacing:0.03em; flex-wrap:wrap; gap:6px; }
  footer .ok { color:var(--green); } footer .fail { color:var(--red); } footer .alert-count { color:var(--red); font-weight:700; }

  @media (prefers-reduced-motion: reduce) { .ticker-track{animation:none;} .entry.new{animation:none;} }
</style>
</head>
<body>

<header>
  <div class="title-row">
    <div class="title">WIRE // GLOBAL CONFLICT MONITOR<span class="cursor"></span></div>
    <div class="nav" id="view-nav">
      <button data-view="live" class="active">LIVE WIRE</button>
      <button data-view="timeline">TIMELINE</button>
    </div>
  </div>
  <div class="subtitle">public news + reddit + telegram wire scan · markets · seismic · flights · geo mentions · times in ET</div>
</header>

<div class="ticker-wrap"><div class="ticker-track" id="ticker">loading wire...</div></div>

<div class="panels-row">
  <div class="panel-col" style="flex:1.3;">
    <div class="panel-label"><span>INTENSITY · EVENTS/HR (24H)</span><span class="note">news=amber reddit=red telegram=blue</span></div>
    <canvas id="intensity-canvas"></canvas>
  </div>
  <div class="panel-col" style="flex:1.3; position:relative;">
    <div class="panel-label"><span>GEO MENTIONS · COUNTRY/CITY LEVEL</span><span class="note">gazetteer match, not device geolocation — drag to pan, scroll/buttons to zoom, click an arrow for stories</span></div>
    <div id="geo-map">
      <div id="geo-map-inner">
        <img id="geo-map-img" src="https://upload.wikimedia.org/wikipedia/commons/8/83/Equirectangular_projection_SW.jpg" alt="world map">
      </div>
      <div id="geo-zoom-controls">
        <button id="geo-zoom-in" title="zoom in">+</button>
        <button id="geo-zoom-out" title="zoom out">−</button>
        <button id="geo-zoom-reset" title="reset view">⤾</button>
        <button id="geo-popout" title="pop out map">⛶</button>
      </div>
    </div>
    <div id="geo-popup"></div>
  </div>
</div>
<div id="geo-map-backdrop"></div>

<div class="panels-row">
  <div class="panel-col">
    <div class="panel-label"><span>PREDICTION MARKETS · POLYMARKET</span></div>
    <div class="strip" id="markets">loading...</div>
  </div>
  <div class="panel-col">
    <div class="panel-label"><span>OIL / GOLD / FX</span><span class="note" id="finance-note">yahoo finance / stooq fallback</span></div>
    <div class="strip" id="finance">loading...</div>
  </div>
  <div class="panel-col">
    <div class="panel-label"><span>SEISMIC · USGS M3.5+</span><span class="note">quakes near watch zones only, informational</span></div>
    <div class="strip" id="quakes">loading...</div>
  </div>
  <div class="panel-col">
    <div class="panel-label"><span>CENTCOM AOR AIR ACTIVITY</span><span class="note" id="flight-note">airplanes.live / opensky · no key needed · civilian-visible only</span></div>
    <div class="strip" id="flights">loading...</div>
  </div>
</div>

<div class="toolbar">
  <input id="search" type="text" placeholder="search (comma-separated terms, OR)...">
  <input id="exclude" type="text" placeholder="exclude (comma-separated terms)...">
  <div class="type-toggle" id="type-toggle">
    <button data-type="all" class="active">ALL</button>
    <button data-type="news">NEWS</button>
    <button data-type="reddit">REDDIT</button>
    <button data-type="telegram">TELEGRAM</button>
    <button data-type="alerts">ALERTS</button>
  </div>
  <button id="clear-toggle" class="chip">CLEAR &amp; SHOW NEW ONLY</button>
</div>
<div class="quick-filters" id="quick-filters"></div>

<main id="feed"><div class="empty">tuning in to the wire... first results in under a minute</div></main>

<footer>
  <span id="poll-info">last poll: —</span>
  <span id="feed-status">feeds: —</span>
  <span id="total-count">matched: 0</span>
  <span id="alert-count">alerts: 0</span>
  <span id="flight-status">flights: —</span>
  <span id="seismic-status">seismic: —</span>
</footer>

<script>
const COLORS = ["#ffb627","#ff8c42","#ffd580","#e0a458","#c9822e","#ffc857","#d9a441","#f2b04c"];
function colorFor(source) { let h=0; for (const c of source) h=(h*31+c.charCodeAt(0))%COLORS.length; return COLORS[h]; }
const TYPE_COLOR = { news:"#4a3c22", reddit:"#ff4d3d", telegram:"#6bb3ff" };

let knownIds = new Set();
let firstLoad = true;
let lastData = null;
let typeFilter = "all";
let clearActive = false;
let clearedAt = null;
let currentView = "live";
let timelineData = null;

document.getElementById('view-nav').addEventListener('click', async (e) => {
  const btn = e.target.closest('button'); if (!btn) return;
  currentView = btn.dataset.view;
  [...document.getElementById('view-nav').children].forEach(b => b.classList.toggle('active', b===btn));
  if (currentView === 'timeline' && !timelineData) {
    const res = await fetch('/api/timeline');
    timelineData = (await res.json()).timeline;
  }
  renderFromCache();
});

document.getElementById('clear-toggle').addEventListener('click', () => {
  clearActive = !clearActive;
  const btn = document.getElementById('clear-toggle');
  btn.classList.toggle('active', clearActive);
  if (clearActive) { clearedAt = Date.now()/1000; btn.textContent = 'SHOW ALL AGAIN'; }
  else { clearedAt = null; btn.textContent = 'CLEAR & SHOW NEW ONLY'; }
  renderFromCache();
});

const NEW_WINDOW_SECONDS = 180;
function isActuallyRecent(h) { return (Date.now()/1000 - h.ts) <= NEW_WINDOW_SECONDS; }

const QUICK = ["Israel", "Iran", "Gulf", "Ukraine", "Kuwait", "Gaza", "Taiwan", "Russia"];
document.getElementById('quick-filters').innerHTML =
  QUICK.map(q => `<button class="chip" data-q="${q}">${q}</button>`).join('') +
  `<button class="chip clear" id="clear-filters">clear filters</button>`;

document.getElementById('quick-filters').addEventListener('click', (e) => {
  const btn = e.target.closest('button'); if (!btn) return;
  if (btn.id === 'clear-filters') { document.getElementById('search').value=''; document.getElementById('exclude').value=''; }
  else if (btn.dataset.q) { document.getElementById('search').value = btn.dataset.q; }
  renderFromCache();
});

document.getElementById('type-toggle').addEventListener('click', (e) => {
  const btn = e.target.closest('button'); if (!btn) return;
  typeFilter = btn.dataset.type;
  [...document.getElementById('type-toggle').children].forEach(b => b.classList.toggle('active', b===btn));
  renderFromCache();
});

document.getElementById('search').addEventListener('input', renderFromCache);
document.getElementById('exclude').addEventListener('input', renderFromCache);

function renderFromCache() { if (lastData) render(lastData, false); }

async function poll() {
  try {
    const res = await fetch('/api/data');
    lastData = await res.json();
    render(lastData, true);
  } catch (e) {
    document.getElementById('poll-info').textContent = 'connection lost — retrying...';
  }
  setTimeout(poll, 1500);
}

function pctClass(pct) { if (pct>=65) return 'high'; if (pct<=35) return 'low'; return 'mid'; }

function renderMarkets(list) {
  const el = document.getElementById('markets');
  if (!list || !list.length) { el.innerHTML = `<div class="empty" style="padding:12px;">no market data yet</div>`; return; }
  el.innerHTML = list.map(m => `
    <a class="mini-card" href="${m.link}" target="_blank" rel="noopener">
      <div class="q">${escapeHtml(m.question)}</div>
      <div class="row"><span class="pct ${pctClass(m.leading_price)}">${m.leading_price}%</span>
      <span class="sub">${escapeHtml(m.leading_outcome)}</span></div>
      <div class="sub">vol $${Math.round(m.volume).toLocaleString()}</div>
    </a>`).join('');
}

function renderFinance(data, fstatus) {
  const el = document.getElementById('finance');
  const keys = Object.keys(data || {});
  if (!keys.length) {
    const failed = (fstatus && fstatus.failed && fstatus.failed.length) ? ` (failed: ${fstatus.failed.join(', ')})` : '';
    el.innerHTML = `<div class="empty" style="padding:12px;">no data yet${escapeHtml(failed)}</div>`;
    return;
  }
  el.innerHTML = keys.map(k => {
    const d = data[k];
    return `<div class="mini-card"><div class="q">${escapeHtml(k)}</div>
      <div class="row"><span class="pct mid">${d.price}</span><span class="sub">${escapeHtml(d.date||'')}</span></div></div>`;
  }).join('');
}

function renderQuakes(list) {
  const el = document.getElementById('quakes');
  if (!list || !list.length) { el.innerHTML = `<div class="empty" style="padding:12px;">no recent M3.5+ events</div>`; return; }
  el.innerHTML = list.map(q => `
    <a class="mini-card quake ${q.near_watch_zone ? 'watch' : ''}" href="${q.url}" target="_blank" rel="noopener">
      <div class="q">${escapeHtml(q.place)}</div>
      <div class="row"><span class="pct mid">M${q.mag}</span><span class="sub">${q.time}</span></div>
      ${q.near_watch_zone ? `<div class="sub">near: ${escapeHtml(q.near_watch_zone)}</div>` : ''}
    </a>`).join('');
}

function renderFlights(list, fstatus) {
  const el = document.getElementById('flights');
  const statusEl = document.getElementById('flight-status');
  if (fstatus && fstatus.error && (!list || !list.length)) {
    statusEl.innerHTML = `flights: <span class="fail">error</span>`;
  } else if (fstatus) {
    const src = fstatus.source ? ` (${fstatus.source})` : '';
    statusEl.innerHTML = `flights: <span class="ok">${fstatus.count||0}</span>${src} @ ${fstatus.last_poll||'—'}`;
  }
  if (!list || !list.length) {
    const errText = (fstatus && fstatus.error) ? `<div class="sub" style="margin-top:4px;">${escapeHtml(fstatus.error)}</div>` : '';
    el.innerHTML = `<div class="empty" style="padding:12px;">no flight data yet${errText}</div>`;
    return;
  }
  el.innerHTML = list.map(f => `
    <div class="mini-card ${f.military_hint ? 'mil' : ''}">
      <div class="q">${escapeHtml(f.callsign)}${f.military_hint ? ' ⚑' : ''}</div>
      <div class="sub">${escapeHtml(f.origin_country||'unknown')}</div>
      <div class="row"><span class="sub">${f.alt_ft!=null ? f.alt_ft.toLocaleString()+'ft' : '—'}</span>
      <span class="sub">${f.speed_kt!=null ? f.speed_kt+'kt' : '—'}</span></div>
    </div>`).join('');
}

function renderIntensity(buckets) {
  const canvas = document.getElementById('intensity-canvas');
  const ctx = canvas.getContext('2d');
  const w = canvas.clientWidth || 400, h = 70;
  canvas.width = w * devicePixelRatio; canvas.height = h * devicePixelRatio;
  ctx.scale(devicePixelRatio, devicePixelRatio);
  ctx.clearRect(0,0,w,h);
  if (!buckets || !buckets.length) return;
  const maxVal = Math.max(1, ...buckets.map(b => b.news+b.reddit+b.telegram));
  const barW = w / buckets.length;
  buckets.forEach((b,i) => {
    const x = i*barW;
    let y = h;
    [['news','#8a6d3b'],['reddit','#ff4d3d'],['telegram','#6bb3ff']].forEach(([key,color]) => {
      const val = b[key];
      const barH = (val / maxVal) * (h-14);
      ctx.fillStyle = color;
      ctx.fillRect(x+1, y-barH, barW-2, barH);
      y -= barH;
    });
    if (i % 4 === 0) {
      ctx.fillStyle = '#4a3c22'; ctx.font = '8px IBM Plex Mono';
      ctx.fillText(b.bucket, x, h-2);
    }
  });
}

// ---- Geo map pan/zoom ----
const geoMapEl = document.getElementById('geo-map');
const geoInnerEl = document.getElementById('geo-map-inner');
let geoView = { scale: 1, x: 0, y: 0 };
const GEO_MIN_SCALE = 1, GEO_MAX_SCALE = 8;

function clampGeoView() {
  const rect = geoMapEl.getBoundingClientRect();
  const w = rect.width * geoView.scale, h = rect.height * geoView.scale;
  const minX = Math.min(0, rect.width - w), minY = Math.min(0, rect.height - h);
  geoView.x = Math.max(minX, Math.min(0, geoView.x));
  geoView.y = Math.max(minY, Math.min(0, geoView.y));
}

function applyGeoView() {
  clampGeoView();
  geoInnerEl.style.transform = `translate(${geoView.x}px, ${geoView.y}px) scale(${geoView.scale})`;
  if (typeof updateMarkerScales === 'function') updateMarkerScales();
}

function geoZoomAt(clientX, clientY, factor) {
  const rect = geoMapEl.getBoundingClientRect();
  const mx = clientX - rect.left, my = clientY - rect.top;
  const newScale = Math.max(GEO_MIN_SCALE, Math.min(GEO_MAX_SCALE, geoView.scale * factor));
  const realFactor = newScale / geoView.scale;
  geoView.x = mx - (mx - geoView.x) * realFactor;
  geoView.y = my - (my - geoView.y) * realFactor;
  geoView.scale = newScale;
  applyGeoView();
}

geoMapEl.addEventListener('wheel', (e) => {
  e.preventDefault();
  const factor = e.deltaY < 0 ? 1.2 : 1 / 1.2;
  geoZoomAt(e.clientX, e.clientY, factor);
}, { passive: false });

let geoDragging = false, geoDragStart = null;
geoMapEl.addEventListener('mousedown', (e) => {
  if (e.target.closest('.geo-arrow') || e.target.closest('#geo-zoom-controls')) return;
  geoDragging = true;
  geoMapEl.classList.add('grabbing');
  geoDragStart = { x: e.clientX - geoView.x, y: e.clientY - geoView.y };
});
window.addEventListener('mousemove', (e) => {
  if (!geoDragging) return;
  geoView.x = e.clientX - geoDragStart.x;
  geoView.y = e.clientY - geoDragStart.y;
  applyGeoView();
});
window.addEventListener('mouseup', () => { geoDragging = false; geoMapEl.classList.remove('grabbing'); });

// touch support (pinch zoom + drag)
let geoTouchStart = null, geoTouchPinchDist = null;
geoMapEl.addEventListener('touchstart', (e) => {
  if (e.touches.length === 1) {
    geoTouchStart = { x: e.touches[0].clientX - geoView.x, y: e.touches[0].clientY - geoView.y };
  } else if (e.touches.length === 2) {
    geoTouchPinchDist = Math.hypot(e.touches[0].clientX - e.touches[1].clientX, e.touches[0].clientY - e.touches[1].clientY);
  }
}, { passive: true });
geoMapEl.addEventListener('touchmove', (e) => {
  if (e.touches.length === 1 && geoTouchStart) {
    geoView.x = e.touches[0].clientX - geoTouchStart.x;
    geoView.y = e.touches[0].clientY - geoTouchStart.y;
    applyGeoView();
  } else if (e.touches.length === 2 && geoTouchPinchDist) {
    const dist = Math.hypot(e.touches[0].clientX - e.touches[1].clientX, e.touches[0].clientY - e.touches[1].clientY);
    const factor = dist / geoTouchPinchDist;
    const cx = (e.touches[0].clientX + e.touches[1].clientX) / 2;
    const cy = (e.touches[0].clientY + e.touches[1].clientY) / 2;
    geoZoomAt(cx, cy, factor);
    geoTouchPinchDist = dist;
  }
}, { passive: true });
geoMapEl.addEventListener('touchend', () => { geoTouchStart = null; geoTouchPinchDist = null; });

document.getElementById('geo-zoom-in').addEventListener('click', (e) => {
  e.stopPropagation();
  const r = geoMapEl.getBoundingClientRect();
  geoZoomAt(r.left + r.width/2, r.top + r.height/2, 1.4);
});
document.getElementById('geo-zoom-out').addEventListener('click', (e) => {
  e.stopPropagation();
  const r = geoMapEl.getBoundingClientRect();
  geoZoomAt(r.left + r.width/2, r.top + r.height/2, 1/1.4);
});
document.getElementById('geo-zoom-reset').addEventListener('click', (e) => {
  e.stopPropagation();
  geoView = { scale: 1, x: 0, y: 0 };
  applyGeoView();
});

const geoBackdrop = document.getElementById('geo-map-backdrop');
document.getElementById('geo-popout').addEventListener('click', (e) => {
  e.stopPropagation();
  const isFull = geoMapEl.classList.toggle('fullscreen');
  geoBackdrop.classList.toggle('show', isFull);
  document.getElementById('geo-popout').textContent = isFull ? '⤡' : '⛶';
  geoView = { scale: 1, x: 0, y: 0 };
  applyGeoView();
});
geoBackdrop.addEventListener('click', () => {
  geoMapEl.classList.remove('fullscreen');
  geoBackdrop.classList.remove('show');
  document.getElementById('geo-popout').textContent = '⛶';
  geoView = { scale: 1, x: 0, y: 0 };
  applyGeoView();
});
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && geoMapEl.classList.contains('fullscreen')) {
    geoMapEl.classList.remove('fullscreen');
    geoBackdrop.classList.remove('show');
    document.getElementById('geo-popout').textContent = '⛶';
    geoView = { scale: 1, x: 0, y: 0 };
    applyGeoView();
  }
});

let geoDataCache = [];
let activeGeoPlace = null;
let geoMarkerEls = []; // [{el, baseScale}] — updated on zoom so pins shrink as you zoom in

function updateMarkerScales() {
  const counterScale = 1 / Math.sqrt(geoView.scale); // shrink pins as you zoom in, but not all the way
  geoMarkerEls.forEach(({ el, baseScale }) => {
    el.style.transform = `translate(-50%,-100%) scale(${(baseScale * counterScale).toFixed(3)})`;
  });
}

function renderGeo(list) {
  geoDataCache = list || [];
  const inner = document.getElementById('geo-map-inner');
  inner.querySelectorAll('.geo-arrow').forEach(el => el.remove());
  geoMarkerEls = [];
  if (!list || !list.length) return;

  // Reference (unscaled) pixel space = the map's own current on-screen size;
  // since #geo-map-inner is 100% of #geo-map before the zoom transform, this
  // stays a stable coordinate system regardless of current zoom level.
  const refW = geoMapEl.clientWidth || 400, refH = geoMapEl.clientHeight || 220;
  const toXY = (lat, lon) => ({
    x: ((lon + 180) / 360) * refW,
    y: ((90 - lat) / 180) * refH,
  });

  // Log scale so one huge count (e.g. 22) doesn't dwarf everything else.
  const sized = list.map(g => ({
    g, ...toXY(g.lat, g.lon),
    r: 10 + Math.log2(g.count + 1) * 6,
  }));

  // Simple iterative separation: push apart any two markers whose circles
  // overlap, so nearby countries (Israel/Lebanon/Jordan, Gulf states, etc.)
  // fan out instead of stacking on the same pixel.
  for (let iter = 0; iter < 60; iter++) {
    let moved = false;
    for (let i = 0; i < sized.length; i++) {
      for (let j = i + 1; j < sized.length; j++) {
        const a = sized[i], b = sized[j];
        const dx = b.x - a.x, dy = b.y - a.y;
        const dist = Math.hypot(dx, dy) || 0.01;
        const minDist = a.r + b.r + 6;
        if (dist < minDist) {
          moved = true;
          const push = (minDist - dist) / 2;
          const ux = dx / dist, uy = dy / dist;
          a.x -= ux * push; a.y -= uy * push;
          b.x += ux * push; b.y += uy * push;
        }
      }
    }
    if (!moved) break;
  }

  const maxR = Math.max(...sized.map(s => s.r));
  sized.forEach(({ g, x, y, r }) => {
    const baseScale = 0.55 + (r / maxR) * 0.8;
    const arrow = document.createElement('div');
    arrow.className = 'geo-arrow' + (activeGeoPlace === g.place ? ' active' : '');
    arrow.style.left = Math.max(0, Math.min(refW, x)) + 'px';
    arrow.style.top = Math.max(0, Math.min(refH, y)) + 'px';
    arrow.innerHTML = `
      <div class="count-badge">${g.count}</div>
      <div class="geo-label">${escapeHtml(g.place)}</div>
      <div class="pin"></div>
      <div class="pin-dot"></div>`;
    arrow.addEventListener('click', (e) => {
      e.stopPropagation();
      showGeoPopup(g, arrow);
    });
    inner.appendChild(arrow);
    geoMarkerEls.push({ el: arrow, baseScale });
  });
  updateMarkerScales();
}

function showGeoPopup(place, arrowEl) {
  const popup = document.getElementById('geo-popup');
  const map = document.getElementById('geo-map');
  document.querySelectorAll('.geo-arrow').forEach(el => el.classList.remove('active'));
  arrowEl.classList.add('active');
  activeGeoPlace = place.place;

  const stories = (place.items || []).map(it => `
    <div class="gp-story">
      <a href="${it.link}" target="_blank" rel="noopener">${escapeHtml(it.title)}</a>
      <span class="gp-src">${escapeHtml(it.source)} · ${it.type} · ${it.time}</span>
      <div class="gp-tweet">
        <textarea readonly rows="2">${escapeHtml(generateTweet(it))}</textarea>
        <button class="gp-tweet-copy-btn">copy</button>
      </div>
    </div>`).join('') || `<div class="gp-story">no linked stories captured yet</div>`;

  popup.innerHTML = `
    <div class="gp-title"><span>${escapeHtml(place.place.toUpperCase())} · ${place.count} mention(s)</span>
    <span class="gp-close" id="gp-close">✕</span></div>
    ${stories}`;

  const arrowRect = arrowEl.getBoundingClientRect();
  popup.style.display = 'block';
  let left = arrowRect.left + 12;
  if (left + 280 > window.innerWidth) left = Math.max(4, window.innerWidth - 284);
  popup.style.left = left + 'px';
  let top = arrowRect.top;
  popup.style.top = top + 'px';

  document.getElementById('gp-close').addEventListener('click', (e) => {
    e.stopPropagation();
    popup.style.display = 'none';
    activeGeoPlace = null;
    arrowEl.classList.remove('active');
  });
}

document.addEventListener('click', (e) => {
  if (!e.target.closest('#geo-popup') && !e.target.closest('.geo-arrow')) {
    const popup = document.getElementById('geo-popup');
    if (popup) popup.style.display = 'none';
    activeGeoPlace = null;
    document.querySelectorAll('.geo-arrow').forEach(el => el.classList.remove('active'));
  }
});

function confBadge(item) {
  if (!item.alert || !item.confidence) return '';
  const n = item.confidence;
  const color = n >= 3 ? 'var(--green)' : n === 2 ? 'var(--amber)' : 'var(--amber-faint)';
  return `<span class="confidence" style="color:${color};border:1px solid ${color};padding:2px 6px;">${n} src</span>`;
}

// ---- Twitter-ready rephrase (deterministic, local — no external calls) ----
const TWEET_LEAD_INS = ["BREAKING:", "Just in —", "Update:", "On the wire:", "Developing:", "Report:", "🚨"];
function pickLeadIn(item) {
  const seed = item.id || item.link || item.title || 'x';
  let idx = 0;
  for (const c of seed) idx = (idx * 31 + c.charCodeAt(0)) % TWEET_LEAD_INS.length;
  return TWEET_LEAD_INS[idx];
}

const REPHRASE_RULES = [
  [/\bkilled\b/gi, 'dead'], [/\bwounded\b/gi, 'injured'], [/\bstrikes\b/gi, 'hits'],
  [/\bstrike\b/gi, 'hit'], [/\bofficials say\b/gi, 'officials report'],
  [/\baccording to\b/gi, 'per'], [/\bannounced\b/gi, 'confirmed'],
  [/\battack\b/gi, 'assault'], [/\bmilitary\b/gi, 'armed forces'],
  [/\bforces\b/gi, 'troops'], [/\bsources say\b/gi, 'insiders say'],
  [/\bwarns\b/gi, 'cautions'], [/\bslams\b/gi, 'condemns'],
];

// Arabic, Persian/Farsi extensions, Hebrew, Cyrillic, CJK — if a headline is
// mostly one of these scripts, the English word-swap rules can't meaningfully
// apply anyway and forcing an English "BREAKING:" lead-in onto it just mixes
// languages. Detect that up front and leave non-Latin text exactly as-is.
const NON_LATIN_RE = /[\u0600-\u06FF\u0750-\u077F\u0590-\u05FF\u0400-\u04FF\u3040-\u30FF\u4E00-\u9FFF]/g;
function isMostlyNonLatin(text) {
  const t = (text || '').trim();
  if (!t) return false;
  const letters = t.replace(/[\s\d\p{P}]/gu, '');
  if (!letters.length) return false;
  const nonLatinCount = (letters.match(NON_LATIN_RE) || []).length;
  return nonLatinCount / letters.length > 0.35;
}

function rephraseTitle(title) {
  let t = (title || '').trim();
  if (isMostlyNonLatin(t)) return t; // leave non-English text untouched
  // strip trailing " - Source" / " | Source" tags some feeds append
  t = t.replace(/\s*[-|]\s*[A-Za-z0-9 .]{2,24}$/, '');
  REPHRASE_RULES.forEach(([re, rep]) => { t = t.replace(re, rep); });
  return t;
}

function generateTweet(item) {
  const nonLatin = isMostlyNonLatin(item.title);
  const lead = nonLatin ? '' : pickLeadIn(item) + ' ';
  let body = rephraseTitle(item.title);
  const suffix = `\n\nvia ${item.source}\n${item.link}`;
  const budget = 280 - suffix.length - lead.length;
  if (body.length > budget) body = body.slice(0, Math.max(0, budget - 1)).trim() + '…';
  return `${lead}${body}${suffix}`;
}

document.addEventListener('click', (e) => {
  const btn = e.target.closest('.tweet-copy-btn') || e.target.closest('.gp-tweet-copy-btn');
  if (!btn) return;
  const wrap = btn.closest('.entry-tweet') || btn.closest('.gp-tweet');
  const textarea = wrap.querySelector('textarea');
  navigator.clipboard.writeText(textarea.value).then(() => {
    btn.classList.add('copied');
    const old = btn.textContent;
    btn.textContent = 'copied ✓';
    setTimeout(() => { btn.classList.remove('copied'); btn.textContent = old; }, 1500);
  }).catch(() => {
    textarea.select();
    document.execCommand('copy');
  });
});

function render(data, isPollTick) {
  const { headlines, status, markets: marketsList, flights: flightsList, flight_status: fstatus,
          finance, finance_status, quakes: quakesList, intensity, geo } = data;

  document.getElementById('poll-info').textContent = `last poll: ${status.last_poll || '—'}`;
  const failText = status.feeds_failed.length ? ` (failed: ${status.feeds_failed.join(', ')})` : '';
  document.getElementById('feed-status').innerHTML =
    `feeds: <span class="${status.feeds_failed.length?'fail':'ok'}">${status.feeds_ok}/${status.feeds_total}</span>${failText}`;
  document.getElementById('total-count').textContent = `matched: ${status.total_seen}`;
  document.getElementById('alert-count').innerHTML = `alerts: <span class="alert-count">${status.total_alerts||0}</span>`;
  document.getElementById('seismic-status').innerHTML =
    data.seismic_status && data.seismic_status.ok ? `seismic: <span class="ok">live</span>` : `seismic: <span class="fail">—</span>`;

  renderMarkets(marketsList);
  renderFinance(finance, finance_status);
  renderQuakes(quakesList);
  renderFlights(flightsList, fstatus);
  renderIntensity(intensity);
  renderGeo(geo);

  if (headlines.length) {
    const tickerItems = headlines.slice(0,10).map(h =>
      (h.alert && isActuallyRecent(h)) ? `<span class="alert-tick">⚠ ${h.source}: ${h.title}</span>` : `${h.source}: ${h.title}`);
    document.getElementById('ticker').innerHTML = tickerItems.join('   <span class="sep">///</span>   ');
    const duration = Math.max(30, tickerItems.join(' ').length * 0.09);
    document.getElementById('ticker').style.setProperty('--ticker-duration', duration + 's');
  }

  const feedEl = document.getElementById('feed');
  const searchTerms = document.getElementById('search').value.trim().toLowerCase().split(',').map(s=>s.trim()).filter(Boolean);
  const excludeTerms = document.getElementById('exclude').value.trim().toLowerCase().split(',').map(s=>s.trim()).filter(Boolean);

  let sourceList = currentView === 'timeline' ? (timelineData || []) : headlines;
  let filtered = sourceList;
  if (typeFilter === 'alerts') filtered = filtered.filter(h => h.alert);
  else if (typeFilter !== 'all') filtered = filtered.filter(h => h.type === typeFilter);
  if (searchTerms.length) filtered = filtered.filter(h => {
    const hay = (h.title+' '+h.source+' '+h.keyword).toLowerCase();
    return searchTerms.some(t => hay.includes(t));
  });
  if (excludeTerms.length) filtered = filtered.filter(h => {
    const hay = (h.title+' '+h.source+' '+h.keyword).toLowerCase();
    return !excludeTerms.some(t => hay.includes(t));
  });
  if (clearActive && currentView === 'live') filtered = filtered.filter(h => h.ts > clearedAt);

  if (!filtered.length) {
    const emptyMsg = clearActive ? 'cleared — waiting for new stories to come in...'
      : (sourceList.length ? 'no matches for current filters' : 'tuning in to the wire... first results in under a minute');
    feedEl.innerHTML = `<div class="empty">${emptyMsg}</div>`;
    return;
  }

  feedEl.innerHTML = filtered.map(h => {
    const isNew = (currentView !== 'live' || firstLoad || !isPollTick) ? false : (!knownIds.has(h.id) && isActuallyRecent(h));
    const typeColor = TYPE_COLOR[h.type] || 'var(--amber-faint)';
    const alertClass = h.alert ? ' alert' : '';
    return `<div class="entry${alertClass} ${isNew?'new':''}" style="--src-color:${colorFor(h.source)};--type-color:${typeColor}">
      <span class="time">${h.time}</span>
      <span class="source">${escapeHtml(h.source)}</span>
      <span class="headline"><a href="${h.link}" target="_blank" rel="noopener">${h.alert?'⚠ ':''}${escapeHtml(h.title)}</a></span>
      <span class="tag">${escapeHtml(h.alert ? (h.alert_keyword||'alert') : h.keyword)}</span>
      ${confBadge(h)}
      <div class="entry-tweet">
        <span class="tweet-label">🐦 tweet-ready<br>(rephrased)</span>
        <textarea class="tweet-text" readonly rows="3">${escapeHtml(generateTweet(h))}</textarea>
        <button class="tweet-copy-btn">copy</button>
      </div>
    </div>`;
  }).join('');

  if (isPollTick && currentView === 'live') { headlines.forEach(h => knownIds.add(h.id)); firstLoad = false; }
}

function escapeHtml(str) { const div = document.createElement('div'); div.textContent = str; return div.innerHTML; }

poll();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(PAGE)


# ---------------------------------------------------------------------------
# App-window launcher
# ---------------------------------------------------------------------------

def open_app_window(url: str):
    time.sleep(1.2)
    for name in ["msedge", "chrome", "google-chrome", "chromium"]:
        path = shutil.which(name)
        if path:
            try:
                subprocess.Popen([path, f"--app={url}", "--window-size=1400,960"])
                return
            except Exception:
                continue
    webbrowser.open(url)


def main():
    threading.Thread(target=news_loop, daemon=True).start()
    threading.Thread(target=reddit_loop, daemon=True).start()
    threading.Thread(target=telegram_loop, daemon=True).start()
    threading.Thread(target=poly_loop, daemon=True).start()
    threading.Thread(target=poll_flights, daemon=True).start()
    threading.Thread(target=finance_loop, daemon=True).start()
    threading.Thread(target=seismic_loop, daemon=True).start()
    url = f"http://127.0.0.1:{PORT}"
    threading.Thread(target=open_app_window, args=(url,), daemon=True).start()
    print(f"WIRE Conflict Monitor running at {url}  (Ctrl+C here to stop it)")
    app.run(host="127.0.0.1", port=PORT, debug=False, use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()