import time
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import feedparser
import pydantic

# Give every outbound feed fetch a hard timeout so one slow/dead source
# (of which there are now ~45) can't stall the whole request.
FEED_FETCH_TIMEOUT_SECONDS = 8
MAX_WORKERS = 20

app = FastAPI(title="Personalized News Aggregator")

# Enable CORS for local testing or web deployment
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# RSS Feed sources customized to your exact interests
# Each source is tagged with a "region" so the frontend can filter
# between Southern Africa specific coverage and Global coverage.
FEEDS = {
    "sports": [
        {"source": "MLB / Phillies", "url": "https://www.mlb.com/feeds/news/rss.xml", "region": "global"},
        {"source": "Premier League", "url": "https://www.skysports.com/rss/12040", "region": "global"},
        {"source": "BBC Golf", "url": "http://feeds.bbci.co.uk/sport/golf/rss.xml", "region": "global"},
        {"source": "Cricinfo - Global", "url": "https://www.espncricinfo.com/rss/content/story/news.xml", "region": "global"},
        {"source": "SA Rugby / Springboks", "url": "https://www.sarugby.co.za/rss/", "region": "za"},
        {"source": "Formula 1", "url": "https://www.formula1.com/content/fom-website/en/latest/all.xml", "region": "global"},
        # --- Phillies deep coverage ---
        {"source": "Philadelphia Inquirer - Phillies", "url": "https://www.inquirer.com/arc/outboundfeeds/rss/category/sports/phillies/", "region": "global"},
        {"source": "The Good Phight (SB Nation)", "url": "https://www.goodfight.com/rss/index.xml", "region": "global"},
        {"source": "PhoulBallz - Prospect Intel", "url": "https://phoulballz.com/feed/", "region": "global"},
        {"source": "FanSided Phillies Network", "url": "https://thatballisouttahere.com/feed", "region": "global"},
        {"source": "Phillies Insider", "url": "https://feeds.feedburner.com/PhilliesInsider", "region": "global"},
        # --- Man United / Premier League deep coverage ---
        {"source": "Stretty News (MUFC)", "url": "https://www.strettyend.com/feed", "region": "global"},
        {"source": "The Peoples Person (United News)", "url": "https://thepeoplesperson.com/feed/", "region": "global"},
        {"source": "United In Focus", "url": "https://www.unitedinfocus.com/feed/", "region": "global"},
        {"source": "Football365 Premier League", "url": "https://football365.com/premier-league/rss", "region": "global"},
        # --- Springbok Rugby, Lions URC & SA domestic ---
        {"source": "SA Rugby Magazine", "url": "https://www.sarugbymag.co.za/feed/", "region": "za"},
        {"source": "Rugby365 SA Focus", "url": "https://www.rugby365.com/feed/", "region": "za"},
        {"source": "United Rugby Championship (URC)", "url": "https://www.unitedrugby.com/rss", "region": "global"},
        {"source": "Super Rugby & SANZAAR News", "url": "https://super.rugby/rubicon/rss/news/", "region": "global"},
        {"source": "KEO.co.za SA Rugby Analysis", "url": "https://www.keo.co.za/feed/", "region": "za"},
        # --- Proteas & SA domestic cricket ---
        {"source": "SA Cricket Magazine", "url": "https://www.sacricketmag.com/feed/", "region": "za"},
        {"source": "ESPNcricinfo South Africa Feed", "url": "https://www.espncricinfo.com/rss/content/story/feeds/2.xml", "region": "za"},
        {"source": "Cricket South Africa (Official)", "url": "https://cricket.co.za/feed/", "region": "za"},
        {"source": "Club & Grassroots SA Cricket", "url": "https://clubcricket.co.za/feed/", "region": "za"},
        # --- Golf & Champions League ---
        {"source": "Golf Monthly", "url": "https://www.golfmonthly.com/feeds/all", "region": "global"},
        {"source": "Golf Digest", "url": "https://www.golfdigest.com/feed/rss", "region": "global"},
        {"source": "GolfWRX (Equipment & Tech)", "url": "https://www.golfwrx.com/feed/", "region": "global"},
        {"source": "UEFA Champions League (Official)", "url": "https://www.uefa.com/rss/uefachampionsleague/news/rss.xml", "region": "global"},
        {"source": "The Coaches' Voice", "url": "https://www.coachesvoice.com/feed/", "region": "global"}
    ],
    "engineering": [
        {"source": "Port Technology Intl", "url": "https://www.porttechnology.org/feed/", "region": "global"},
        {"source": "Dredging Today", "url": "https://www.dredgingtoday.com/feed/", "region": "global"},
        {"source": "Railway Gazette Intl", "url": "https://www.railwaygazette.com/124.rss", "region": "global"},
        {"source": "Engineering News SA", "url": "https://www.engineeringnews.co.za/rss/engineering-news", "region": "za"},
        {"source": "Transnet Port Terminals", "url": "https://www.transnetportterminals.net/rss", "region": "za"},
        # --- Southern Africa port, coastal & water engineering ---
        {"source": "Engineering News - Africa Edition", "url": "https://www.engineeringnews.co.za/page/rss-feed/feed:africa-edition", "region": "za"},
        {"source": "Maritime Executive - Ports & Infrastructure", "url": "https://www.maritime-executive.com/rss", "region": "global"},
        {"source": "Infrastructure News SA", "url": "https://www.infrastructurene.ws/feed/", "region": "za"},
        {"source": "SA Dept. of Water & Sanitation", "url": "https://www.dws.gov.za/Rss/default.aspx", "region": "za"},
        {"source": "Africa Ports & Ships", "url": "https://www.africaports.co.za/#feed", "region": "za"}
    ],
    "finance": [
        {"source": "Reuters Markets", "url": "http://feeds.reuters.com/reuters/businessNews", "region": "global"},
        {"source": "CNBC Finance", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html", "region": "global"},
        {"source": "Moneyweb SA (JSE Focus)", "url": "https://www.moneyweb.co.za/feed/", "region": "za"},
        # --- South African financial & market intel ---
        {"source": "Daily Investor SA", "url": "https://dailyinvestor.com/feed/", "region": "za"},
        {"source": "Business Day SA", "url": "https://www.businesslive.co.za/rss/?publication=bd", "region": "za"},
        {"source": "Fin24 / News24 Financial", "url": "https://www.fin24.com/rss/southafrica", "region": "za"}
    ],
    "hobbies": [
        {"source": "Fine Woodworking", "url": "https://www.finewoodworking.com/feed", "region": "global"},
        {"source": "BBC Wildlife", "url": "https://www.discoverwildlife.com/feed", "region": "global"},
        {"source": "Top Gear / Automotive", "url": "https://www.topgear.com/car-news/rss.xml", "region": "global"},
        {"source": "Africa Geographic", "url": "https://africageographic.com/feed/", "region": "za"}
    ],
    "conflicts": [
        {"source": "BBC World News", "url": "http://feeds.bbci.co.uk/news/world/rss.xml", "region": "global"},
        {"source": "Al Jazeera English", "url": "https://www.aljazeera.com/xml/rss/all.xml", "region": "global"},
        {"source": "News24 South Africa", "url": "https://www.news24.com/news24/rss", "region": "za"},
        # --- Global conflict & geopolitics deep coverage ---
        {"source": "Defense News Global", "url": "https://www.defensenews.com/arc/outboundfeeds/rss/", "region": "global"},
        {"source": "Institute for the Study of War (ISW)", "url": "https://www.understandingwar.org/rss.xml", "region": "global"},
        {"source": "War on the Rocks", "url": "https://war-on-the-rocks.com/feed/", "region": "global"},
        {"source": "Bellingcat OSINT Investigations", "url": "https://www.bellingcat.com/feed/", "region": "global"},
        {"source": "GDELT Project - Conflict Event Stream", "url": "https://gdeltdoc.blob.core.windows.net/data/gdeltv2/rss/index.html", "region": "global"}
    ]
}

# Keywords to boost relevance for your core focus areas
KEYWORD_BOOSTS = [
    "Phillies", "Manchester United", "Bournemouth", "Springboks", "Lions", "Carlos Sainz",
    "South Africa", "SA20", "IPL", "Ryder Cup", "coastal", "port", "beach", "railway",
    "export", "JSE", "NYSE", "ETF", "woodworking", "wildlife"
]

# Sub-topic keyword map, used so you can weight/limit individual interests
# *within* a grouped tab (e.g. cap how much F1 content shows up under Sports).
TOPIC_KEYWORDS = {
    "sports": [
        ("Phillies/MLB", ["phillies", "mlb", "baseball"]),
        ("Premier League", ["manchester united", "bournemouth", "premier league", "epl"]),
        ("Golf", ["golf", "ryder cup", "pga", "masters"]),
        ("Cricket", ["cricket", "ipl", "sa20", "test match", "odi", "t20"]),
        ("Rugby", ["rugby", "springboks", "lions", "six nations", "urc"]),
        ("F1", ["formula 1", "f1", "carlos sainz", "grand prix", "paddock"]),
        ("Champions League", ["champions league", "uefa champions league", "ucl", "ballon d'or"]),
    ],
    "engineering": [
        ("Ports", ["port", "harbour", "harbor", "terminal", "container"]),
        ("Dredging", ["dredge", "dredging"]),
        ("Rail", ["rail", "railway", "locomotive", "freight"]),
        ("Water Infrastructure", ["water", "sanitation", "dam", "desalination", "water treatment"]),
    ],
    "finance": [
        ("JSE", ["jse", "johannesburg stock"]),
        ("ETF", ["etf", "exchange-traded"]),
        ("Markets", ["market", "nyse", "dow", "s&p", "nasdaq"]),
    ],
    "hobbies": [
        ("Woodworking", ["woodworking", "wood", "joinery", "carpentry"]),
        ("Wildlife", ["wildlife", "animal", "conservation", "species"]),
        ("Automotive", ["car", "automotive", "engine", "vehicle", "motoring"]),
    ],
    "conflicts": [
        ("Africa", ["africa", "south africa", "sadc"]),
        ("Middle East", ["gaza", "israel", "middle east", "iran", "lebanon"]),
        ("Defense & Security", ["defense", "defence", "military", "nato", "weapons", "missile", "army", "navy", "air force"]),
        ("World News", ["united nations", "un ", "diplomat", "sanctions"]),
    ],
}


def classify_topic(category: str, text: str) -> str:
    text_lower = text.lower()
    for topic_name, keywords in TOPIC_KEYWORDS.get(category, []):
        for kw in keywords:
            if kw in text_lower:
                return topic_name
    return "General"


def parse_published_time(entry):
    if hasattr(entry, 'published_parsed') and entry.published_parsed:
        return datetime.fromtimestamp(time.mktime(entry.published_parsed))
    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
        return datetime.fromtimestamp(time.mktime(entry.updated_parsed))
    return datetime.now()


def format_relative_time(dt: datetime) -> str:
    seconds = max((datetime.now() - dt).total_seconds(), 0)
    if seconds < 60:
        return "just now"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes} min{'s' if minutes != 1 else ''} ago"
    hours = int(minutes // 60)
    if hours < 24:
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = int(hours // 24)
    return f"{days} day{'s' if days != 1 else ''} ago"


def fetch_one_feed(cat: str, feed_info: dict, catchup_hours: Optional[int]):
    """Fetches and parses a single feed, with a hard socket timeout so a
    slow/dead source can't hang the whole request. Returns a list of
    already-shaped article dicts (never raises)."""
    articles = []
    old_timeout = socket.getdefaulttimeout()
    try:
        socket.setdefaulttimeout(FEED_FETCH_TIMEOUT_SECONDS)
        parsed = feedparser.parse(feed_info["url"])
        for entry in parsed.entries[:10]:  # Limit per source for performance
            pub_date = parse_published_time(entry)

            if catchup_hours:
                cutoff = datetime.now() - timedelta(hours=catchup_hours)
                if pub_date < cutoff:
                    continue

            title = entry.get("title", "No Title")
            summary = entry.get("summary", entry.get("description", ""))
            link = entry.get("link", "#")

            relevance = sum(1 for kw in KEYWORD_BOOSTS if kw.lower() in (title + summary).lower())
            interactions = round((pub_date.timestamp() % 100) + (relevance * 10))
            topic = classify_topic(cat, title + " " + summary)

            articles.append({
                "id": abs(hash(link)),
                "title": title,
                "summary": summary[:250] + "..." if len(summary) > 250 else summary,
                "link": link,
                "category": cat,
                "topic": topic,
                "source": feed_info["source"],
                "region": feed_info.get("region", "global"),
                "published": pub_date.isoformat(),
                "published_formatted": pub_date.strftime("%b %d, %H:%M"),
                "relevance": relevance,
                "interactions": interactions
            })
    except Exception:
        pass
    finally:
        socket.setdefaulttimeout(old_timeout)
    return articles


@app.get("/api/news")
def get_news(
    category: Optional[str] = Query("all"),
    sort: Optional[str] = Query("latest"),
    catchup_hours: Optional[int] = Query(None),
    region: Optional[str] = Query("all")  # "all", "za" (Southern Africa), or "global"
):
    articles = []
    categories_to_fetch = FEEDS.keys() if category == "all" else [category]

    # Build the list of (category, feed) jobs, applying the region filter
    # up front so we never even fetch sources outside the requested region.
    jobs = []
    for cat in categories_to_fetch:
        if cat not in FEEDS:
            continue
        for feed_info in FEEDS[cat]:
            if region in ("za", "global") and feed_info.get("region", "global") != region:
                continue
            jobs.append((cat, feed_info))

    # Fetch every feed concurrently. With ~45 sources now in the catalog,
    # doing this sequentially was slow enough to occasionally time out
    # mid-request, which is what produced partial/stale-looking results
    # (e.g. "unified feed" silently truncating to whichever categories had
    # finished fetching before the timeout).
    if jobs:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_one_feed, cat, feed_info, catchup_hours) for cat, feed_info in jobs]
            for future in as_completed(futures):
                articles.extend(future.result())

    # Sorting Logic
    if sort == "latest":
        articles.sort(key=lambda x: x["published"], reverse=True)
    elif sort == "relevance":
        articles.sort(key=lambda x: (x["relevance"], x["published"]), reverse=True)
    elif sort == "interactions":
        articles.sort(key=lambda x: x["interactions"], reverse=True)

    return {"articles": articles}


@app.get("/api/topics")
def get_topics():
    """Expose the topic taxonomy per category so the frontend can render
    weighting controls without hardcoding the keyword map twice."""
    return {cat: [name for name, _ in topics] for cat, topics in TOPIC_KEYWORDS.items()}


BULLETIN_CANDIDATES_PER_FEED = 3
BULLETIN_COUNT = 8


@app.get("/api/bulletins")
def get_bulletins(region: Optional[str] = Query("all")):
    """Live ticker of the freshest headlines across every feed in the
    catalog (optionally scoped to a region), refreshed on every request.
    Each bulletin carries a "detail" (fuller summary) and a "link"
    (source URL) so it can be opened for more context."""
    jobs = []
    for cat, feeds in FEEDS.items():
        for feed_info in feeds:
            if region in ("za", "global") and feed_info.get("region", "global") != region:
                continue
            jobs.append((cat, feed_info))

    candidates = []
    if jobs:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_one_feed, cat, feed_info, None) for cat, feed_info in jobs]
            for future in as_completed(futures):
                # Only keep the freshest couple of entries per feed as bulletin candidates
                candidates.extend(future.result()[:BULLETIN_CANDIDATES_PER_FEED])

    candidates.sort(key=lambda x: x["published"], reverse=True)

    bulletins = []
    for item in candidates[:BULLETIN_COUNT]:
        pub_date = datetime.fromisoformat(item["published"])
        detail = item["summary"] if item["summary"] and item["summary"].strip() else item["title"]
        bulletins.append({
            "time": format_relative_time(pub_date),
            "text": item["title"],
            "detail": detail,
            "link": item["link"],
            "source": item["source"],
            "category": item["category"]
        })
    return bulletins
