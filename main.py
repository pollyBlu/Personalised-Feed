import time
from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import feedparser
import pydantic

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
        {"source": "Formula 1", "url": "https://www.formula1.com/content/fom-website/en/latest/all.xml", "region": "global"}
    ],
    "engineering": [
        {"source": "Port Technology Intl", "url": "https://www.porttechnology.org/feed/", "region": "global"},
        {"source": "Dredging Today", "url": "https://www.dredgingtoday.com/feed/", "region": "global"},
        {"source": "Railway Gazette Intl", "url": "https://www.railwaygazette.com/124.rss", "region": "global"},
        {"source": "Engineering News SA", "url": "https://www.engineeringnews.co.za/rss/engineering-news", "region": "za"},
        {"source": "Transnet Port Terminals", "url": "https://www.transnetportterminals.net/rss", "region": "za"}
    ],
    "finance": [
        {"source": "Reuters Markets", "url": "http://feeds.reuters.com/reuters/businessNews", "region": "global"},
        {"source": "CNBC Finance", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html", "region": "global"},
        {"source": "Moneyweb SA (JSE Focus)", "url": "https://www.moneyweb.co.za/feed/", "region": "za"}
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
        {"source": "News24 South Africa", "url": "https://www.news24.com/news24/rss", "region": "za"}
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
    ],
    "engineering": [
        ("Ports", ["port", "harbour", "harbor", "terminal", "container"]),
        ("Dredging", ["dredge", "dredging"]),
        ("Rail", ["rail", "railway", "locomotive", "freight"]),
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

@app.get("/api/news")
def get_news(
    category: Optional[str] = Query("all"),
    sort: Optional[str] = Query("latest"),
    catchup_hours: Optional[int] = Query(None),
    region: Optional[str] = Query("all")  # "all", "za" (Southern Africa), or "global"
):
    articles = []
    categories_to_fetch = FEEDS.keys() if category == "all" else [category]

    for cat in categories_to_fetch:
        if cat not in FEEDS:
            continue
        for feed_info in FEEDS[cat]:
            # Region filter: skip sources that don't match the requested region
            if region in ("za", "global") and feed_info.get("region", "global") != region:
                continue
            try:
                parsed = feedparser.parse(feed_info["url"])
                for entry in parsed.entries[:10]: # Limit per source for performance
                    pub_date = parse_published_time(entry)
                    
                    # Filter for Catchup Mode if requested
                    if catchup_hours:
                        cutoff = datetime.now() - timedelta(hours=catchup_hours)
                        if pub_date < cutoff:
                            continue

                    title = entry.get("title", "No Title")
                    summary = entry.get("summary", entry.get("description", ""))
                    link = entry.get("link", "#")

                    # Calculate dummy relevance based on keyword hits
                    relevance = sum(1 for kw in KEYWORD_BOOSTS if kw.lower() in (title + summary).lower())
                    
                    # Simulated interaction score (for sorting)
                    interactions = round((pub_date.timestamp() % 100) + (relevance * 10))

                    topic = classify_topic(cat, title + " " + summary)

                    articles.append({
                        "id": hash(link),
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
            except Exception as e:
                continue

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


@app.get("/api/bulletins")
def get_bulletins():
    # Sidebar quick sports and breaking headlines bulletin.
    # Each bulletin now carries a "detail" (fuller text) and a "link"
    # (source URL) so it can be opened for more context.
    return [
        {
            "time": "10 mins ago",
            "text": "Phillies secure series win with late home run in the 9th.",
            "detail": "The Phillies closed out the series with a walk-off home run in the bottom of the 9th, "
                       "extending their winning streak. The bullpen held a one-run lead through the final two innings "
                       "after a shaky start from the opener.",
            "link": "https://www.mlb.com/phillies"
        },
        {
            "time": "25 mins ago",
            "text": "SA20 auction details finalized for upcoming season.",
            "detail": "Franchise owners finalized retention lists ahead of the SA20 player auction, with several "
                       "overseas stars expected to headline the marquee bracket. The auction date and full player "
                       "pool will be published shortly.",
            "link": "https://www.sa20.co.za"
        },
        {
            "time": "1 hour ago",
            "text": "Springboks squad announced for winter international fixtures.",
            "detail": "The Springboks coaching staff named a 34-man squad for the upcoming winter internationals, "
                       "with a handful of new caps included alongside the regular starting core.",
            "link": "https://www.sarugby.co.za"
        },
        {
            "time": "2 hours ago",
            "text": "Major Southern African port expansion project greenlit.",
            "detail": "Regulators approved a multi-year expansion of container handling capacity at a major "
                       "Southern African port, aimed at easing congestion and reducing export bottlenecks over the "
                       "next three years.",
            "link": "https://www.porttechnology.org"
        },
        {
            "time": "3 hours ago",
            "text": "Carlos Sainz puts in top time during FP2 session.",
            "detail": "Carlos Sainz topped the times in the second free practice session, edging out the "
                       "championship leaders on a low-fuel run. Teams reported mixed tyre degradation heading into "
                       "qualifying.",
            "link": "https://www.formula1.com"
        }
    ]
