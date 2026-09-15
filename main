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
FEEDS = {
    "sports": [
        {"source": "MLB / Phillies", "url": "https://www.mlb.com/feeds/news/rss.xml"},
        {"source": "Premier League", "url": "https://www.skysports.com/rss/12040"},
        {"source": "BBC Golf", "url": "http://feeds.bbci.co.uk/sport/golf/rss.xml"},
        {"source": "Cricinfo - Global", "url": "https://www.espncricinfo.com/rss/content/story/news.xml"},
        {"source": "SA Rugby / Springboks", "url": "https://www.sarugby.co.za/rss/"},
        {"source": "Formula 1", "url": "https://www.formula1.com/content/fom-website/en/latest/all.xml"}
    ],
    "engineering": [
        {"source": "Port Technology Intl", "url": "https://www.porttechnology.org/feed/"},
        {"source": "Dredging Today", "url": "https://www.dredgingtoday.com/feed/"},
        {"source": "Railway Gazette Intl", "url": "https://www.railwaygazette.com/124.rss"},
        {"source": "Engineering News SA", "url": "https://www.engineeringnews.co.za/rss/engineering-news"}
    ],
    "finance": [
        {"source": "Reuters Markets", "url": "http://feeds.reuters.com/reuters/businessNews"},
        {"source": "CNBC Finance", "url": "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
        {"source": "Moneyweb SA (JSE Focus)", "url": "https://www.moneyweb.co.za/feed/"}
    ],
    "hobbies": [
        {"source": "Fine Woodworking", "url": "https://www.finewoodworking.com/feed"},
        {"source": "BBC Wildlife", "url": "https://www.discoverwildlife.com/feed"},
        {"source": "Top Gear / Automotive", "url": "https://www.topgear.com/car-news/rss.xml"}
    ],
    "conflicts": [
        {"source": "BBC World News", "url": "http://feeds.bbci.co.uk/news/world/rss.xml"},
        {"source": "Al Jazeera English", "url": "https://www.aljazeera.com/xml/rss/all.xml"}
    ]
}

# Keywords to boost relevance for your core focus areas
KEYWORD_BOOSTS = [
    "Phillies", "Manchester United", "Bournemouth", "Springboks", "Lions", "Carlos Sainz",
    "South Africa", "SA20", "IPL", "Ryder Cup", "coastal", "port", "beach", "railway",
    "export", "JSE", "NYSE", "ETF", "woodworking", "wildlife"
]

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
    catchup_hours: Optional[int] = Query(None)
):
    articles = []
    categories_to_fetch = FEEDS.keys() if category == "all" else [category]

    for cat in categories_to_fetch:
        if cat not in FEEDS:
            continue
        for feed_info in FEEDS[cat]:
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

                    articles.append({
                        "id": hash(link),
                        "title": title,
                        "summary": summary[:250] + "..." if len(summary) > 250 else summary,
                        "link": link,
                        "category": cat,
                        "source": feed_info["source"],
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

@app.get("/api/bulletins")
def get_bulletins():
    # Sidebar quick sports and breaking headlines bulletin
    return [
        {"time": "10 mins ago", "text": "Phillies secure series win with late home run in the 9th."},
        {"time": "25 mins ago", "text": "SA20 auction details finalized for upcoming season."},
        {"time": "1 hour ago", "text": "Springboks squad announced for winter international fixtures."},
        {"time": "2 hours ago", "text": "Major Southern African port expansion project greenlit."},
        {"time": "3 hours ago", "text": "Carlos Sainz puts in top time during FP2 session."}
    ]
