from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import feedparser
import httpx
from bs4 import BeautifulSoup


@dataclass(frozen=True, slots=True)
class RawNewsItem:
    external_id: str
    source: str
    source_kind: str
    source_home: str
    title: str
    summary: str
    url: str
    published_at: datetime
    source_categories: list[str]
    trust_score: float


class NewsSource:
    name: str
    kind: str
    homepage: str
    trust_score: float

    async def fetch(self, client: httpx.AsyncClient) -> list[RawNewsItem]:
        raise NotImplementedError


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_rss_datetime(entry: feedparser.FeedParserDict) -> datetime | None:
    published = entry.get("published") or entry.get("updated")
    if published:
        try:
            return ensure_utc(parsedate_to_datetime(published))
        except (TypeError, ValueError, IndexError):
            pass

    structured = entry.get("published_parsed") or entry.get("updated_parsed")
    if structured:
        return datetime(*structured[:6], tzinfo=timezone.utc)

    return None


def clean_text(value: str | None) -> str:
    return " ".join((value or "").split())


class RssSource(NewsSource):
    def __init__(
        self,
        *,
        name: str,
        kind: str,
        homepage: str,
        feed_url: str,
        trust_score: float,
        max_items: int = 25,
    ) -> None:
        self.name = name
        self.kind = kind
        self.homepage = homepage
        self.feed_url = feed_url
        self.trust_score = trust_score
        self.max_items = max_items

    async def fetch(self, client: httpx.AsyncClient) -> list[RawNewsItem]:
        response = await client.get(self.feed_url)
        response.raise_for_status()

        parsed = feedparser.parse(response.text)
        items: list[RawNewsItem] = []

        for entry in parsed.entries[: self.max_items]:
            published_at = parse_rss_datetime(entry)
            if published_at is None:
                continue

            url = clean_text(entry.get("link"))
            if not url:
                continue

            categories = [
                clean_text(tag.get("term"))
                for tag in entry.get("tags", [])
                if clean_text(tag.get("term"))
            ]

            items.append(
                RawNewsItem(
                    external_id=clean_text(entry.get("id") or entry.get("guid") or url),
                    source=self.name,
                    source_kind=self.kind,
                    source_home=self.homepage,
                    title=clean_text(entry.get("title")),
                    summary=clean_text(entry.get("summary") or entry.get("description")),
                    url=url,
                    published_at=published_at,
                    source_categories=categories,
                    trust_score=self.trust_score,
                )
            )

        return items


class TreasurySource(NewsSource):
    name = "U.S. Treasury"
    kind = "official"
    homepage = "https://home.treasury.gov/news/press-releases"
    trust_score = 0.96

    async def fetch(self, client: httpx.AsyncClient) -> list[RawNewsItem]:
        response = await client.get(self.homepage)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        items: list[RawNewsItem] = []
        seen_urls: set[str] = set()

        for headline in soup.select("h3.featured-stories__headline"):
            anchor = headline.find("a", href=True)
            container = headline.parent
            time_node = container.find("time", class_="datetime") if container else None

            if anchor is None or time_node is None:
                continue

            url = urljoin(self.homepage, anchor["href"])
            if url in seen_urls:
                continue

            published_at_raw = time_node.get("datetime")
            if not published_at_raw:
                continue

            seen_urls.add(url)
            categories = []
            subcategory = container.select_one(".subcategory a") if container else None
            if subcategory is not None:
                categories.append(clean_text(subcategory.get_text(" ", strip=True)))

            items.append(
                RawNewsItem(
                    external_id=url,
                    source=self.name,
                    source_kind=self.kind,
                    source_home=self.homepage,
                    title=clean_text(anchor.get_text(" ", strip=True)),
                    summary="",
                    url=url,
                    published_at=ensure_utc(datetime.fromisoformat(published_at_raw)),
                    source_categories=categories,
                    trust_score=self.trust_score,
                )
            )

        return items[:30]


class BeaSource(NewsSource):
    name = "BEA Releases"
    kind = "official"
    homepage = "https://www.bea.gov/news/current-releases"
    trust_score = 0.94

    async def fetch(self, client: httpx.AsyncClient) -> list[RawNewsItem]:
        response = await client.get(self.homepage)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        items: list[RawNewsItem] = []

        for row in soup.select("tr.release-row"):
            anchor = row.select_one("td[headers='view-title-table-column'] a[href]")
            time_node = row.select_one("time[datetime]")

            if anchor is None or time_node is None:
                continue

            published_at_raw = time_node.get("datetime")
            if not published_at_raw:
                continue

            items.append(
                RawNewsItem(
                    external_id=urljoin(self.homepage, anchor["href"]),
                    source=self.name,
                    source_kind=self.kind,
                    source_home=self.homepage,
                    title=clean_text(anchor.get_text(" ", strip=True)),
                    summary="",
                    url=urljoin(self.homepage, anchor["href"]),
                    published_at=ensure_utc(datetime.fromisoformat(published_at_raw)),
                    source_categories=["Economic release"],
                    trust_score=self.trust_score,
                )
            )

        return items[:25]


def build_default_sources() -> list[NewsSource]:
    return [
        RssSource(
            name="Federal Reserve Press",
            kind="official",
            homepage="https://www.federalreserve.gov/newsevents/pressreleases.htm",
            feed_url="https://www.federalreserve.gov/feeds/press_all.xml",
            trust_score=0.99,
            max_items=30,
        ),
        RssSource(
            name="Federal Reserve Speeches",
            kind="official",
            homepage="https://www.federalreserve.gov/newsevents/speech/default.htm",
            feed_url="https://www.federalreserve.gov/feeds/speeches.xml",
            trust_score=0.95,
            max_items=20,
        ),
        TreasurySource(),
        BeaSource(),
        RssSource(
            name="CNBC Markets",
            kind="media",
            homepage="https://www.cnbc.com/us-top-news-and-analysis/",
            feed_url="https://www.cnbc.com/id/100003114/device/rss/rss.html",
            trust_score=0.82,
            max_items=35,
        ),
        RssSource(
            name="MarketWatch",
            kind="media",
            homepage="https://www.marketwatch.com/",
            feed_url="https://feeds.marketwatch.com/marketwatch/topstories/",
            trust_score=0.8,
            max_items=35,
        ),
    ]
