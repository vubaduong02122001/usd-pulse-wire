from __future__ import annotations

import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from app.config import Settings
from app.models import SpeechSnapshot, SpeechTapeItem
from app.services.impact import assess_raw_item
from app.services.sources import RawNewsItem, clean_text, ensure_utc


UTC = timezone.utc
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
TRUMP_TAPE_HINTS = (
    "trade",
    "tariff",
    "import",
    "export",
    "tax",
    "economic",
    "economy",
    "treasury",
    "inflation",
    "oil",
    "energy",
    "dollar",
    "steel",
    "aluminum",
    "copper",
    "pharmaceutical",
    "shipping",
    "manufacturing",
)
SIGNAL_HINTS = {
    "Treasury / fiscal": (
        "tariff",
        "import",
        "imports",
        "export",
        "exports",
        "tax",
        "oecd",
        "trade",
        "payments",
        "steel",
        "aluminum",
        "copper",
        "pharmaceutical",
    ),
    "Risk sentiment": ("oil", "energy", "shipping", "sanctions"),
    "Inflation": ("inflation", "prices"),
    "Growth": ("economic", "economy", "manufacturing", "supply chain"),
    "FX / rates": ("dollar", "exchange rate", "currency"),
}


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized.replace("Z", "+00:00")

    try:
        return ensure_utc(datetime.fromisoformat(normalized))
    except ValueError:
        return None


def infer_signals(title: str, summary: str) -> list[str]:
    text = f"{title} {summary}".casefold()
    return [
        signal
        for signal, hints in SIGNAL_HINTS.items()
        if any(hint in text for hint in hints)
    ]


class SpeechTapeService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None
        self._snapshot: SpeechSnapshot | None = None

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.settings.http_timeout_seconds,
                headers={
                    "User-Agent": self.settings.request_user_agent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
                follow_redirects=True,
            )

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def snapshot(self, *, force: bool = False) -> SpeechSnapshot:
        async with self._lock:
            if self._client is None:
                raise RuntimeError("HTTP client is not initialized.")

            now = datetime.now(UTC)
            if (
                not force
                and self._snapshot is not None
                and now - self._snapshot.updated_at < timedelta(minutes=20)
            ):
                return self._snapshot

            sitemap_items = await self._fetch_sitemap_items()
            candidates = [
                item
                for item in sitemap_items
                if item["published_at"] is not None
                and now - item["published_at"] <= timedelta(days=120)
            ][:30]
            parsed = await asyncio.gather(
                *(self._fetch_item(entry["url"], entry["published_at"]) for entry in candidates)
            )
            items = [item for item in parsed if item is not None]
            items.sort(key=lambda item: (item.impact_score, item.published_at), reverse=True)
            self._snapshot = SpeechSnapshot(
                updated_at=now,
                source="White House official remarks / presidential actions",
                items=items[:8],
            )
            return self._snapshot

    async def _fetch_sitemap_items(self) -> list[dict[str, datetime | str]]:
        if self._client is None:
            raise RuntimeError("HTTP client is not initialized.")

        sitemap_urls = [
            "https://www.whitehouse.gov/post-sitemap.xml",
            "https://www.whitehouse.gov/post-sitemap2.xml",
        ]
        collected: list[dict[str, datetime | str]] = []

        for sitemap_url in sitemap_urls:
            response = await self._client.get(sitemap_url)
            response.raise_for_status()
            root = ET.fromstring(response.text)
            for url_node in root.findall("sm:url", SITEMAP_NS):
                loc_node = url_node.find("sm:loc", SITEMAP_NS)
                lastmod_node = url_node.find("sm:lastmod", SITEMAP_NS)
                if loc_node is None or loc_node.text is None:
                    continue

                url = loc_node.text.strip()
                if "/remarks/" not in url and "/presidential-actions/" not in url:
                    continue
                if "/remarks/" not in url and not any(hint in url.casefold() for hint in TRUMP_TAPE_HINTS):
                    continue

                collected.append(
                    {
                        "url": url,
                        "published_at": parse_iso_datetime(lastmod_node.text if lastmod_node is not None else None),
                    }
                )

        collected.sort(
            key=lambda item: item["published_at"] or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        return collected

    async def _fetch_item(
        self,
        url: str,
        fallback_published_at: datetime | None,
    ) -> SpeechTapeItem | None:
        if self._client is None:
            raise RuntimeError("HTTP client is not initialized.")

        response = await self._client.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        title_node = soup.find("h1")
        title = clean_text(title_node.get_text(" ", strip=True) if title_node else "")
        if not title:
            return None

        published_at = self._extract_published_at(soup) or fallback_published_at
        if published_at is None:
            return None

        paragraphs = [
            clean_text(node.get_text(" ", strip=True))
            for node in soup.select(".entry-content p, main p, article p")
        ]
        paragraphs = [paragraph for paragraph in paragraphs if paragraph]
        summary = " ".join(paragraphs[:2]).strip()
        if len(summary) > 360:
            summary = summary[:357].rstrip() + "..."

        raw = RawNewsItem(
            external_id=url,
            source="Trump White House",
            source_kind="official",
            source_home="https://www.whitehouse.gov/",
            title=title,
            summary=summary,
            url=url,
            published_at=published_at,
            source_categories=[
                "White House remarks" if "/remarks/" in url else "Presidential action"
            ],
            trust_score=0.95,
        )
        assessed = assess_raw_item(raw)
        if assessed is None:
            fallback_signals = infer_signals(title, summary)
            if not fallback_signals:
                return None

            fallback_terms = [
                hint
                for hint in TRUMP_TAPE_HINTS
                if hint in f"{title} {summary}".casefold()
            ][:4]
            fallback_score = min(68, 46 + len(fallback_signals) * 6 + len(fallback_terms) * 2)
            fallback_level = "high" if fallback_score >= 76 else "medium" if fallback_score >= 48 else "watch"
            return SpeechTapeItem(
                id=raw.external_id,
                title=title,
                summary=summary,
                url=url,
                published_at=published_at,
                source=raw.source,
                item_kind="remarks" if "/remarks/" in url else "policy",
                impact_score=fallback_score,
                impact_level=fallback_level,
                matched_signals=fallback_signals[:4],
                matched_terms=fallback_terms,
            )

        return SpeechTapeItem(
            id=assessed.id,
            title=assessed.title,
            summary=assessed.summary,
            url=assessed.url,
            published_at=assessed.published_at,
            source=assessed.source,
            item_kind="remarks" if "/remarks/" in url else "policy",
            impact_score=assessed.impact_score,
            impact_level=assessed.impact_level,
            matched_signals=assessed.matched_signals,
            matched_terms=assessed.matched_terms,
        )

    def _extract_published_at(self, soup: BeautifulSoup) -> datetime | None:
        time_node = soup.select_one("time[datetime]")
        if time_node is not None:
            parsed = parse_iso_datetime(time_node.get("datetime"))
            if parsed is not None:
                return parsed

        meta_node = soup.select_one("meta[property='article:published_time']")
        if meta_node is not None:
            parsed = parse_iso_datetime(meta_node.get("content"))
            if parsed is not None:
                return parsed

        canonical_node = soup.select_one("link[rel='canonical'][href]")
        if canonical_node is not None:
            canonical = canonical_node.get("href")
            if canonical:
                return self._parse_date_from_url(urljoin("https://www.whitehouse.gov", canonical))

        return None

    def _parse_date_from_url(self, url: str) -> datetime | None:
        parts = [part for part in url.split("/") if part]
        if len(parts) < 5:
            return None

        try:
            year = int(parts[-3])
            month = int(parts[-2])
        except ValueError:
            return None

        return datetime(year, month, 1, tzinfo=UTC)
