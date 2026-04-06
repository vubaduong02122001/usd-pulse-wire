from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from app.models import NewsItem
from app.services.sources import RawNewsItem


@dataclass(frozen=True, slots=True)
class SignalGroup:
    label: str
    category: str
    weight: int
    keywords: tuple[str, ...]


SIGNAL_GROUPS: tuple[SignalGroup, ...] = (
    SignalGroup(
        label="Fed policy",
        category="Monetary Policy",
        weight=34,
        keywords=(
            "fomc",
            "fed funds",
            "rate cut",
            "rate hike",
            "interest rate",
            "monetary policy",
            "balance sheet",
            "dot plot",
            "quantitative tightening",
        ),
    ),
    SignalGroup(
        label="Inflation",
        category="Inflation",
        weight=32,
        keywords=(
            "inflation",
            "cpi",
            "consumer price",
            "producer price",
            "ppi",
            "core pce",
            "pce",
            "personal income and outlays",
        ),
    ),
    SignalGroup(
        label="Labor",
        category="Labor",
        weight=30,
        keywords=(
            "payroll",
            "nonfarm",
            "labor market",
            "jobs report",
            "employment",
            "unemployment",
            "jobless",
        ),
    ),
    SignalGroup(
        label="Growth",
        category="Growth",
        weight=26,
        keywords=(
            "gdp",
            "gross domestic product",
            "retail sales",
            "consumer spending",
            "durable goods",
            "trade in goods and services",
            "trade balance",
            "current account",
            "international trade",
        ),
    ),
    SignalGroup(
        label="Treasury / fiscal",
        category="Fiscal",
        weight=24,
        keywords=(
            "treasury international capital",
            "tic",
            "debt issuance",
            "borrowing",
            "auction",
            "fiscal",
            "tax",
            "global tax",
            "oecd",
            "sanctions",
            "tariff",
            "trade policy",
            "import",
            "imports",
            "export",
            "exports",
            "aluminum",
            "steel",
            "copper",
            "pharmaceutical",
            "financial stability",
            "regulators",
            "liquidity regulation",
            "stablecoin",
            "capital requirements",
        ),
    ),
    SignalGroup(
        label="FX / rates",
        category="Dollar Flows",
        weight=24,
        keywords=(
            "usd",
            "u s dollar",
            "dollar",
            "dxy",
            "foreign exchange",
            "fx",
            "treasury yields",
            "treasury yield",
            "bond market",
            "yield curve",
        ),
    ),
    SignalGroup(
        label="Risk sentiment",
        category="Risk Sentiment",
        weight=18,
        keywords=(
            "crude oil",
            "oil prices",
            "strait of hormuz",
            "trade deal",
            "trade war",
            "capital flows",
            "market volatility",
            "safe haven",
            "risk off",
        ),
    ),
)

TERM_BONUSES = {
    "fomc": 20,
    "economic projections": 12,
    "payroll": 12,
    "nonfarm": 12,
    "unemployment": 8,
    "cpi": 14,
    "pce": 14,
    "inflation": 10,
    "gdp": 8,
    "gross domestic product": 8,
    "personal income and outlays": 10,
    "trade balance": 7,
    "tariff": 9,
    "global tax": 8,
    "oecd": 6,
    "import": 6,
    "imports": 6,
    "aluminum": 6,
    "steel": 6,
    "copper": 6,
    "interest rate": 12,
    "rate cut": 12,
    "rate hike": 12,
    "treasury yields": 8,
    "treasury yield": 8,
}


SPACE_RE = re.compile(r"\s+")
PUNCT_RE = re.compile(r"[^a-z0-9\s]")


def normalize_text(value: str) -> str:
    lowered = value.casefold()
    sanitized = PUNCT_RE.sub(" ", lowered)
    return SPACE_RE.sub(" ", sanitized).strip()


def term_matches(text: str, term: str) -> bool:
    escaped = re.escape(term).replace(r"\ ", r"\s+")
    return re.search(rf"\b{escaped}\b", text) is not None


def make_item_id(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()


def freshness_bonus(published_at: datetime, now: datetime) -> int:
    age_seconds = max(0, int((now - published_at).total_seconds()))
    if age_seconds <= 2 * 60 * 60:
        return 10
    if age_seconds <= 12 * 60 * 60:
        return 6
    if age_seconds <= 24 * 60 * 60:
        return 3
    return 0


def impact_level(score: int) -> str:
    if score >= 76:
        return "high"
    if score >= 48:
        return "medium"
    return "watch"


def assess_raw_item(raw: RawNewsItem, *, now: datetime | None = None) -> NewsItem | None:
    now = now or datetime.now(timezone.utc)
    if (now - raw.published_at).days > 60:
        return None

    text = normalize_text(" ".join([raw.title, raw.summary, *raw.source_categories]))

    matched_signals: list[str] = []
    matched_terms: list[str] = []
    categories = {category for category in raw.source_categories if category}
    score = 0
    bonus = 0

    for group in SIGNAL_GROUPS:
        group_hits = [term for term in group.keywords if term_matches(text, term)]
        if not group_hits:
            continue

        matched_signals.append(group.label)
        matched_terms.extend(group_hits[:2])
        categories.add(group.category)
        score += group.weight + min(4, len(group_hits) - 1)
        bonus += sum(TERM_BONUSES.get(term, 0) for term in group_hits)

    if not matched_signals:
        return None

    trust_component = round(raw.trust_score * 14)
    official_bonus = 10 if raw.source_kind == "official" else 0
    final_score = min(
        100,
        score
        + bonus
        + trust_component
        + official_bonus
        + freshness_bonus(raw.published_at, now),
    )

    if final_score < 36:
        return None

    return NewsItem(
        id=make_item_id(raw.url),
        source=raw.source,
        source_kind=raw.source_kind,
        source_home=raw.source_home,
        title=raw.title,
        summary=raw.summary,
        url=raw.url,
        published_at=raw.published_at,
        detected_at=now,
        categories=sorted(categories),
        matched_signals=matched_signals[:4],
        matched_terms=list(dict.fromkeys(matched_terms))[:5],
        impact_score=final_score,
        impact_level=impact_level(final_score),
        trust_score=raw.trust_score,
    )
