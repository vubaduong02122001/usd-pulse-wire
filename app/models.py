from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class NewsItem(BaseModel):
    id: str
    source: str
    source_kind: str
    source_home: str
    title: str
    summary: str
    url: str
    published_at: datetime
    detected_at: datetime
    categories: list[str]
    matched_signals: list[str]
    matched_terms: list[str]
    impact_score: int
    impact_level: str
    trust_score: float


class SourceStatus(BaseModel):
    name: str
    kind: str
    homepage: str
    last_success_at: datetime | None = None
    last_error: str | None = None
    last_item_count: int = 0


class HubStatus(BaseModel):
    poll_interval_seconds: int
    connected_sources: int
    subscriber_count: int
    tracked_items: int
    last_sync_at: datetime | None = None
    last_new_item_at: datetime | None = None
    last_refresh_count: int = 0
    sources: list[SourceStatus]


class AssetQuote(BaseModel):
    symbol: str
    label: str
    group: str
    venue: str
    currency: str
    last: float
    previous_close: float | None = None
    absolute_change: float | None = None
    percent_change: float | None = None
    day_high: float | None = None
    day_low: float | None = None
    updated_at: datetime
    direction: str


class MarketSnapshot(BaseModel):
    updated_at: datetime
    source: str
    quotes: list[AssetQuote]


class TimelinePoint(BaseModel):
    at: datetime
    label: str
    display_value: str | None = None
    numeric_value: float | None = None
    note: str | None = None


class MacroIndicator(BaseModel):
    id: str
    title: str
    category: str
    frequency: str
    source: str
    source_url: str
    unit: str
    signals: list[str]
    current_display: str
    previous_display: str | None = None
    current_value: float | None = None
    previous_value: float | None = None
    updated_at: datetime
    history: list[TimelinePoint]
    note: str | None = None


class ScheduledEvent(BaseModel):
    id: str
    title: str
    category: str
    frequency: str
    importance: str
    source: str
    source_url: str
    signals: list[str]
    summary: str
    scheduled_at: datetime | None = None
    last_release_at: datetime | None = None
    history: list[TimelinePoint]
    note: str | None = None


class CalendarSnapshot(BaseModel):
    updated_at: datetime
    source: str
    indicators: list[MacroIndicator]
    schedule: list[ScheduledEvent]


class SpeechTapeItem(BaseModel):
    id: str
    title: str
    summary: str
    url: str
    published_at: datetime
    source: str
    item_kind: str
    impact_score: int
    impact_level: str
    matched_signals: list[str]
    matched_terms: list[str]


class SpeechSnapshot(BaseModel):
    updated_at: datetime
    source: str
    items: list[SpeechTapeItem]


class PriceBar(BaseModel):
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None


class AssetChart(BaseModel):
    updated_at: datetime
    source: str
    symbol: str
    label: str
    group: str
    interval: str
    range: str
    bars: list[PriceBar]


class QuantRegime(BaseModel):
    label: str
    usd_bias: str
    volatility_state: str
    headline_risk: str
    confidence: int
    summary: str
    actions: list[str]
    focus_assets: list[str]


class QuantAssetOutlook(BaseModel):
    asset: str
    label: str
    group: str
    spot: float | None = None
    one_day_bias: str
    one_week_bias: str
    expected_move_pct_1d: float | None = None
    expected_move_pct_1w: float | None = None
    range_low: float | None = None
    range_high: float | None = None
    stop_level: float | None = None
    take_profit_level: float | None = None
    volatility_regime: str
    risk_stance: str
    confidence: int
    trend_score: float
    news_score: float
    event_score: float
    driver_summary: str
    drivers: list[str]


class QuantSnapshot(BaseModel):
    updated_at: datetime
    source: str
    regime: QuantRegime
    assets: list[QuantAssetOutlook]
