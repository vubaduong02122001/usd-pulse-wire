from __future__ import annotations

import asyncio
import csv
import io
import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx

from app.config import Settings
from app.models import AssetChart, AssetQuote, MarketSnapshot, PriceBar


UTC = timezone.utc


@dataclass(frozen=True, slots=True)
class TrackedAsset:
    label: str
    group: str
    snapshot_kind: str
    snapshot_id: str
    chart_kind: str
    chart_id: str
    venue: str
    currency: str = "USD"
    assetclass: str | None = None
    note: str = ""


@dataclass(frozen=True, slots=True)
class ChartPreset:
    timeframe: str
    cache_seconds: int
    fred_days: int
    nasdaq_days: int
    coinbase_days: int
    coinbase_granularity: int


TRACKED_ASSETS: tuple[TrackedAsset, ...] = (
    TrackedAsset(
        label="DXY",
        group="Dollar",
        snapshot_kind="fred",
        snapshot_id="DTWEXBGS",
        chart_kind="fred",
        chart_id="DTWEXBGS",
        venue="FRED broad dollar index",
        currency="IDX",
        note="Broad trade-weighted USD index proxy.",
    ),
    TrackedAsset(
        label="EURUSD",
        group="FX",
        snapshot_kind="fred",
        snapshot_id="DEXUSEU",
        chart_kind="fred",
        chart_id="DEXUSEU",
        venue="FRED EUR/USD",
    ),
    TrackedAsset(
        label="USDJPY",
        group="FX",
        snapshot_kind="fred",
        snapshot_id="DEXJPUS",
        chart_kind="fred",
        chart_id="DEXJPUS",
        venue="FRED USD/JPY",
        currency="JPY",
    ),
    TrackedAsset(
        label="GBPUSD",
        group="FX",
        snapshot_kind="fred",
        snapshot_id="DEXUSUK",
        chart_kind="fred",
        chart_id="DEXUSUK",
        venue="FRED GBP/USD",
    ),
    TrackedAsset(
        label="GOLD",
        group="Commodity",
        snapshot_kind="nasdaq",
        snapshot_id="GLD",
        chart_kind="nasdaq",
        chart_id="GLD",
        venue="Nasdaq GLD",
        assetclass="etf",
        note="GLD ETF proxy for spot gold.",
    ),
    TrackedAsset(
        label="WTI",
        group="Commodity",
        snapshot_kind="nasdaq",
        snapshot_id="USO",
        chart_kind="nasdaq",
        chart_id="USO",
        venue="Nasdaq USO",
        assetclass="etf",
        note="USO ETF proxy for WTI crude.",
    ),
    TrackedAsset(
        label="US10Y",
        group="Rates",
        snapshot_kind="fred",
        snapshot_id="DGS10",
        chart_kind="fred",
        chart_id="DGS10",
        venue="FRED 10Y Treasury yield",
        currency="%",
    ),
    TrackedAsset(
        label="SPX",
        group="Equity",
        snapshot_kind="fred",
        snapshot_id="SP500",
        chart_kind="fred",
        chart_id="SP500",
        venue="FRED S&P 500",
    ),
    TrackedAsset(
        label="BTCUSD",
        group="Crypto",
        snapshot_kind="coinbase",
        snapshot_id="BTC-USD",
        chart_kind="coinbase",
        chart_id="BTC-USD",
        venue="Coinbase Exchange",
    ),
    TrackedAsset(
        label="ETHUSD",
        group="Crypto",
        snapshot_kind="coinbase",
        snapshot_id="ETH-USD",
        chart_kind="coinbase",
        chart_id="ETH-USD",
        venue="Coinbase Exchange",
    ),
)

TRACKED_BY_LABEL = {asset.label: asset for asset in TRACKED_ASSETS}
TIMEFRAME_PRESETS = {
    preset.timeframe: preset
    for preset in (
        ChartPreset("1D", cache_seconds=20, fred_days=7, nasdaq_days=14, coinbase_days=1, coinbase_granularity=300),
        ChartPreset("5D", cache_seconds=30, fred_days=21, nasdaq_days=30, coinbase_days=5, coinbase_granularity=3600),
        ChartPreset("1M", cache_seconds=60, fred_days=45, nasdaq_days=60, coinbase_days=30, coinbase_granularity=21600),
        ChartPreset("3M", cache_seconds=120, fred_days=120, nasdaq_days=120, coinbase_days=90, coinbase_granularity=86400),
        ChartPreset("1Y", cache_seconds=300, fred_days=400, nasdaq_days=400, coinbase_days=300, coinbase_granularity=86400),
    )
}


def quote_digits(asset: TrackedAsset) -> int:
    if asset.group == "FX":
        return 4 if asset.label != "USDJPY" else 3
    if asset.group in {"Rates", "Dollar"}:
        return 3
    if asset.group == "Crypto":
        return 2
    return 2


def parse_numeric(value: str | float | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    cleaned = (
        str(value)
        .replace("$", "")
        .replace("%", "")
        .replace(",", "")
        .replace("N/A", "")
        .strip()
    )
    if not cleaned:
        return None
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        return float(cleaned)
    except ValueError:
        return None


def direction_from_change(change: float | None) -> str:
    if change is None:
        return "flat"
    if change > 0:
        return "up"
    if change < 0:
        return "down"
    return "flat"


def round_price(value: float | None, asset: TrackedAsset) -> float | None:
    if value is None:
        return None
    return round(float(value), quote_digits(asset))


class MarketDataService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = asyncio.Lock()
        self._chart_lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None
        self._snapshot: MarketSnapshot | None = None
        self._chart_cache: dict[tuple[str, str], AssetChart] = {}

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.settings.http_timeout_seconds,
                headers={
                    "User-Agent": self.settings.request_user_agent,
                    "Accept": "application/json,text/plain,*/*",
                },
                follow_redirects=True,
            )

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def resolve_asset(self, asset: str) -> TrackedAsset:
        needle = asset.strip().upper()
        config = TRACKED_BY_LABEL.get(needle)
        if config is None:
            raise KeyError(f"Unsupported asset: {asset}")
        return config

    async def snapshot(self, *, force: bool = False) -> MarketSnapshot:
        async with self._lock:
            if self._client is None:
                raise RuntimeError("HTTP client is not initialized.")

            now = datetime.now(UTC)
            if (
                not force
                and self._snapshot is not None
                and now - self._snapshot.updated_at
                < timedelta(seconds=self.settings.market_cache_seconds)
            ):
                return self._snapshot

            results = await asyncio.gather(
                *(self._fetch_quote(asset) for asset in TRACKED_ASSETS),
                return_exceptions=True,
            )

            quotes: list[AssetQuote] = []
            for result in results:
                if isinstance(result, AssetQuote):
                    quotes.append(result)

            if quotes:
                quotes.sort(key=lambda quote: (quote.group, quote.label))
                source = "Mixed feeds: FRED + Nasdaq proxies + Coinbase"
                if len(quotes) < len(TRACKED_ASSETS):
                    source = (
                        f"Partial market tape {len(quotes)}/{len(TRACKED_ASSETS)} loaded | "
                        "FRED + Nasdaq proxies + Coinbase"
                    )
                self._snapshot = MarketSnapshot(
                    updated_at=now,
                    source=source,
                    quotes=quotes,
                )
                return self._snapshot

            if self._snapshot is not None:
                return self._snapshot

            self._snapshot = MarketSnapshot(
                updated_at=now,
                source="Market feeds temporarily unavailable",
                quotes=[],
            )
            return self._snapshot

    async def chart(self, asset: str, timeframe: str = "5D", *, force: bool = False) -> AssetChart:
        async with self._chart_lock:
            if self._client is None:
                raise RuntimeError("HTTP client is not initialized.")

            config = self.resolve_asset(asset)
            preset = TIMEFRAME_PRESETS.get(timeframe.upper(), TIMEFRAME_PRESETS["5D"])
            now = datetime.now(UTC)
            cache_key = (config.label, preset.timeframe)
            cached = self._chart_cache.get(cache_key)
            if (
                not force
                and cached is not None
                and now - cached.updated_at < timedelta(seconds=preset.cache_seconds)
            ):
                return cached

            try:
                if config.chart_kind == "fred":
                    chart = await self._chart_from_fred(config, preset, now)
                elif config.chart_kind == "nasdaq":
                    chart = await self._chart_from_nasdaq(config, preset, now)
                elif config.chart_kind == "coinbase":
                    chart = await self._chart_from_coinbase(config, preset, now)
                else:
                    raise RuntimeError(f"Unsupported chart provider for {config.label}.")
                self._chart_cache[cache_key] = chart
                return chart
            except Exception:
                if cached is not None:
                    return cached
                return AssetChart(
                    updated_at=now,
                    source="Chart feed temporarily unavailable",
                    symbol=config.chart_id,
                    label=config.label,
                    group=config.group,
                    interval="n/a",
                    range=preset.timeframe,
                    bars=[],
                )

    async def _fetch_quote(self, asset: TrackedAsset) -> AssetQuote | None:
        try:
            if asset.snapshot_kind == "fred":
                return await self._quote_from_fred(asset)
            if asset.snapshot_kind == "nasdaq":
                return await self._quote_from_nasdaq(asset)
            if asset.snapshot_kind == "coinbase":
                return await self._quote_from_coinbase(asset)
        except Exception:
            return None
        return None

    async def _quote_from_fred(self, asset: TrackedAsset) -> AssetQuote | None:
        points = await self._fetch_fred_points(asset.snapshot_id, days=40)
        if len(points) < 2:
            return None

        recent = points[-5:] if len(points) >= 5 else points
        updated_at, last = points[-1]
        previous_close = points[-2][1]
        absolute_change = float(last) - float(previous_close)
        percent_change = (
            None
            if previous_close == 0
            else (float(last) - float(previous_close)) / float(previous_close) * 100
        )

        return AssetQuote(
            symbol=asset.snapshot_id,
            label=asset.label,
            group=asset.group,
            venue=asset.venue,
            currency=asset.currency,
            last=round(float(last), quote_digits(asset)),
            previous_close=round_price(previous_close, asset),
            absolute_change=round_price(absolute_change, asset),
            percent_change=None if percent_change is None else round(percent_change, 2),
            day_high=round_price(max(value for _, value in recent), asset),
            day_low=round_price(min(value for _, value in recent), asset),
            updated_at=updated_at,
            direction=direction_from_change(absolute_change),
        )

    async def _quote_from_nasdaq(self, asset: TrackedAsset) -> AssetQuote | None:
        rows = await self._fetch_nasdaq_rows(asset.chart_id, asset.assetclass or "stocks", days=30, limit=10)
        if len(rows) < 2:
            return None

        latest = rows[0]
        previous = rows[1]
        last = parse_numeric(latest.get("close"))
        previous_close = parse_numeric(previous.get("close"))
        if last is None or previous_close is None:
            return None

        absolute_change = last - previous_close
        percent_change = None if previous_close == 0 else absolute_change / previous_close * 100
        updated_at = datetime.strptime(latest["date"], "%m/%d/%Y").replace(tzinfo=UTC)

        return AssetQuote(
            symbol=asset.chart_id,
            label=asset.label,
            group=asset.group,
            venue=asset.note or asset.venue,
            currency=asset.currency,
            last=round(float(last), quote_digits(asset)),
            previous_close=round_price(previous_close, asset),
            absolute_change=round_price(absolute_change, asset),
            percent_change=None if percent_change is None else round(percent_change, 2),
            day_high=round_price(parse_numeric(latest.get("high")), asset),
            day_low=round_price(parse_numeric(latest.get("low")), asset),
            updated_at=updated_at,
            direction=direction_from_change(absolute_change),
        )

    async def _quote_from_coinbase(self, asset: TrackedAsset) -> AssetQuote | None:
        if self._client is None:
            return None

        response = await self._client.get(
            f"https://api.exchange.coinbase.com/products/{asset.snapshot_id}/stats"
        )
        response.raise_for_status()
        payload = response.json()
        last = parse_numeric(payload.get("last"))
        previous_close = parse_numeric(payload.get("open"))
        if last is None or previous_close is None:
            return None

        absolute_change = last - previous_close
        percent_change = None if previous_close == 0 else absolute_change / previous_close * 100
        now = datetime.now(UTC)
        return AssetQuote(
            symbol=asset.snapshot_id,
            label=asset.label,
            group=asset.group,
            venue=asset.venue,
            currency=asset.currency,
            last=round(float(last), quote_digits(asset)),
            previous_close=round_price(previous_close, asset),
            absolute_change=round_price(absolute_change, asset),
            percent_change=None if percent_change is None else round(percent_change, 2),
            day_high=round_price(parse_numeric(payload.get("high")), asset),
            day_low=round_price(parse_numeric(payload.get("low")), asset),
            updated_at=now,
            direction=direction_from_change(absolute_change),
        )

    async def _chart_from_fred(
        self,
        asset: TrackedAsset,
        preset: ChartPreset,
        now: datetime,
    ) -> AssetChart:
        points = await self._fetch_fred_points(asset.chart_id, days=preset.fred_days)
        if not points:
            raise RuntimeError(f"No FRED points for {asset.label}.")

        bars: list[PriceBar] = []
        previous_close: float | None = None
        for moment, close in points:
            open_value = close if previous_close is None else previous_close
            bars.append(
                PriceBar(
                    time=moment,
                    open=round(float(open_value), quote_digits(asset)),
                    high=round(max(float(open_value), float(close)), quote_digits(asset)),
                    low=round(min(float(open_value), float(close)), quote_digits(asset)),
                    close=round(float(close), quote_digits(asset)),
                    volume=None,
                )
            )
            previous_close = close

        source = asset.venue
        if asset.note:
            source = f"{source} | {asset.note}"

        return AssetChart(
            updated_at=now,
            source=source,
            symbol=asset.chart_id,
            label=asset.label,
            group=asset.group,
            interval="1d",
            range=preset.timeframe,
            bars=bars,
        )

    async def _chart_from_nasdaq(
        self,
        asset: TrackedAsset,
        preset: ChartPreset,
        now: datetime,
    ) -> AssetChart:
        rows = await self._fetch_nasdaq_rows(
            asset.chart_id,
            asset.assetclass or "stocks",
            days=preset.nasdaq_days,
            limit=min(365, max(40, preset.nasdaq_days + 10)),
        )
        if not rows:
            raise RuntimeError(f"No Nasdaq rows for {asset.label}.")

        bars: list[PriceBar] = []
        for row in reversed(rows):
            open_value = parse_numeric(row.get("open"))
            high_value = parse_numeric(row.get("high"))
            low_value = parse_numeric(row.get("low"))
            close_value = parse_numeric(row.get("close"))
            volume = parse_numeric(row.get("volume"))
            if None in {open_value, high_value, low_value, close_value}:
                continue

            bars.append(
                PriceBar(
                    time=datetime.strptime(row["date"], "%m/%d/%Y").replace(tzinfo=UTC),
                    open=round(float(open_value), quote_digits(asset)),
                    high=round(float(high_value), quote_digits(asset)),
                    low=round(float(low_value), quote_digits(asset)),
                    close=round(float(close_value), quote_digits(asset)),
                    volume=volume,
                )
            )

        source = asset.venue
        if asset.note:
            source = f"{source} | {asset.note}"

        return AssetChart(
            updated_at=now,
            source=source,
            symbol=asset.chart_id,
            label=asset.label,
            group=asset.group,
            interval="1d",
            range=preset.timeframe,
            bars=bars,
        )

    async def _chart_from_coinbase(
        self,
        asset: TrackedAsset,
        preset: ChartPreset,
        now: datetime,
    ) -> AssetChart:
        if self._client is None:
            raise RuntimeError("HTTP client is not initialized.")

        end = now
        start = end - timedelta(days=preset.coinbase_days)
        response = await self._client.get(
            f"https://api.exchange.coinbase.com/products/{asset.chart_id}/candles",
            params={
                "granularity": preset.coinbase_granularity,
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not payload:
            raise RuntimeError(f"No Coinbase candles for {asset.label}.")

        bars: list[PriceBar] = []
        for timestamp, low, high, open_value, close, volume in sorted(payload, key=lambda row: row[0]):
            bars.append(
                PriceBar(
                    time=datetime.fromtimestamp(int(timestamp), tz=UTC),
                    open=round(float(open_value), quote_digits(asset)),
                    high=round(float(high), quote_digits(asset)),
                    low=round(float(low), quote_digits(asset)),
                    close=round(float(close), quote_digits(asset)),
                    volume=float(volume),
                )
            )

        return AssetChart(
            updated_at=now,
            source=asset.venue,
            symbol=asset.chart_id,
            label=asset.label,
            group=asset.group,
            interval=str(preset.coinbase_granularity),
            range=preset.timeframe,
            bars=bars,
        )

    async def _fetch_fred_points(self, series_id: str, *, days: int) -> list[tuple[datetime, float]]:
        if self._client is None:
            raise RuntimeError("HTTP client is not initialized.")

        start_date = (datetime.now(UTC) - timedelta(days=days)).strftime("%Y-%m-%d")
        response = await self._client.get(
            "https://fred.stlouisfed.org/graph/fredgraph.csv",
            params={"id": series_id, "cosd": start_date},
        )
        response.raise_for_status()
        text = response.text
        if text.lstrip().startswith("<!DOCTYPE html>"):
            raise RuntimeError(f"FRED CSV unavailable for {series_id}.")

        reader = csv.DictReader(io.StringIO(text))
        points: list[tuple[datetime, float]] = []
        for row in reader:
            raw_value = (row.get(series_id) or "").strip()
            if raw_value in {"", ".", "NaN"}:
                continue
            value = parse_numeric(raw_value)
            date_value = row.get("observation_date")
            if value is None or not date_value:
                continue
            points.append(
                (
                    datetime.strptime(date_value, "%Y-%m-%d").replace(tzinfo=UTC),
                    float(value),
                )
            )
        return points

    async def _fetch_nasdaq_rows(
        self,
        symbol: str,
        assetclass: str,
        *,
        days: int,
        limit: int,
    ) -> list[dict]:
        end = datetime.now(UTC).date()
        start = end - timedelta(days=days)
        query = urllib.parse.urlencode(
            {
                "assetclass": assetclass,
                "fromdate": start.isoformat(),
                "todate": end.isoformat(),
                "limit": limit,
            }
        )
        url = f"https://api.nasdaq.com/api/quote/{symbol}/historical?{query}"

        def run_request() -> dict:
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Origin": "https://www.nasdaq.com",
                    "Referer": "https://www.nasdaq.com/",
                },
            )
            with urllib.request.urlopen(
                request,
                timeout=max(30, self.settings.http_timeout_seconds * 2),
            ) as response:
                return json.loads(response.read().decode("utf-8", "replace"))

        payload = await asyncio.to_thread(run_request)
        rows = (((payload.get("data") or {}).get("tradesTable") or {}).get("rows")) or []
        if not isinstance(rows, list):
            return []
        return rows
