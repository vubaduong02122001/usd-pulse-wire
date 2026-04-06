from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx

from app.config import Settings
from app.models import AssetChart, AssetQuote, MarketSnapshot, PriceBar


UTC = timezone.utc


@dataclass(frozen=True, slots=True)
class TrackedAsset:
    symbol: str
    label: str
    group: str


@dataclass(frozen=True, slots=True)
class ChartPreset:
    timeframe: str
    interval: str
    range: str
    cache_seconds: int


TRACKED_ASSETS: tuple[TrackedAsset, ...] = (
    TrackedAsset("DX-Y.NYB", "DXY", "Dollar"),
    TrackedAsset("EURUSD=X", "EURUSD", "FX"),
    TrackedAsset("JPY=X", "USDJPY", "FX"),
    TrackedAsset("GBPUSD=X", "GBPUSD", "FX"),
    TrackedAsset("GC=F", "GOLD", "Commodity"),
    TrackedAsset("CL=F", "WTI", "Commodity"),
    TrackedAsset("^TNX", "US10Y", "Rates"),
    TrackedAsset("^GSPC", "SPX", "Equity"),
    TrackedAsset("BTC-USD", "BTCUSD", "Crypto"),
    TrackedAsset("ETH-USD", "ETHUSD", "Crypto"),
)

TRACKED_BY_LABEL = {asset.label: asset for asset in TRACKED_ASSETS}
TRACKED_BY_SYMBOL = {asset.symbol: asset for asset in TRACKED_ASSETS}
TIMEFRAME_PRESETS = {
    preset.timeframe: preset
    for preset in (
        ChartPreset("1D", "5m", "1d", 20),
        ChartPreset("5D", "15m", "5d", 40),
        ChartPreset("1M", "1h", "1mo", 120),
        ChartPreset("3M", "1d", "3mo", 300),
        ChartPreset("1Y", "1wk", "1y", 900),
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
        config = TRACKED_BY_LABEL.get(needle) or TRACKED_BY_SYMBOL.get(asset.strip())
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

            try:
                response = await self._client.get(
                    "https://query1.finance.yahoo.com/v7/finance/spark",
                    params={
                        "symbols": ",".join(asset.symbol for asset in TRACKED_ASSETS),
                        "interval": "1m",
                        "range": "1d",
                    },
                )
                response.raise_for_status()

                payload = response.json()
                result = payload.get("spark", {}).get("result", [])
                quotes = [
                    quote
                    for entry in result
                    if (quote := self._build_quote_from_spark(entry, now)) is not None
                ]

                quotes.sort(key=lambda quote: (quote.group, quote.label))
                self._snapshot = MarketSnapshot(
                    updated_at=now,
                    source="Yahoo Finance spark",
                    quotes=quotes,
                )
            except httpx.HTTPError:
                if self._snapshot is None:
                    self._snapshot = MarketSnapshot(
                        updated_at=now,
                        source="Market feed temporarily rate-limited",
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
                response = await self._client.get(
                    f"https://query1.finance.yahoo.com/v8/finance/chart/{config.symbol}",
                    params={
                        "interval": preset.interval,
                        "range": preset.range,
                        "includePrePost": "false",
                        "events": "div,splits",
                    },
                )
                response.raise_for_status()

                payload = response.json()
                result = (payload.get("chart") or {}).get("result") or []
                if not result:
                    raise RuntimeError(f"No chart data returned for {config.label}.")

                entry = result[0]
                timestamps = entry.get("timestamp") or []
                indicators = (((entry.get("indicators") or {}).get("quote")) or [{}])[0]
                opens = indicators.get("open") or []
                highs = indicators.get("high") or []
                lows = indicators.get("low") or []
                closes = indicators.get("close") or []
                volumes = indicators.get("volume") or []
                bars: list[PriceBar] = []

                for index, timestamp in enumerate(timestamps):
                    close = closes[index] if index < len(closes) else None
                    if close is None:
                        continue

                    open_value = (
                        opens[index] if index < len(opens) and opens[index] is not None else close
                    )
                    high_value = (
                        highs[index] if index < len(highs) and highs[index] is not None else close
                    )
                    low_value = (
                        lows[index] if index < len(lows) and lows[index] is not None else close
                    )
                    volume = volumes[index] if index < len(volumes) else None

                    bars.append(
                        PriceBar(
                            time=datetime.fromtimestamp(int(timestamp), tz=UTC),
                            open=round(float(open_value), quote_digits(config)),
                            high=round(float(high_value), quote_digits(config)),
                            low=round(float(low_value), quote_digits(config)),
                            close=round(float(close), quote_digits(config)),
                            volume=None if volume is None else float(volume),
                        )
                    )

                chart = AssetChart(
                    updated_at=now,
                    source="Yahoo Finance chart",
                    symbol=config.symbol,
                    label=config.label,
                    group=config.group,
                    interval=preset.interval,
                    range=preset.range,
                    bars=bars,
                )
                self._chart_cache[cache_key] = chart
            except httpx.HTTPError:
                if cached is not None:
                    return cached
                chart = AssetChart(
                    updated_at=now,
                    source="Chart feed temporarily rate-limited",
                    symbol=config.symbol,
                    label=config.label,
                    group=config.group,
                    interval=preset.interval,
                    range=preset.range,
                    bars=[],
                )
            return chart

    def _build_quote_from_spark(
        self,
        entry: dict,
        now: datetime,
    ) -> AssetQuote | None:
        symbol = entry.get("symbol")
        config = TRACKED_BY_SYMBOL.get(symbol)
        responses = entry.get("response") or []
        if config is None or not responses:
            return None

        meta = responses[0].get("meta", {})
        if not meta:
            return None

        last = meta.get("regularMarketPrice")
        if last is None:
            return None

        previous_close = meta.get("previousClose") or meta.get("chartPreviousClose")
        absolute_change = (
            None if previous_close in (None, 0) else float(last) - float(previous_close)
        )
        percent_change = (
            None
            if previous_close in (None, 0)
            else (float(last) - float(previous_close)) / float(previous_close) * 100
        )

        direction = "flat"
        if absolute_change is not None:
            if absolute_change > 0:
                direction = "up"
            elif absolute_change < 0:
                direction = "down"

        updated_at = datetime.fromtimestamp(
            int(meta.get("regularMarketTime", now.timestamp())),
            tz=UTC,
        )
        return AssetQuote(
            symbol=symbol,
            label=config.label,
            group=config.group,
            venue=meta.get("fullExchangeName") or meta.get("exchangeName") or "",
            currency=meta.get("currency") or "USD",
            last=float(last),
            previous_close=None if previous_close is None else float(previous_close),
            absolute_change=absolute_change,
            percent_change=percent_change,
            day_high=(
                None
                if meta.get("regularMarketDayHigh") in (None, 0)
                else float(meta.get("regularMarketDayHigh"))
            ),
            day_low=(
                None
                if meta.get("regularMarketDayLow") in (None, 0)
                else float(meta.get("regularMarketDayLow"))
            ),
            updated_at=updated_at,
            direction=direction,
        )
