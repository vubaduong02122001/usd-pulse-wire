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
    note: str = ""
    fallback_snapshot_kind: str | None = None
    fallback_snapshot_id: str | None = None
    fallback_chart_kind: str | None = None
    fallback_chart_id: str | None = None


@dataclass(frozen=True, slots=True)
class ChartPreset:
    timeframe: str
    cache_seconds: int
    yahoo_interval: str
    yahoo_range: str
    fred_days: int
    coinbase_days: int
    coinbase_granularity: int


TRACKED_ASSETS: tuple[TrackedAsset, ...] = (
    TrackedAsset(
        label="DXY",
        group="Dollar",
        snapshot_kind="yahoo",
        snapshot_id="DX-Y.NYB",
        chart_kind="yahoo",
        chart_id="DX-Y.NYB",
        venue="ICE Futures US Dollar Index",
        currency="IDX",
        fallback_snapshot_kind="fred",
        fallback_snapshot_id="DTWEXBGS",
        fallback_chart_kind="fred",
        fallback_chart_id="DTWEXBGS",
        note="Fallback is FRED broad trade-weighted USD index proxy.",
    ),
    TrackedAsset(
        label="EURUSD",
        group="FX",
        snapshot_kind="yahoo",
        snapshot_id="EURUSD=X",
        chart_kind="yahoo",
        chart_id="EURUSD=X",
        venue="Yahoo Finance FX",
        fallback_snapshot_kind="fred",
        fallback_snapshot_id="DEXUSEU",
        fallback_chart_kind="fred",
        fallback_chart_id="DEXUSEU",
    ),
    TrackedAsset(
        label="USDJPY",
        group="FX",
        snapshot_kind="yahoo",
        snapshot_id="JPY=X",
        chart_kind="yahoo",
        chart_id="JPY=X",
        venue="Yahoo Finance FX",
        currency="JPY",
        fallback_snapshot_kind="fred",
        fallback_snapshot_id="DEXJPUS",
        fallback_chart_kind="fred",
        fallback_chart_id="DEXJPUS",
    ),
    TrackedAsset(
        label="GBPUSD",
        group="FX",
        snapshot_kind="yahoo",
        snapshot_id="GBPUSD=X",
        chart_kind="yahoo",
        chart_id="GBPUSD=X",
        venue="Yahoo Finance FX",
        fallback_snapshot_kind="fred",
        fallback_snapshot_id="DEXUSUK",
        fallback_chart_kind="fred",
        fallback_chart_id="DEXUSUK",
    ),
    TrackedAsset(
        label="GOLD",
        group="Commodity",
        snapshot_kind="yahoo",
        snapshot_id="GC=F",
        chart_kind="yahoo",
        chart_id="GC=F",
        venue="COMEX Gold Futures",
        fallback_snapshot_kind="stooq",
        fallback_snapshot_id="gc.f",
    ),
    TrackedAsset(
        label="WTI",
        group="Commodity",
        snapshot_kind="yahoo",
        snapshot_id="CL=F",
        chart_kind="yahoo",
        chart_id="CL=F",
        venue="NYMEX WTI Crude Futures",
        fallback_snapshot_kind="stooq",
        fallback_snapshot_id="cl.f",
        fallback_chart_kind="fred",
        fallback_chart_id="DCOILWTICO",
        note="Fallback chart is FRED WTI spot series synthesized into OHLC bars.",
    ),
    TrackedAsset(
        label="US10Y",
        group="Rates",
        snapshot_kind="yahoo",
        snapshot_id="^TNX",
        chart_kind="yahoo",
        chart_id="^TNX",
        venue="Cboe 10Y Treasury Yield Index",
        currency="%",
        fallback_snapshot_kind="fred",
        fallback_snapshot_id="DGS10",
        fallback_chart_kind="fred",
        fallback_chart_id="DGS10",
    ),
    TrackedAsset(
        label="SPX",
        group="Equity",
        snapshot_kind="yahoo",
        snapshot_id="^GSPC",
        chart_kind="yahoo",
        chart_id="^GSPC",
        venue="S&P 500 Index",
        fallback_snapshot_kind="fred",
        fallback_snapshot_id="SP500",
        fallback_chart_kind="fred",
        fallback_chart_id="SP500",
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
        ChartPreset("1D", cache_seconds=20, yahoo_interval="5m", yahoo_range="1d", fred_days=7, coinbase_days=1, coinbase_granularity=300),
        ChartPreset("5D", cache_seconds=30, yahoo_interval="15m", yahoo_range="5d", fred_days=21, coinbase_days=5, coinbase_granularity=3600),
        ChartPreset("1M", cache_seconds=60, yahoo_interval="1h", yahoo_range="1mo", fred_days=45, coinbase_days=30, coinbase_granularity=21600),
        ChartPreset("3M", cache_seconds=120, yahoo_interval="1d", yahoo_range="3mo", fred_days=120, coinbase_days=90, coinbase_granularity=86400),
        ChartPreset("1Y", cache_seconds=300, yahoo_interval="1wk", yahoo_range="1y", fred_days=400, coinbase_days=300, coinbase_granularity=86400),
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
    if isinstance(value, (float, int)):
        return float(value)

    cleaned = (
        str(value)
        .replace("$", "")
        .replace("%", "")
        .replace(",", "")
        .replace("N/D", "")
        .replace("N/A", "")
        .strip()
    )
    if not cleaned:
        return None
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
        config = TRACKED_BY_LABEL.get(asset.strip().upper())
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
            quotes = [result for result in results if isinstance(result, AssetQuote)]

            if quotes:
                quotes.sort(key=lambda quote: (quote.group, quote.label))
                source = "Mixed feeds: Yahoo chart + Coinbase + fallback FRED/Stooq"
                if len(quotes) < len(TRACKED_ASSETS):
                    source = (
                        f"Partial market tape {len(quotes)}/{len(TRACKED_ASSETS)} loaded | "
                        "Yahoo chart + Coinbase + fallback FRED/Stooq"
                    )
                self._snapshot = MarketSnapshot(updated_at=now, source=source, quotes=quotes)
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

            for kind, identifier in (
                (config.chart_kind, config.chart_id),
                (config.fallback_chart_kind, config.fallback_chart_id),
            ):
                if not kind or not identifier:
                    continue
                try:
                    chart = await self._chart_from_provider(config, preset, now, kind, identifier)
                except Exception:
                    continue
                if chart.bars:
                    self._chart_cache[cache_key] = chart
                    return chart

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
        for kind, identifier in (
            (asset.snapshot_kind, asset.snapshot_id),
            (asset.fallback_snapshot_kind, asset.fallback_snapshot_id),
        ):
            if not kind or not identifier:
                continue
            try:
                return await self._quote_from_provider(asset, kind, identifier)
            except Exception:
                continue
        return None

    async def _quote_from_provider(
        self,
        asset: TrackedAsset,
        kind: str,
        identifier: str,
    ) -> AssetQuote:
        if kind == "yahoo":
            return await self._quote_from_yahoo(asset, identifier)
        if kind == "fred":
            return await self._quote_from_fred(asset, identifier)
        if kind == "coinbase":
            return await self._quote_from_coinbase(asset, identifier)
        if kind == "stooq":
            return await self._quote_from_stooq(asset, identifier)
        raise RuntimeError(f"Unsupported quote provider: {kind}")

    async def _chart_from_provider(
        self,
        asset: TrackedAsset,
        preset: ChartPreset,
        now: datetime,
        kind: str,
        identifier: str,
    ) -> AssetChart:
        if kind == "yahoo":
            return await self._chart_from_yahoo(asset, identifier, preset, now)
        if kind == "fred":
            return await self._chart_from_fred(asset, identifier, preset, now)
        if kind == "coinbase":
            return await self._chart_from_coinbase(asset, identifier, preset, now)
        raise RuntimeError(f"Unsupported chart provider: {kind}")

    async def _quote_from_yahoo(self, asset: TrackedAsset, symbol: str) -> AssetQuote:
        payload = await self._fetch_yahoo_chart_payload(symbol, interval="5m", range_="1d")
        result = ((payload.get("chart") or {}).get("result") or [None])[0]
        if result is None:
            raise RuntimeError(f"No Yahoo quote data for {asset.label}.")

        meta = result.get("meta") or {}
        timestamps = result.get("timestamp") or []
        indicators = (((result.get("indicators") or {}).get("quote")) or [{}])[0]
        closes = [parse_numeric(value) for value in (indicators.get("close") or [])]
        highs = [parse_numeric(value) for value in (indicators.get("high") or [])]
        lows = [parse_numeric(value) for value in (indicators.get("low") or [])]

        last = parse_numeric(meta.get("regularMarketPrice"))
        if last is None:
            last = next((value for value in reversed(closes) if value is not None), None)
        previous_close = (
            parse_numeric(meta.get("previousClose"))
            or parse_numeric(meta.get("chartPreviousClose"))
            or next((value for value in closes if value is not None), None)
        )
        if last is None or previous_close is None:
            raise RuntimeError(f"Incomplete Yahoo quote data for {asset.label}.")

        absolute_change = float(last) - float(previous_close)
        percent_change = None if previous_close == 0 else absolute_change / float(previous_close) * 100
        updated_at = datetime.fromtimestamp(
            int(meta.get("regularMarketTime") or timestamps[-1] or datetime.now(UTC).timestamp()),
            tz=UTC,
        )

        intraday_highs = [value for value in highs if value is not None]
        intraday_lows = [value for value in lows if value is not None]
        day_high = parse_numeric(meta.get("regularMarketDayHigh"))
        day_low = parse_numeric(meta.get("regularMarketDayLow"))
        if day_high in {None, 0} and intraday_highs:
            day_high = max(intraday_highs)
        if day_low in {None, 0} and intraday_lows:
            day_low = min(intraday_lows)

        return AssetQuote(
            symbol=symbol,
            label=asset.label,
            group=asset.group,
            venue=asset.venue,
            currency=asset.currency,
            last=round(float(last), quote_digits(asset)),
            previous_close=round_price(previous_close, asset),
            absolute_change=round_price(absolute_change, asset),
            percent_change=None if percent_change is None else round(percent_change, 2),
            day_high=round_price(day_high, asset),
            day_low=round_price(day_low, asset),
            updated_at=updated_at,
            direction=direction_from_change(absolute_change),
        )

    async def _quote_from_fred(self, asset: TrackedAsset, series_id: str) -> AssetQuote:
        points = await self._fetch_fred_points(series_id, days=40)
        if len(points) < 2:
            raise RuntimeError(f"No FRED quote data for {asset.label}.")

        recent = points[-5:]
        updated_at, last = points[-1]
        previous_close = points[-2][1]
        absolute_change = float(last) - float(previous_close)
        percent_change = None if previous_close == 0 else absolute_change / float(previous_close) * 100

        venue = asset.venue if asset.snapshot_kind == "fred" else f"{asset.venue} fallback"
        note = asset.note if asset.label == "DXY" else ""
        if note and "Fallback" in note:
            venue = f"{venue} | {note}"

        return AssetQuote(
            symbol=series_id,
            label=asset.label,
            group=asset.group,
            venue=venue,
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

    async def _quote_from_stooq(self, asset: TrackedAsset, symbol: str) -> AssetQuote:
        line = await self._fetch_stooq_line(symbol)
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 7:
            raise RuntimeError(f"Invalid Stooq payload for {asset.label}.")

        open_value = parse_numeric(parts[3])
        high_value = parse_numeric(parts[4])
        low_value = parse_numeric(parts[5])
        close_value = parse_numeric(parts[6])
        if None in {open_value, high_value, low_value, close_value}:
            raise RuntimeError(f"Incomplete Stooq payload for {asset.label}.")

        absolute_change = float(close_value) - float(open_value)
        percent_change = None if open_value == 0 else absolute_change / float(open_value) * 100
        updated_at = self._parse_stooq_timestamp(parts[1], parts[2])

        return AssetQuote(
            symbol=symbol,
            label=asset.label,
            group=asset.group,
            venue=f"{asset.venue} | Stooq quote fallback",
            currency=asset.currency,
            last=round(float(close_value), quote_digits(asset)),
            previous_close=round_price(open_value, asset),
            absolute_change=round_price(absolute_change, asset),
            percent_change=None if percent_change is None else round(percent_change, 2),
            day_high=round_price(high_value, asset),
            day_low=round_price(low_value, asset),
            updated_at=updated_at,
            direction=direction_from_change(absolute_change),
        )

    async def _quote_from_coinbase(self, asset: TrackedAsset, product_id: str) -> AssetQuote:
        if self._client is None:
            raise RuntimeError("HTTP client is not initialized.")

        response = await self._client.get(f"https://api.exchange.coinbase.com/products/{product_id}/stats")
        response.raise_for_status()
        payload = response.json()
        last = parse_numeric(payload.get("last"))
        previous_close = parse_numeric(payload.get("open"))
        if last is None or previous_close is None:
            raise RuntimeError(f"Incomplete Coinbase quote data for {asset.label}.")

        absolute_change = last - previous_close
        percent_change = None if previous_close == 0 else absolute_change / previous_close * 100

        return AssetQuote(
            symbol=product_id,
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
            updated_at=datetime.now(UTC),
            direction=direction_from_change(absolute_change),
        )

    async def _chart_from_yahoo(
        self,
        asset: TrackedAsset,
        symbol: str,
        preset: ChartPreset,
        now: datetime,
    ) -> AssetChart:
        payload = await self._fetch_yahoo_chart_payload(
            symbol,
            interval=preset.yahoo_interval,
            range_=preset.yahoo_range,
        )
        result = ((payload.get("chart") or {}).get("result") or [None])[0]
        if result is None:
            raise RuntimeError(f"No Yahoo chart data for {asset.label}.")

        timestamps = result.get("timestamp") or []
        indicators = (((result.get("indicators") or {}).get("quote")) or [{}])[0]
        opens = indicators.get("open") or []
        highs = indicators.get("high") or []
        lows = indicators.get("low") or []
        closes = indicators.get("close") or []
        volumes = indicators.get("volume") or []

        bars: list[PriceBar] = []
        for index, timestamp in enumerate(timestamps):
            close = parse_numeric(closes[index] if index < len(closes) else None)
            if close is None:
                continue

            open_value = parse_numeric(opens[index] if index < len(opens) else close) or close
            high_value = parse_numeric(highs[index] if index < len(highs) else close) or close
            low_value = parse_numeric(lows[index] if index < len(lows) else close) or close
            volume = parse_numeric(volumes[index] if index < len(volumes) else None)

            bars.append(
                PriceBar(
                    time=datetime.fromtimestamp(int(timestamp), tz=UTC),
                    open=round(float(open_value), quote_digits(asset)),
                    high=round(float(high_value), quote_digits(asset)),
                    low=round(float(low_value), quote_digits(asset)),
                    close=round(float(close), quote_digits(asset)),
                    volume=volume,
                )
            )

        if not bars:
            raise RuntimeError(f"Empty Yahoo bars for {asset.label}.")

        return AssetChart(
            updated_at=now,
            source=asset.venue,
            symbol=symbol,
            label=asset.label,
            group=asset.group,
            interval=preset.yahoo_interval,
            range=preset.yahoo_range,
            bars=bars,
        )

    async def _chart_from_fred(
        self,
        asset: TrackedAsset,
        series_id: str,
        preset: ChartPreset,
        now: datetime,
    ) -> AssetChart:
        points = await self._fetch_fred_points(series_id, days=preset.fred_days)
        if not points:
            raise RuntimeError(f"No FRED chart data for {asset.label}.")

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

        source = asset.venue if asset.chart_kind == "fred" else f"{asset.venue} fallback"
        if asset.note and "Fallback" in asset.note:
            source = f"{source} | {asset.note}"

        return AssetChart(
            updated_at=now,
            source=source,
            symbol=series_id,
            label=asset.label,
            group=asset.group,
            interval="1d",
            range=preset.timeframe,
            bars=bars,
        )

    async def _chart_from_coinbase(
        self,
        asset: TrackedAsset,
        product_id: str,
        preset: ChartPreset,
        now: datetime,
    ) -> AssetChart:
        if self._client is None:
            raise RuntimeError("HTTP client is not initialized.")

        end = now
        start = end - timedelta(days=preset.coinbase_days)
        response = await self._client.get(
            f"https://api.exchange.coinbase.com/products/{product_id}/candles",
            params={
                "granularity": preset.coinbase_granularity,
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not payload:
            raise RuntimeError(f"No Coinbase bars for {asset.label}.")

        bars = [
            PriceBar(
                time=datetime.fromtimestamp(int(timestamp), tz=UTC),
                low=round(float(low), quote_digits(asset)),
                high=round(float(high), quote_digits(asset)),
                open=round(float(open_value), quote_digits(asset)),
                close=round(float(close), quote_digits(asset)),
                volume=float(volume),
            )
            for timestamp, low, high, open_value, close, volume in sorted(payload, key=lambda row: row[0])
        ]

        return AssetChart(
            updated_at=now,
            source=asset.venue,
            symbol=product_id,
            label=asset.label,
            group=asset.group,
            interval=str(preset.coinbase_granularity),
            range=preset.timeframe,
            bars=bars,
        )

    async def _fetch_yahoo_chart_payload(self, symbol: str, *, interval: str, range_: str) -> dict:
        if self._client is None:
            raise RuntimeError("HTTP client is not initialized.")

        response = await self._client.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(symbol, safe='=^-.')}",
            params={
                "interval": interval,
                "range": range_,
                "includePrePost": "false",
                "events": "div,splits",
            },
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"},
            timeout=max(20, self.settings.http_timeout_seconds + 5),
        )
        response.raise_for_status()
        return response.json()

    async def _fetch_fred_points(self, series_id: str, *, days: int) -> list[tuple[datetime, float]]:
        if self._client is None:
            raise RuntimeError("HTTP client is not initialized.")

        start_date = (datetime.now(UTC) - timedelta(days=days)).strftime("%Y-%m-%d")
        response = await self._client.get(
            "https://fred.stlouisfed.org/graph/fredgraph.csv",
            params={"id": series_id, "cosd": start_date},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=max(20, self.settings.http_timeout_seconds + 10),
        )
        response.raise_for_status()
        text = response.text
        if text.lstrip().startswith("<!DOCTYPE html>"):
            raise RuntimeError(f"FRED CSV unavailable for {series_id}.")

        reader = csv.DictReader(io.StringIO(text))
        points: list[tuple[datetime, float]] = []
        for row in reader:
            date_text = row.get("observation_date")
            value = parse_numeric(row.get(series_id))
            if not date_text or value is None:
                continue
            points.append(
                (
                    datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=UTC),
                    float(value),
                )
            )
        return points

    async def _fetch_stooq_line(self, symbol: str) -> str:
        url = f"https://stooq.com/q/l/?s={urllib.parse.quote(symbol)}"

        def run_request() -> str:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "text/plain,*/*"},
            )
            with urllib.request.urlopen(request, timeout=max(15, self.settings.http_timeout_seconds)) as response:
                return response.read().decode("utf-8", "replace").strip()

        return await asyncio.to_thread(run_request)

    def _parse_stooq_timestamp(self, date_text: str, time_text: str) -> datetime:
        if date_text == "N/D":
            return datetime.now(UTC)
        if time_text == "N/D":
            return datetime.strptime(date_text, "%Y%m%d").replace(tzinfo=UTC)
        return datetime.strptime(f"{date_text}{time_text}", "%Y%m%d%H%M%S").replace(tzinfo=UTC)
