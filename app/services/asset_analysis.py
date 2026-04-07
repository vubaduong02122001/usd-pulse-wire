from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import log, sqrt, tanh
from statistics import pstdev

from app.config import Settings
from app.models import (
    AnalysisFormula,
    AnalysisTimeframeState,
    AssetAnalysisSnapshot,
    AssetTradePlan,
    AssetQuote,
    NewsItem,
    PriceBar,
    ScheduledEvent,
    SpeechTapeItem,
)
from app.services.calendar_data import EconomicCalendarService
from app.services.hub import NewsHub
from app.services.market_data import (
    TRACKED_BY_LABEL,
    MarketDataService,
    TrackedAsset,
    parse_numeric,
    quote_digits,
)
from app.services.speech_data import SpeechTapeService


UTC = timezone.utc
ANALYSIS_TIMEFRAMES = ("1m", "3m", "5m", "15m", "30m", "45m", "1h", "2h", "4h")
TIMEFRAME_MINUTES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "45m": 45,
    "1h": 60,
    "2h": 120,
    "4h": 240,
}
TIMEFRAME_LIMITS = {
    "1m": 240,
    "3m": 220,
    "5m": 220,
    "15m": 180,
    "30m": 160,
    "45m": 140,
    "1h": 180,
    "2h": 160,
    "4h": 140,
}
TF_MACRO_WEIGHTS = {
    "1m": 0.05,
    "3m": 0.07,
    "5m": 0.09,
    "15m": 0.12,
    "30m": 0.14,
    "45m": 0.16,
    "1h": 0.18,
    "2h": 0.22,
    "4h": 0.28,
}
PRIMARY_TF = "4h"
SECONDARY_TF = "1h"
EXECUTION_TF = "15m"
MICRO_TF = "5m"
COINBASE_PRODUCTS = {"BTCUSD": "BTC-USD", "ETHUSD": "ETH-USD"}
SIGNAL_DIRECTION_RULES = {
    "Fed policy": {"DXY": 1.4, "EURUSD": -1.4, "USDJPY": 1.2, "GOLD": -0.9, "US10Y": 1.3, "SPX": -0.8, "BTCUSD": -0.5, "ETHUSD": -0.5},
    "Inflation": {"DXY": 1.1, "EURUSD": -1.0, "USDJPY": 0.8, "GOLD": -0.4, "US10Y": 1.2, "SPX": -0.9, "BTCUSD": -0.3, "ETHUSD": -0.3},
    "Labor": {"DXY": 0.7, "EURUSD": -0.7, "USDJPY": 0.6, "US10Y": 0.7, "SPX": -0.2},
    "Growth": {"DXY": 0.2, "EURUSD": -0.2, "WTI": 0.9, "SPX": 0.9, "BTCUSD": 0.6, "ETHUSD": 0.6, "GOLD": -0.2},
    "Treasury / fiscal": {"DXY": 0.7, "EURUSD": -0.6, "USDJPY": 0.4, "GOLD": 0.3, "US10Y": 0.8, "SPX": -0.4, "WTI": -0.1},
    "FX / rates": {"DXY": 1.1, "EURUSD": -1.1, "USDJPY": 0.9, "GOLD": -0.4, "US10Y": 0.9},
    "Risk sentiment": {"DXY": 0.7, "EURUSD": -0.3, "USDJPY": -0.7, "GOLD": 1.0, "WTI": -0.5, "SPX": -1.0, "BTCUSD": -1.0, "ETHUSD": -1.0},
}
CROSS_ASSET_EXPOSURES = {
    "DXY": {"US10Y": 0.9, "SPX": -0.45, "GOLD": -0.55, "BTCUSD": -0.2, "WTI": -0.1},
    "EURUSD": {"DXY": -1.0, "US10Y": -0.35, "SPX": 0.1, "GOLD": 0.16},
    "USDJPY": {"DXY": 0.6, "US10Y": 0.8, "SPX": 0.2, "GOLD": -0.18},
    "GBPUSD": {"DXY": -0.92, "US10Y": -0.26, "SPX": 0.08, "GOLD": 0.08},
    "GOLD": {"DXY": -0.72, "US10Y": -0.64, "SPX": -0.15, "BTCUSD": 0.14, "WTI": 0.08},
    "WTI": {"DXY": -0.25, "SPX": 0.72, "GOLD": -0.05},
    "US10Y": {"DXY": 0.72, "SPX": -0.32, "GOLD": -0.44},
    "SPX": {"DXY": -0.55, "US10Y": -0.42, "BTCUSD": 0.26, "WTI": 0.28},
    "BTCUSD": {"DXY": -0.45, "US10Y": -0.35, "SPX": 0.38, "ETHUSD": 0.45},
    "ETHUSD": {"DXY": -0.48, "US10Y": -0.32, "SPX": 0.34, "BTCUSD": 0.5},
}
ANALYSIS_FORMULAS = [
    AnalysisFormula(name="Log Return", formula="r_t = ln(C_t / C_{t-1})", description="Base return transform used for slope, volatility, and signal normalization."),
    AnalysisFormula(name="EMA", formula="EMA_t(n) = a*C_t + (1-a)*EMA_{t-1}(n), a = 2/(n+1)", description="Fast/slow EMA spread measures persistent trend pressure."),
    AnalysisFormula(name="EWMA Vol", formula="sigma_t^2 = lambda*sigma_{t-1}^2 + (1-lambda)*r_t^2", description="Real-time volatility estimate used for sizing, stop distance, and confidence."),
    AnalysisFormula(name="OLS Trend", formula="beta = Cov(x, ln C) / Var(x), t_beta = beta / SE(beta)", description="Regression slope t-stat quantifies directional persistence on each timeframe."),
    AnalysisFormula(name="ADX", formula="ADX = EMA(DX), DX = 100*|DI+ - DI-|/(DI+ + DI-)", description="Directional strength filter for Dow markup/markdown confirmation."),
    AnalysisFormula(name="ATR", formula="ATR = EMA(max(H-L, |H-C_prev|, |L-C_prev|))", description="Volatility-based stop, target, and risk budget anchor."),
    AnalysisFormula(name="Microstructure", formula="M = w1*CLV + w2*Wick + w3*RangeExp + w4*VolZ + w5*Imbalance - w6*Spread", description="Short-horizon tape pressure using bar proxies and order-book data when available."),
    AnalysisFormula(name="Ensemble", formula="S = sum_i w_i f_i, E[r_h] = tanh(S/k) * Move_h", description="Multi-factor forecast score mapped into horizon-specific expected return."),
]


@dataclass(frozen=True, slots=True)
class OrderBookStats:
    spread_bps: float
    imbalance: float
    liquidity_state: str


@dataclass(frozen=True, slots=True)
class TfMetrics:
    trend_score: float
    momentum_score: float
    reversion_score: float
    volatility_score: float
    micro_score: float
    slope_tstat: float
    ema_spread_pct: float
    adx: float
    plus_di: float
    minus_di: float
    support: float | None
    resistance: float | None
    atr_pct: float
    realized_vol: float
    zscore: float
    close_location: float
    volume_zscore: float | None
    range_expansion: float


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def average(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    return pstdev(values)


def close_location(bar: PriceBar) -> float:
    spread = bar.high - bar.low
    if spread <= 0:
        return 0.0
    return ((bar.close - bar.low) - (bar.high - bar.close)) / spread


def wick_skew(bar: PriceBar) -> float:
    spread = bar.high - bar.low
    if spread <= 0:
        return 0.0
    upper = bar.high - max(bar.open, bar.close)
    lower = min(bar.open, bar.close) - bar.low
    return (lower - upper) / spread


def range_expansion(bars: list[PriceBar]) -> float:
    if len(bars) < 6:
        return 1.0
    ranges = [abs(bar.high - bar.low) / bar.close * 100 if bar.close else 0.0 for bar in bars]
    recent = average(ranges[-3:])
    baseline = average(ranges[-12:-3] or ranges[:-3] or [recent])
    if baseline == 0:
        return 1.0
    return recent / baseline


def volume_zscore(bars: list[PriceBar], *, window: int = 18) -> float | None:
    sample = [bar.volume for bar in bars[-window:] if bar.volume is not None]
    if len(sample) < 6:
        return None
    baseline = sample[:-1]
    sigma = stddev(baseline)
    if sigma == 0:
        return 0.0
    return (sample[-1] - average(baseline)) / sigma


def pct_returns(bars: list[PriceBar]) -> list[float]:
    values: list[float] = []
    for previous, current in zip(bars, bars[1:], strict=False):
        if previous.close == 0:
            continue
        values.append((current.close - previous.close) / previous.close * 100)
    return values


def realized_vol_pct(bars: list[PriceBar], *, window: int = 36) -> float:
    returns = pct_returns(bars[-(window + 1):] if len(bars) > window else bars)
    if len(returns) < 3:
        return 0.0
    return pstdev(returns) * sqrt(max(6, min(24, len(returns))))


def true_range_pct(bar: PriceBar, previous_close: float | None) -> float:
    if bar.close == 0:
        return 0.0
    points = [bar.high - bar.low]
    if previous_close is not None:
        points.append(abs(bar.high - previous_close))
        points.append(abs(bar.low - previous_close))
    return max(points) / bar.close * 100


def atr_pct(bars: list[PriceBar], *, window: int = 14) -> float:
    sample = bars[-window:] if len(bars) > window else bars
    if not sample:
        return 0.0
    ranges = []
    previous_close: float | None = None
    for bar in sample:
        ranges.append(true_range_pct(bar, previous_close))
        previous_close = bar.close
    return average(ranges)


def ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    alpha = 2 / (length + 1)
    current = values[0]
    for value in values[1:]:
        current = alpha * value + (1 - alpha) * current
    return current


def zscore_last(values: list[float], *, window: int = 20) -> float:
    if len(values) < 4:
        return 0.0
    sample = values[-window:] if len(values) > window else values
    sigma = stddev(sample[:-1] or sample)
    if sigma == 0:
        return 0.0
    baseline = average(sample[:-1] or sample)
    return (sample[-1] - baseline) / sigma


def regression_tstat(values: list[float], *, window: int = 24) -> float:
    sample = values[-window:] if len(values) > window else values
    n = len(sample)
    if n < 6:
        return 0.0
    logs = [log(max(value, 1e-12)) for value in sample]
    x_values = list(range(n))
    x_bar = average([float(x) for x in x_values])
    y_bar = average(logs)
    sxx = sum((x - x_bar) ** 2 for x in x_values)
    if sxx == 0:
        return 0.0
    sxy = sum((x - x_bar) * (y - y_bar) for x, y in zip(x_values, logs, strict=True))
    beta = sxy / sxx
    residuals = [y - (y_bar + beta * (x - x_bar)) for x, y in zip(x_values, logs, strict=True)]
    if n <= 2:
        return 0.0
    s2 = sum(residual ** 2 for residual in residuals) / max(1, n - 2)
    se_beta = sqrt(s2 / sxx) if sxx > 0 else 0.0
    if se_beta == 0:
        return 0.0
    return beta / se_beta


def directional_movement_adx(bars: list[PriceBar], *, period: int = 14) -> tuple[float, float, float]:
    if len(bars) < period + 2:
        return 0.0, 0.0, 0.0

    tr_values: list[float] = []
    plus_dm_values: list[float] = []
    minus_dm_values: list[float] = []
    previous = bars[0]
    for current in bars[1:]:
        up_move = current.high - previous.high
        down_move = previous.low - current.low
        plus_dm = up_move if up_move > down_move and up_move > 0 else 0.0
        minus_dm = down_move if down_move > up_move and down_move > 0 else 0.0
        tr = max(current.high - current.low, abs(current.high - previous.close), abs(current.low - previous.close))
        tr_values.append(tr)
        plus_dm_values.append(plus_dm)
        minus_dm_values.append(minus_dm)
        previous = current

    tr_smooth = sum(tr_values[:period])
    plus_smooth = sum(plus_dm_values[:period])
    minus_smooth = sum(minus_dm_values[:period])
    dx_values: list[float] = []

    for index in range(period, len(tr_values)):
        if tr_smooth <= 0:
            plus_di = 0.0
            minus_di = 0.0
        else:
            plus_di = 100 * plus_smooth / tr_smooth
            minus_di = 100 * minus_smooth / tr_smooth
        denominator = plus_di + minus_di
        dx = 0.0 if denominator == 0 else 100 * abs(plus_di - minus_di) / denominator
        dx_values.append(dx)

        tr_smooth = tr_smooth - (tr_smooth / period) + tr_values[index]
        plus_smooth = plus_smooth - (plus_smooth / period) + plus_dm_values[index]
        minus_smooth = minus_smooth - (minus_smooth / period) + minus_dm_values[index]

    if tr_smooth <= 0:
        return 0.0, 0.0, 0.0
    plus_di = 100 * plus_smooth / tr_smooth
    minus_di = 100 * minus_smooth / tr_smooth
    adx = average(dx_values[-period:]) if dx_values else 0.0
    return adx, plus_di, minus_di


def resample_bars(bars: list[PriceBar], timeframe: str) -> list[PriceBar]:
    minutes = TIMEFRAME_MINUTES[timeframe]
    if minutes == 1 and timeframe == "1m":
        return bars[-TIMEFRAME_LIMITS[timeframe]:]

    buckets: dict[int, list[PriceBar]] = {}
    for bar in bars:
        stamp = int(bar.time.timestamp())
        bucket = stamp - (stamp % (minutes * 60))
        buckets.setdefault(bucket, []).append(bar)

    output: list[PriceBar] = []
    for bucket in sorted(buckets):
        group = buckets[bucket]
        output.append(
            PriceBar(
                time=datetime.fromtimestamp(bucket, tz=UTC),
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
                volume=sum(item.volume or 0.0 for item in group) if any(item.volume is not None for item in group) else None,
            )
        )
    return output[-TIMEFRAME_LIMITS[timeframe]:]


class AssetAnalysisService:
    def __init__(
        self,
        settings: Settings,
        hub: NewsHub,
        market_data: MarketDataService,
        calendar_data: EconomicCalendarService,
        speech_data: SpeechTapeService,
    ) -> None:
        self.settings = settings
        self.hub = hub
        self.market_data = market_data
        self.calendar_data = calendar_data
        self.speech_data = speech_data
        self._lock = asyncio.Lock()
        self._cache: dict[str, AssetAnalysisSnapshot] = {}
        self._last_trade_state: dict[str, dict[str, str | int]] = {}

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def snapshot(self, asset: str, *, force: bool = False) -> AssetAnalysisSnapshot:
        asset_key = asset.strip().upper()
        if asset_key not in TRACKED_BY_LABEL:
            raise KeyError(f"Unsupported asset: {asset}")

        async with self._lock:
            now = datetime.now(UTC)
            cached = self._cache.get(asset_key)
            if (
                not force
                and cached is not None
                and now - cached.updated_at < timedelta(seconds=12)
            ):
                return cached

            market, calendar, speeches, news_items = await asyncio.gather(
                self.market_data.snapshot(force=force),
                self.calendar_data.snapshot(force=False),
                self.speech_data.snapshot(force=False),
                self.hub.snapshot(),
            )
            quote = next((item for item in market.quotes if item.label == asset_key), None)
            config = self.market_data.resolve_asset(asset_key)
            timeframe_bars = await self._fetch_timeframe_bars(config)

            news_score, news_drivers = self._score_news(asset_key, news_items, speeches.items)
            cross_score, cross_drivers = self._score_cross_asset(asset_key, market.quotes)
            event_intensity, event_drivers = self._event_risk(now, asset_key, calendar.schedule)
            order_book = await self._fetch_coinbase_order_book(asset_key)

            timeframe_states = [
                self._analyze_timeframe(
                    asset=config,
                    timeframe=timeframe,
                    bars=timeframe_bars.get(timeframe, []),
                    quote=quote,
                    news_score=news_score,
                    cross_score=cross_score,
                    event_intensity=event_intensity,
                    order_book=order_book if timeframe in {"1m", "3m", "5m"} else None,
                )
                for timeframe in ANALYSIS_TIMEFRAMES
            ]

            analysis = self._build_snapshot(
                now=now,
                asset=config,
                quote=quote,
                timeframe_states=timeframe_states,
                news_drivers=news_drivers,
                cross_drivers=cross_drivers,
                event_drivers=event_drivers,
                event_intensity=event_intensity,
            )
            self._cache[asset_key] = analysis
            return analysis

    async def _fetch_timeframe_bars(self, asset: TrackedAsset) -> dict[str, list[PriceBar]]:
        fine, coarse = await asyncio.gather(
            self._fetch_fine_bars(asset),
            self._fetch_coarse_bars(asset),
        )
        return {
            "1m": resample_bars(fine, "1m"),
            "3m": resample_bars(fine, "3m"),
            "5m": resample_bars(fine, "5m"),
            "15m": resample_bars(fine, "15m"),
            "30m": resample_bars(fine, "30m"),
            "45m": resample_bars(fine, "45m"),
            "1h": resample_bars(coarse, "1h"),
            "2h": resample_bars(coarse, "2h"),
            "4h": resample_bars(coarse, "4h"),
        }

    async def _fetch_fine_bars(self, asset: TrackedAsset) -> list[PriceBar]:
        try:
            if asset.label in COINBASE_PRODUCTS:
                return await self._fetch_coinbase_window(asset, granularity=60, hours=8)
            payload = await self.market_data._fetch_yahoo_chart_payload(asset.chart_id, interval="1m", range_="5d")
        except Exception:
            return []
        return self._parse_yahoo_bars(asset, payload)[-480:]

    async def _fetch_coarse_bars(self, asset: TrackedAsset) -> list[PriceBar]:
        try:
            if asset.label in COINBASE_PRODUCTS:
                return await self._fetch_coinbase_window(asset, granularity=3600, hours=24 * 30)
            payload = await self.market_data._fetch_yahoo_chart_payload(asset.chart_id, interval="60m", range_="1mo")
        except Exception:
            return []
        return self._parse_yahoo_bars(asset, payload)

    def _parse_yahoo_bars(self, asset: TrackedAsset, payload: dict) -> list[PriceBar]:
        result = ((payload.get("chart") or {}).get("result") or [None])[0]
        if result is None:
            return []
        timestamps = result.get("timestamp") or []
        indicators = (((result.get("indicators") or {}).get("quote")) or [{}])[0]
        opens = indicators.get("open") or []
        highs = indicators.get("high") or []
        lows = indicators.get("low") or []
        closes = indicators.get("close") or []
        volumes = indicators.get("volume") or []

        output: list[PriceBar] = []
        digits = quote_digits(asset)
        for index, timestamp in enumerate(timestamps):
            close = parse_numeric(closes[index] if index < len(closes) else None)
            if close is None:
                continue
            open_value = parse_numeric(opens[index] if index < len(opens) else close) or close
            high_value = parse_numeric(highs[index] if index < len(highs) else close) or close
            low_value = parse_numeric(lows[index] if index < len(lows) else close) or close
            volume = parse_numeric(volumes[index] if index < len(volumes) else None)
            output.append(
                PriceBar(
                    time=datetime.fromtimestamp(int(timestamp), tz=UTC),
                    open=round(float(open_value), digits),
                    high=round(float(high_value), digits),
                    low=round(float(low_value), digits),
                    close=round(float(close), digits),
                    volume=volume,
                )
            )
        return output

    async def _fetch_coinbase_window(
        self,
        asset: TrackedAsset,
        *,
        granularity: int,
        hours: int,
    ) -> list[PriceBar]:
        client = self.market_data._client
        product_id = COINBASE_PRODUCTS.get(asset.label)
        if client is None or product_id is None:
            return []

        now = datetime.now(UTC)
        end = now
        step_hours = max(1, int((granularity * 280) / 3600))
        chunks: list[PriceBar] = []

        while (now - end).total_seconds() < hours * 3600:
            start = max(now - timedelta(hours=hours), end - timedelta(hours=step_hours))
            try:
                response = await client.get(
                    f"https://api.exchange.coinbase.com/products/{product_id}/candles",
                    params={
                        "granularity": granularity,
                        "start": start.isoformat().replace("+00:00", "Z"),
                        "end": end.isoformat().replace("+00:00", "Z"),
                    },
                )
                response.raise_for_status()
                payload = response.json()
            except Exception:
                break
            digits = quote_digits(asset)
            chunk = [
                PriceBar(
                    time=datetime.fromtimestamp(int(timestamp), tz=UTC),
                    low=round(float(low), digits),
                    high=round(float(high), digits),
                    open=round(float(open_value), digits),
                    close=round(float(close), digits),
                    volume=float(volume),
                )
                for timestamp, low, high, open_value, close, volume in sorted(payload, key=lambda row: row[0])
            ]
            chunks.extend(chunk)
            end = start
            if start <= now - timedelta(hours=hours):
                break

        deduped: dict[int, PriceBar] = {int(bar.time.timestamp()): bar for bar in chunks}
        return [deduped[key] for key in sorted(deduped)]

    async def _fetch_coinbase_order_book(self, asset_key: str) -> OrderBookStats | None:
        product_id = COINBASE_PRODUCTS.get(asset_key)
        client = self.market_data._client
        if product_id is None or client is None:
            return None
        try:
            response = await client.get(
                f"https://api.exchange.coinbase.com/products/{product_id}/book",
                params={"level": 2},
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            return None
        bids = payload.get("bids") or []
        asks = payload.get("asks") or []
        if not bids or not asks:
            return None
        top_bids = [(float(price), float(size)) for price, size, *_ in bids[:10]]
        top_asks = [(float(price), float(size)) for price, size, *_ in asks[:10]]
        best_bid = top_bids[0][0]
        best_ask = top_asks[0][0]
        mid = (best_bid + best_ask) / 2
        if mid <= 0:
            return None
        spread_bps = (best_ask - best_bid) / mid * 10000
        bid_depth = sum(size for _, size in top_bids)
        ask_depth = sum(size for _, size in top_asks)
        total_depth = bid_depth + ask_depth
        imbalance = 0.0 if total_depth == 0 else (bid_depth - ask_depth) / total_depth
        liquidity_state = "stressed" if spread_bps >= 12 else "thin" if spread_bps >= 6 else "normal"
        return OrderBookStats(
            spread_bps=round(spread_bps, 3),
            imbalance=round(imbalance, 3),
            liquidity_state=liquidity_state,
        )

    def _score_news(
        self,
        asset_key: str,
        news_items: list[NewsItem],
        speeches: list[SpeechTapeItem],
    ) -> tuple[float, list[str]]:
        now = datetime.now(UTC)
        total = 0.0
        themes: dict[str, int] = {}

        for item in news_items[:48]:
            age_hours = max(0.0, (now - item.published_at).total_seconds() / 3600)
            decay = max(0.12, 1 - age_hours / 96)
            asset_score = sum(SIGNAL_DIRECTION_RULES.get(signal, {}).get(asset_key, 0.0) for signal in item.matched_signals)
            if asset_score == 0:
                continue
            weight = (0.52 + item.impact_score / 120) * decay
            total += asset_score * weight
            for signal in item.matched_signals:
                themes[signal] = themes.get(signal, 0) + 1

        for item in speeches[:10]:
            age_days = max(0.0, (now - item.published_at).total_seconds() / 86400)
            decay = max(0.18, 1 - age_days / 28)
            asset_score = sum(SIGNAL_DIRECTION_RULES.get(signal, {}).get(asset_key, 0.0) for signal in item.matched_signals)
            if asset_score == 0:
                continue
            total += asset_score * decay * 0.72
            for signal in item.matched_signals:
                themes[signal] = themes.get(signal, 0) + 1

        drivers = [f"Macro {signal}" for signal, _ in sorted(themes.items(), key=lambda entry: entry[1], reverse=True)[:3]]
        return clamp(total, -4.2, 4.2), drivers

    def _score_cross_asset(self, asset_key: str, quotes: list[AssetQuote]) -> tuple[float, list[str]]:
        exposures = CROSS_ASSET_EXPOSURES.get(asset_key, {})
        if not exposures:
            return 0.0, []
        by_asset = {quote.label: quote for quote in quotes}
        total = 0.0
        drivers: list[str] = []
        for other, beta in exposures.items():
            quote = by_asset.get(other)
            if quote is None or quote.percent_change is None:
                continue
            contribution = beta * quote.percent_change
            total += contribution
            if abs(contribution) >= 0.18:
                drivers.append(f"{other} {quote.percent_change:+.2f}%")
        return clamp(total * 0.28, -3.5, 3.5), drivers[:3]

    def _event_risk(
        self,
        now: datetime,
        asset_key: str,
        schedule: list[ScheduledEvent],
    ) -> tuple[float, list[str]]:
        total = 0.0
        drivers: list[str] = []
        for event in schedule:
            if event.scheduled_at is None:
                continue
            hours_ahead = (event.scheduled_at - now).total_seconds() / 3600
            if hours_ahead < -6 or hours_ahead > 24 * 5:
                continue
            relevance = sum(abs(SIGNAL_DIRECTION_RULES.get(signal, {}).get(asset_key, 0.0)) for signal in event.signals)
            if relevance == 0:
                continue
            horizon = 1.25 if hours_ahead <= 24 else 0.8 if hours_ahead <= 72 else 0.45
            importance = {"high": 1.2, "medium": 0.7, "watch": 0.45}.get(event.importance, 0.45)
            total += relevance * horizon * importance
            eta = "today" if hours_ahead <= 24 else f"{int(hours_ahead // 24)}d"
            drivers.append(f"{event.title} {eta}")
        return clamp(total * 0.3, 0.0, 4.5), drivers[:3]

    def _compute_metrics(
        self,
        bars: list[PriceBar],
        order_book: OrderBookStats | None,
    ) -> TfMetrics:
        if len(bars) < 12:
            return TfMetrics(
                trend_score=0.0,
                momentum_score=0.0,
                reversion_score=0.0,
                volatility_score=0.0,
                micro_score=0.0,
                slope_tstat=0.0,
                ema_spread_pct=0.0,
                adx=0.0,
                plus_di=0.0,
                minus_di=0.0,
                support=None,
                resistance=None,
                atr_pct=0.0,
                realized_vol=0.0,
                zscore=0.0,
                close_location=0.0,
                volume_zscore=None,
                range_expansion=1.0,
            )

        closes = [bar.close for bar in bars]
        highs = [bar.high for bar in bars]
        lows = [bar.low for bar in bars]
        ema_fast = ema(closes[-min(12, len(closes)):], min(8, max(2, len(closes) // 6)))
        ema_slow = ema(closes[-min(34, len(closes)):], min(21, max(5, len(closes) // 3)))
        atr_value = max(atr_pct(bars, window=14), 0.08)
        slope_t = regression_tstat(closes, window=24)
        ema_spread_pct = 0.0 if ema_slow == 0 else (ema_fast - ema_slow) / ema_slow * 100
        adx, plus_di, minus_di = directional_movement_adx(bars, period=14)
        realized = realized_vol_pct(bars, window=36)
        z_value = zscore_last(closes, window=20)
        volume_surprise = volume_zscore(bars, window=18)
        range_mult = range_expansion(bars)
        clv = close_location(bars[-1])
        micro = clv * 1.2 + wick_skew(bars[-1]) * 0.8 + (0.3 * volume_surprise if volume_surprise is not None else 0.0)
        if order_book is not None:
            micro += order_book.imbalance * 1.5 - order_book.spread_bps * 0.015

        trend = clamp(slope_t * 0.85 + (ema_spread_pct / atr_value) * 0.6 + ((plus_di - minus_di) / 25), -5, 5)
        momentum = clamp(trend * 0.65 + (pct_returns(bars[-4:])[-1] if len(bars) >= 4 and pct_returns(bars[-4:]) else 0.0) / atr_value * 0.4 + micro * 0.35, -5, 5)
        reversion = clamp(-z_value * (1.1 if adx < 18 else 0.6) - (range_mult - 1) * 0.4 * (1 if closes[-1] >= closes[-2] else -1), -4.5, 4.5)
        volatility = clamp(realized + atr_value * 0.7 + max(0.0, range_mult - 1), 0, 6)
        support = min(lows[-20:]) if len(lows) >= 5 else min(lows)
        resistance = max(highs[-20:]) if len(highs) >= 5 else max(highs)

        return TfMetrics(
            trend_score=round(trend, 3),
            momentum_score=round(momentum, 3),
            reversion_score=round(reversion, 3),
            volatility_score=round(volatility, 3),
            micro_score=round(clamp(micro, -4.5, 4.5), 3),
            slope_tstat=round(slope_t, 3),
            ema_spread_pct=round(ema_spread_pct, 3),
            adx=round(adx, 3),
            plus_di=round(plus_di, 3),
            minus_di=round(minus_di, 3),
            support=round(support, 4) if support is not None else None,
            resistance=round(resistance, 4) if resistance is not None else None,
            atr_pct=round(atr_value, 3),
            realized_vol=round(realized, 3),
            zscore=round(z_value, 3),
            close_location=round(clv, 3),
            volume_zscore=None if volume_surprise is None else round(volume_surprise, 3),
            range_expansion=round(range_mult, 3),
        )

    def _dow_phase(self, metrics: TfMetrics) -> str:
        if metrics.trend_score >= 1.0 and metrics.adx >= 18 and metrics.plus_di >= metrics.minus_di:
            return "markup"
        if metrics.trend_score <= -1.0 and metrics.adx >= 18 and metrics.minus_di >= metrics.plus_di:
            return "markdown"
        if abs(metrics.trend_score) < 0.9 and metrics.zscore <= -0.5 and metrics.micro_score > 0:
            return "accumulation"
        if abs(metrics.trend_score) < 0.9 and metrics.zscore >= 0.5 and metrics.micro_score < 0:
            return "distribution"
        return "transition"

    def _analyze_timeframe(
        self,
        *,
        asset: TrackedAsset,
        timeframe: str,
        bars: list[PriceBar],
        quote: AssetQuote | None,
        news_score: float,
        cross_score: float,
        event_intensity: float,
        order_book: OrderBookStats | None,
    ) -> AnalysisTimeframeState:
        metrics = self._compute_metrics(bars, order_book)
        phase = self._dow_phase(metrics)
        macro_weight = TF_MACRO_WEIGHTS[timeframe]
        total_score = (
            metrics.trend_score * 0.38
            + metrics.momentum_score * 0.26
            - metrics.reversion_score * 0.16
            + metrics.micro_score * 0.12
            + news_score * macro_weight
            + cross_score * 0.11
            - event_intensity * 0.08
        )
        total_score = tanh(total_score / 3.2) * 4.0
        expected_move = max(0.12, metrics.atr_pct * 0.9 + metrics.realized_vol * 0.5)
        bias = "bullish" if total_score >= 0.65 else "bearish" if total_score <= -0.65 else "neutral"

        if bias == "bullish" and phase in {"accumulation", "markup", "transition"}:
            signal = "long"
        elif bias == "bearish" and phase in {"distribution", "markdown", "transition"}:
            signal = "short"
        else:
            signal = "avoid"

        if signal == "long" and metrics.micro_score >= -0.4 and metrics.trend_score >= -0.2:
            hold_state = "hold-long"
        elif signal == "short" and metrics.micro_score <= 0.4 and metrics.trend_score <= 0.2:
            hold_state = "hold-short"
        else:
            hold_state = "do-not-hold"

        last_price = quote.last if quote is not None else (bars[-1].close if bars else None)
        stop_level = None
        take_profit_level = None
        if last_price is not None:
            if signal == "long":
                stop_level = round(last_price * (1 - max(expected_move * 0.7, 0.18) / 100), 4)
                take_profit_level = round(last_price * (1 + max(expected_move * 1.45, 0.35) / 100), 4)
            elif signal == "short":
                stop_level = round(last_price * (1 + max(expected_move * 0.7, 0.18) / 100), 4)
                take_profit_level = round(last_price * (1 - max(expected_move * 1.45, 0.35) / 100), 4)

        confidence = int(clamp(42 + abs(total_score) * 10 + metrics.adx * 0.45 + len(bars) / 18 - event_intensity * 2.5, 30, 94))
        summary = (
            f"{timeframe} {phase} with trend {metrics.trend_score:+.2f}, "
            f"momentum {metrics.momentum_score:+.2f}, micro {metrics.micro_score:+.2f}, "
            f"ADX {metrics.adx:.1f}."
        )

        return AnalysisTimeframeState(
            timeframe=timeframe,
            bars_used=len(bars),
            bias=bias,
            signal=signal,
            hold_state=hold_state,
            dow_phase=phase,
            confidence=confidence,
            trend_score=round(metrics.trend_score, 2),
            momentum_score=round(metrics.momentum_score, 2),
            reversion_score=round(metrics.reversion_score, 2),
            volatility_score=round(metrics.volatility_score, 2),
            microstructure_score=round(metrics.micro_score, 2),
            adx=round(metrics.adx, 2),
            slope_tstat=round(metrics.slope_tstat, 2),
            ema_spread_pct=round(metrics.ema_spread_pct, 2),
            support=metrics.support,
            resistance=metrics.resistance,
            stop_level=stop_level,
            take_profit_level=take_profit_level,
            summary=summary,
        )

    def _build_snapshot(
        self,
        *,
        now: datetime,
        asset: TrackedAsset,
        quote: AssetQuote | None,
        timeframe_states: list[AnalysisTimeframeState],
        news_drivers: list[str],
        cross_drivers: list[str],
        event_drivers: list[str],
        event_intensity: float,
    ) -> AssetAnalysisSnapshot:
        state_map = {state.timeframe: state for state in timeframe_states}
        primary = state_map[PRIMARY_TF]
        secondary = state_map[SECONDARY_TF]
        execution = state_map[EXECUTION_TF]
        micro = state_map[MICRO_TF]

        long_alignment = sum(1 for state in (primary, secondary, execution, micro) if state.signal == "long")
        short_alignment = sum(1 for state in (primary, secondary, execution, micro) if state.signal == "short")
        if long_alignment >= 3 and primary.bias == "bullish" and secondary.bias != "bearish":
            aggregate_signal = "long"
        elif short_alignment >= 3 and primary.bias == "bearish" and secondary.bias != "bullish":
            aggregate_signal = "short"
        else:
            aggregate_signal = "avoid"

        risk_state = "event-lock" if event_intensity >= 3.2 else "high-vol" if max(state.volatility_score for state in timeframe_states) >= 3 else "normal"
        hold_decision = self._hold_decision(aggregate_signal, primary, secondary, execution, micro, risk_state)
        trade_plan = self._build_trade_plan(asset, quote, aggregate_signal, hold_decision, primary, secondary, execution, micro, risk_state)
        update_note = self._trade_update_note(asset.label, trade_plan)
        trade_plan = trade_plan.model_copy(update={"update_note": update_note})

        drivers = self._merge_unique([
            f"Dow primary: {primary.dow_phase}",
            f"Dow secondary: {secondary.dow_phase}",
            *news_drivers,
            *cross_drivers,
            *event_drivers,
        ])
        model_summary = (
            f"{asset.label} uses {PRIMARY_TF}>{SECONDARY_TF}>{EXECUTION_TF} stack with full "
            f"{'/'.join(ANALYSIS_TIMEFRAMES)} monitoring, Dow phase detection, OLS slope t-stat, ADX, ATR, "
            "EWMA-style vol normalization, and microstructure pressure updates."
        )
        confidence = int(round(average([primary.confidence, secondary.confidence, execution.confidence])))

        return AssetAnalysisSnapshot(
            updated_at=now,
            source="Realtime multi-timeframe asset analysis engine",
            asset=asset.label,
            label=asset.label,
            group=asset.group,
            spot=quote.last if quote is not None else None,
            primary_timeframe=PRIMARY_TF,
            secondary_timeframe=SECONDARY_TF,
            execution_timeframe=EXECUTION_TF,
            aggregate_signal=aggregate_signal,
            hold_decision=hold_decision,
            dow_phase=primary.dow_phase,
            confidence=confidence,
            risk_state=risk_state,
            model_summary=model_summary,
            update_note=update_note,
            drivers=drivers[:7],
            timeframes=timeframe_states,
            trade_plan=trade_plan,
            formulas=ANALYSIS_FORMULAS,
        )

    def _hold_decision(
        self,
        aggregate_signal: str,
        primary: AnalysisTimeframeState,
        secondary: AnalysisTimeframeState,
        execution: AnalysisTimeframeState,
        micro: AnalysisTimeframeState,
        risk_state: str,
    ) -> str:
        if aggregate_signal == "long":
            if risk_state == "event-lock":
                return "Do not hold full size into event risk"
            if primary.signal == "long" and secondary.signal == "long" and execution.hold_state == "hold-long":
                return "Hold long while 1h/4h remain aligned"
            return "Long thesis weakened; do not hold aggressively"
        if aggregate_signal == "short":
            if risk_state == "event-lock":
                return "Do not hold full short into event risk"
            if primary.signal == "short" and secondary.signal == "short" and execution.hold_state == "hold-short":
                return "Hold short while 1h/4h remain aligned"
            return "Short thesis weakened; avoid holding size"
        if micro.signal in {"long", "short"} and primary.signal == "avoid":
            return "Do not hold; lower timeframe noise is not confirmed by primary trend"
        return "Stand aside until primary and execution timeframes align"

    def _build_trade_plan(
        self,
        asset: TrackedAsset,
        quote: AssetQuote | None,
        aggregate_signal: str,
        hold_decision: str,
        primary: AnalysisTimeframeState,
        secondary: AnalysisTimeframeState,
        execution: AnalysisTimeframeState,
        micro: AnalysisTimeframeState,
        risk_state: str,
    ) -> AssetTradePlan:
        spot = quote.last if quote is not None else None
        entry_low = None
        entry_high = None
        stop_level = None
        take_profit = None

        if spot is not None:
            if aggregate_signal == "long":
                entry_low = round(max(spot * 0.997, execution.support or spot * 0.995), 4)
                entry_high = round(min(spot * 1.0015, execution.resistance or spot * 1.003), 4)
                stop_level = execution.stop_level or primary.stop_level
                take_profit = execution.take_profit_level or primary.take_profit_level
            elif aggregate_signal == "short":
                entry_low = round(max((execution.support or spot * 0.997), spot * 0.9985), 4)
                entry_high = round(min((execution.resistance or spot * 1.003), spot * 1.001), 4)
                stop_level = execution.stop_level or primary.stop_level
                take_profit = execution.take_profit_level or primary.take_profit_level

        status = "standby"
        if aggregate_signal in {"long", "short"}:
            previous = self._last_trade_state.get(asset.label)
            if previous and previous.get("action") == aggregate_signal:
                status = "hold"
            elif previous and previous.get("action") in {"long", "short"} and aggregate_signal != previous.get("action"):
                status = "updated"
            else:
                status = "fresh"
        elif self._last_trade_state.get(asset.label, {}).get("action") in {"long", "short"}:
            status = "invalidated"

        thesis = (
            f"{PRIMARY_TF} {primary.dow_phase} / {SECONDARY_TF} {secondary.dow_phase} with "
            f"{EXECUTION_TF} signal {execution.signal}. Micro {MICRO_TF} is {micro.signal} and risk state is {risk_state}."
        )
        hold_if = [
            f"Hold only if {PRIMARY_TF} stays {primary.dow_phase} and {SECONDARY_TF} bias remains {secondary.bias}.",
            f"Keep holding while {EXECUTION_TF} close stays above stop/invalidation and {MICRO_TF} does not flip hard against the trade.",
            "Reduce size before high-impact macro events if risk_state moves to event-lock.",
        ]
        avoid_if = [
            f"Do not hold if {EXECUTION_TF} signal flips against {aggregate_signal or 'the thesis'}.",
            f"Do not hold if {PRIMARY_TF} Dow phase changes from {primary.dow_phase} to the opposite distribution/markdown side.",
            "Do not hold if event risk surges or the stop level is breached.",
        ]
        return AssetTradePlan(
            action=aggregate_signal,
            status=status,
            confidence=int(round(average([primary.confidence, secondary.confidence, execution.confidence]))),
            thesis=thesis,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            stop_level=stop_level,
            take_profit_level=take_profit,
            hold_if=hold_if,
            avoid_if=avoid_if,
            update_note=None,
        )

    def _trade_update_note(self, asset_key: str, trade_plan: AssetTradePlan) -> str | None:
        previous = self._last_trade_state.get(asset_key)
        self._last_trade_state[asset_key] = {
            "action": trade_plan.action,
            "status": trade_plan.status,
            "confidence": trade_plan.confidence,
        }
        if previous is None:
            return "Initial realtime plan generated."
        if previous.get("action") != trade_plan.action:
            return f"Trade plan updated: {previous.get('action', 'none')} -> {trade_plan.action}."
        if previous.get("status") != trade_plan.status:
            return f"Plan status changed: {previous.get('status', 'none')} -> {trade_plan.status}."
        if abs(int(previous.get("confidence", 0)) - trade_plan.confidence) >= 8:
            return f"Confidence repriced from {previous.get('confidence')} to {trade_plan.confidence}."
        return None

    def _merge_unique(self, values: list[str]) -> list[str]:
        output: list[str] = []
        seen: set[str] = set()
        for value in values:
            cleaned = value.strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            output.append(cleaned)
        return output
