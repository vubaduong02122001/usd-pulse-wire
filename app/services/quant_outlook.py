from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timedelta, timezone
from math import sqrt
from statistics import pstdev

from app.config import Settings
from app.models import (
    AssetQuote,
    NewsItem,
    PriceBar,
    QuantAssetOutlook,
    QuantRegime,
    QuantSnapshot,
    ScheduledEvent,
    SpeechTapeItem,
)
from app.services.calendar_data import EconomicCalendarService
from app.services.hub import NewsHub
from app.services.market_data import TRACKED_ASSETS, MarketDataService
from app.services.speech_data import SpeechTapeService


UTC = timezone.utc
ASSET_ORDER = [asset.label for asset in TRACKED_ASSETS]
SIGNAL_DIRECTION_RULES = {
    "Fed policy": {"DXY": 1.4, "EURUSD": -1.4, "USDJPY": 1.2, "GOLD": -0.9, "US10Y": 1.3, "SPX": -0.8, "BTCUSD": -0.5, "ETHUSD": -0.5},
    "Inflation": {"DXY": 1.1, "EURUSD": -1.0, "USDJPY": 0.8, "GOLD": -0.4, "US10Y": 1.2, "SPX": -0.9, "BTCUSD": -0.3, "ETHUSD": -0.3},
    "Labor": {"DXY": 0.7, "EURUSD": -0.7, "USDJPY": 0.6, "US10Y": 0.7, "SPX": -0.2},
    "Growth": {"DXY": 0.2, "EURUSD": -0.2, "WTI": 0.9, "SPX": 0.9, "BTCUSD": 0.6, "ETHUSD": 0.6, "GOLD": -0.2},
    "Treasury / fiscal": {"DXY": 0.7, "EURUSD": -0.6, "USDJPY": 0.4, "GOLD": 0.3, "US10Y": 0.8, "SPX": -0.4, "WTI": -0.1},
    "FX / rates": {"DXY": 1.1, "EURUSD": -1.1, "USDJPY": 0.9, "GOLD": -0.4, "US10Y": 0.9},
    "Risk sentiment": {"DXY": 0.7, "EURUSD": -0.3, "USDJPY": -0.7, "GOLD": 1.0, "WTI": -0.5, "SPX": -1.0, "BTCUSD": -1.0, "ETHUSD": -1.0},
}
VOL_THRESHOLDS = {
    "Dollar": (0.25, 0.7),
    "FX": (0.35, 0.9),
    "Rates": (0.45, 1.1),
    "Commodity": (0.8, 2.1),
    "Equity": (0.65, 1.6),
    "Crypto": (2.0, 4.8),
}
EVENT_IMPORTANCE_WEIGHTS = {"high": 1.2, "medium": 0.75, "watch": 0.45}


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def safe_pct_change(current: float | None, previous: float | None) -> float:
    if current in {None, 0} or previous in {None, 0}:
        return 0.0
    return (float(current) - float(previous)) / float(previous) * 100


def average(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def pct_returns(bars: list[PriceBar], *, window: int | None = None) -> list[float]:
    if len(bars) < 2:
        return []
    sliced = bars[-window:] if window else bars
    returns: list[float] = []
    for previous, current in zip(sliced, sliced[1:], strict=False):
        if previous.close == 0:
            continue
        returns.append((current.close - previous.close) / previous.close * 100)
    return returns


def realized_vol_pct(bars: list[PriceBar]) -> float:
    returns = pct_returns(bars, window=36)
    if len(returns) < 3:
        return 0.0
    annualizer = sqrt(max(6, min(24, len(returns))))
    return pstdev(returns) * annualizer


def period_return_pct(bars: list[PriceBar], *, lookback: int) -> float:
    if len(bars) < 2:
        return 0.0
    start_index = max(0, len(bars) - 1 - max(1, lookback))
    start = bars[start_index].close
    end = bars[-1].close
    if start == 0:
        return 0.0
    return (end - start) / start * 100


def moving_average_gap_pct(bars: list[PriceBar], fast: int, slow: int) -> float:
    if len(bars) < max(fast, slow):
        return 0.0
    fast_avg = average([bar.close for bar in bars[-fast:]])
    slow_avg = average([bar.close for bar in bars[-slow:]])
    if slow_avg == 0:
        return 0.0
    return (fast_avg - slow_avg) / slow_avg * 100


def bias_label(score: float) -> str:
    if score >= 1.15:
        return "bullish"
    if score <= -1.15:
        return "bearish"
    return "neutral"


def volatility_label(group: str, value: float) -> str:
    calm_threshold, stressed_threshold = VOL_THRESHOLDS.get(group, (0.5, 1.5))
    if value >= stressed_threshold:
        return "stressed"
    if value <= calm_threshold:
        return "compressed"
    return "normal"


def direction_to_text(score: float) -> str:
    if score > 0:
        return "up"
    if score < 0:
        return "down"
    return "flat"


class QuantOutlookService:
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
        self._snapshot: QuantSnapshot | None = None

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def snapshot(self, *, force: bool = False) -> QuantSnapshot:
        async with self._lock:
            now = datetime.now(UTC)
            if (
                not force
                and self._snapshot is not None
                and now - self._snapshot.updated_at < timedelta(seconds=60)
            ):
                return self._snapshot

            market, calendar, speeches, news_items = await asyncio.gather(
                self.market_data.snapshot(force=force),
                self.calendar_data.snapshot(force=False),
                self.speech_data.snapshot(force=False),
                self.hub.snapshot(),
            )

            quotes_by_asset = {quote.label: quote for quote in market.quotes}
            chart_tasks = []
            for asset in ASSET_ORDER:
                chart_tasks.append(self.market_data.chart(asset, timeframe="5D", force=force))
                chart_tasks.append(self.market_data.chart(asset, timeframe="1M", force=force))

            chart_results = await asyncio.gather(*chart_tasks, return_exceptions=True)
            five_day_by_asset: dict[str, list[PriceBar]] = {}
            one_month_by_asset: dict[str, list[PriceBar]] = {}
            for index, asset in enumerate(ASSET_ORDER):
                five_day = chart_results[index * 2]
                one_month = chart_results[index * 2 + 1]
                five_day_by_asset[asset] = [] if isinstance(five_day, Exception) else five_day.bars
                one_month_by_asset[asset] = [] if isinstance(one_month, Exception) else one_month.bars

            recent_news = [item for item in news_items if now - item.published_at <= timedelta(days=5)]
            recent_speeches = [item for item in speeches.items if now - item.published_at <= timedelta(days=45)]
            upcoming_events = [
                item
                for item in calendar.schedule
                if item.scheduled_at is not None and item.scheduled_at >= now - timedelta(hours=8)
            ]

            assets = [
                self._build_asset_outlook(
                    asset,
                    quotes_by_asset.get(asset),
                    five_day_by_asset.get(asset, []),
                    one_month_by_asset.get(asset, []),
                    recent_news,
                    recent_speeches,
                    upcoming_events,
                )
                for asset in ASSET_ORDER
            ]
            regime = self._build_regime(now, assets, recent_news, upcoming_events)

            self._snapshot = QuantSnapshot(
                updated_at=now,
                source="In-house quant overlay: trend + realized vol + news/event scoring",
                regime=regime,
                assets=assets,
            )
            return self._snapshot

    def _build_asset_outlook(
        self,
        asset: str,
        quote: AssetQuote | None,
        bars_5d: list[PriceBar],
        bars_1m: list[PriceBar],
        news_items: list[NewsItem],
        speeches: list[SpeechTapeItem],
        upcoming_events: list[ScheduledEvent],
    ) -> QuantAssetOutlook:
        spot = quote.last if quote is not None else (bars_5d[-1].close if bars_5d else None)
        group = quote.group if quote is not None else self.market_data.resolve_asset(asset).group
        label = quote.label if quote is not None else asset

        intraday_move = quote.percent_change if quote and quote.percent_change is not None else 0.0
        week_move = period_return_pct(bars_5d, lookback=min(24, len(bars_5d) - 1)) if len(bars_5d) > 1 else 0.0
        month_move = period_return_pct(bars_1m, lookback=min(18, len(bars_1m) - 1)) if len(bars_1m) > 1 else 0.0
        ma_gap = moving_average_gap_pct(bars_1m or bars_5d, fast=8, slow=20)

        trend_score = clamp(
            intraday_move * 0.55 + week_move * 0.85 + month_move * 0.28 + ma_gap * 6.0,
            -7.5,
            7.5,
        )
        news_score, news_drivers = self._score_news(asset, news_items, speeches)
        event_score, event_drivers = self._score_events(
            asset,
            upcoming_events,
            directional_anchor=trend_score * 0.6 + news_score * 0.8,
        )
        vol_pct = realized_vol_pct(bars_5d)
        vol_regime = volatility_label(group, vol_pct)

        score_1d = clamp(trend_score * 0.42 + news_score * 0.58 + event_score * 0.52, -8.5, 8.5)
        score_1w = clamp(trend_score * 0.82 + news_score * 0.34 + event_score * 0.26, -8.5, 8.5)

        bias_1d = bias_label(score_1d)
        bias_1w = bias_label(score_1w)
        event_boost = 1 + min(0.55, abs(event_score) * 0.08)
        score_boost = 1 + min(0.45, abs(score_1d) * 0.05)
        base_move_1d = max(vol_pct * event_boost, min(4.8, abs(score_1d) * 0.24))
        expected_move_1d = round(max(0.12, base_move_1d * score_boost), 2)
        expected_move_1w = round(max(expected_move_1d * sqrt(5) * 0.92, abs(score_1w) * 0.35), 2)

        directional_shift_pct = clamp(score_1w * 0.22, -expected_move_1w * 0.42, expected_move_1w * 0.42)
        range_low, range_high = None, None
        stop_level, take_profit_level = None, None
        if spot is not None:
            center = spot * (1 + directional_shift_pct / 100)
            half_width = spot * max(0.15, expected_move_1w * 0.54) / 100
            range_low = round(center - half_width, 4)
            range_high = round(center + half_width, 4)

            if bias_1d == "bullish":
                stop_level = round(spot * (1 - max(0.18, expected_move_1d * 0.72) / 100), 4)
                take_profit_level = round(spot * (1 + max(0.2, expected_move_1w * 0.76) / 100), 4)
            elif bias_1d == "bearish":
                stop_level = round(spot * (1 + max(0.18, expected_move_1d * 0.72) / 100), 4)
                take_profit_level = round(spot * (1 - max(0.2, expected_move_1w * 0.76) / 100), 4)
            else:
                stop_level = round(range_low, 4)
                take_profit_level = round(range_high, 4)

        conflict_penalty = 7 if direction_to_text(news_score) != direction_to_text(trend_score) and news_score else 0
        confidence = int(
            clamp(
                46 + abs(score_1d) * 4.8 + abs(score_1w) * 3.2 + min(10, abs(event_score) * 2.5) - conflict_penalty,
                38,
                93,
            )
        )

        drivers = self._merge_drivers(
            [
                f"Trend {week_move:+.2f}% / 5D, {month_move:+.2f}% / 1M",
                f"Realized vol {vol_pct:.2f}% daily, regime {vol_regime}",
                *news_drivers,
                *event_drivers,
            ]
        )

        risk_stance = self._risk_stance(bias_1d, bias_1w, vol_regime, confidence, event_score)
        driver_summary = " | ".join(drivers[:3])

        return QuantAssetOutlook(
            asset=asset,
            label=label,
            group=group,
            spot=spot,
            one_day_bias=bias_1d,
            one_week_bias=bias_1w,
            expected_move_pct_1d=expected_move_1d,
            expected_move_pct_1w=expected_move_1w,
            range_low=range_low,
            range_high=range_high,
            stop_level=stop_level,
            take_profit_level=take_profit_level,
            volatility_regime=vol_regime,
            risk_stance=risk_stance,
            confidence=confidence,
            trend_score=round(trend_score, 2),
            news_score=round(news_score, 2),
            event_score=round(event_score, 2),
            driver_summary=driver_summary,
            drivers=drivers[:5],
        )

    def _score_news(
        self,
        asset: str,
        news_items: list[NewsItem],
        speeches: list[SpeechTapeItem],
    ) -> tuple[float, list[str]]:
        total = 0.0
        signal_counter: Counter[str] = Counter()

        for item in news_items[:42]:
            age_hours = max(0.0, (datetime.now(UTC) - item.published_at).total_seconds() / 3600)
            decay = max(0.18, 1 - age_hours / 96)
            impact_weight = 0.55 + item.impact_score / 120
            asset_score = sum(
                SIGNAL_DIRECTION_RULES.get(signal, {}).get(asset, 0.0)
                for signal in item.matched_signals
            )
            if asset_score == 0:
                continue
            signal_counter.update(item.matched_signals)
            total += asset_score * decay * impact_weight

        for item in speeches[:10]:
            age_days = max(0.0, (datetime.now(UTC) - item.published_at).total_seconds() / 86400)
            decay = max(0.2, 1 - age_days / 30)
            asset_score = sum(
                SIGNAL_DIRECTION_RULES.get(signal, {}).get(asset, 0.0)
                for signal in item.matched_signals
            )
            if asset_score == 0:
                continue
            signal_counter.update(item.matched_signals)
            total += asset_score * decay * 0.75

        drivers = [f"News theme: {signal}" for signal, _ in signal_counter.most_common(2)]
        return clamp(total, -4.2, 4.2), drivers

    def _score_events(
        self,
        asset: str,
        upcoming_events: list[ScheduledEvent],
        directional_anchor: float,
    ) -> tuple[float, list[str]]:
        total = 0.0
        drivers: list[str] = []
        now = datetime.now(UTC)

        for event in sorted(upcoming_events, key=lambda item: item.scheduled_at or now)[:10]:
            if event.scheduled_at is None:
                continue
            hours_ahead = (event.scheduled_at - now).total_seconds() / 3600
            if hours_ahead < -8 or hours_ahead > 24 * 7:
                continue

            sensitivity = sum(
                abs(SIGNAL_DIRECTION_RULES.get(signal, {}).get(asset, 0.0))
                for signal in event.signals
            )
            if sensitivity == 0:
                continue

            horizon_weight = 1.2 if hours_ahead <= 24 else 0.8 if hours_ahead <= 72 else 0.45
            importance = EVENT_IMPORTANCE_WEIGHTS.get(event.importance, 0.45)
            direction = 1.0 if directional_anchor >= 0.45 else -1.0 if directional_anchor <= -0.45 else 0.0
            total += sensitivity * horizon_weight * importance * 0.35 * direction

            eta = "today" if hours_ahead <= 24 else f"{int(hours_ahead // 24)}d"
            drivers.append(f"Event risk: {event.title} {eta}")

        return clamp(total, -3.0, 3.0), drivers[:2]

    def _risk_stance(
        self,
        bias_1d: str,
        bias_1w: str,
        vol_regime: str,
        confidence: int,
        event_score: float,
    ) -> str:
        if vol_regime == "stressed":
            if confidence >= 68 and bias_1d == bias_1w and bias_1d != "neutral":
                return "Trade smaller; keep trailing hedge on"
            return "Cut gross risk; prefer optionality"
        if abs(event_score) >= 1.4:
            return "Hold lighter into event window"
        if bias_1d == bias_1w and bias_1d != "neutral" and confidence >= 70:
            return "Press trend with defined stop"
        if bias_1d != bias_1w:
            return "Short-term noise; reduce size"
        return "Range bias; fade extremes, keep hedges ready"

    def _build_regime(
        self,
        now: datetime,
        assets: list[QuantAssetOutlook],
        news_items: list[NewsItem],
        upcoming_events: list[ScheduledEvent],
    ) -> QuantRegime:
        by_asset = {asset.asset: asset for asset in assets}
        dxy = by_asset.get("DXY")
        us10y = by_asset.get("US10Y")
        spx = by_asset.get("SPX")
        gold = by_asset.get("GOLD")
        btc = by_asset.get("BTCUSD")

        usd_pressure = average([
            (dxy.trend_score + dxy.news_score + dxy.event_score) if dxy else 0.0,
            (us10y.trend_score + us10y.news_score + us10y.event_score) if us10y else 0.0,
        ])
        risk_pressure = average([
            (spx.trend_score + spx.news_score) if spx else 0.0,
            (btc.trend_score + btc.news_score) if btc else 0.0,
        ])
        haven_pressure = (gold.trend_score + gold.news_score) if gold else 0.0

        if usd_pressure >= 2.1 and risk_pressure <= 0:
            label = "USD DEFENSIVE"
        elif usd_pressure >= 1.4 and (us10y.one_week_bias if us10y else "neutral") == "bullish":
            label = "RATE-LED USD BULL"
        elif usd_pressure <= -1.2 and risk_pressure >= 0.8:
            label = "RISK-ON USD FADE"
        else:
            label = "EVENT-DRIVEN RANGE"

        usd_bias = "bullish" if usd_pressure >= 1.1 else "bearish" if usd_pressure <= -1.1 else "neutral"
        stressed = sum(1 for asset in assets if asset.volatility_regime == "stressed")
        compressed = sum(1 for asset in assets if asset.volatility_regime == "compressed")
        if stressed >= 3:
            volatility_state = "stressed"
        elif compressed >= 4:
            volatility_state = "compressed"
        else:
            volatility_state = "normal"

        high_impact_count = sum(1 for item in news_items if item.impact_level == "high" and now - item.published_at <= timedelta(days=2))
        near_event_count = sum(
            1
            for event in upcoming_events
            if event.scheduled_at is not None and 0 <= (event.scheduled_at - now).total_seconds() <= 72 * 3600
        )
        headline_risk = "high" if high_impact_count >= 4 or near_event_count >= 2 else "elevated" if high_impact_count >= 2 or near_event_count >= 1 else "contained"

        confidence = int(clamp(54 + abs(usd_pressure) * 7 + stressed * 3 + near_event_count * 4, 42, 91))
        focus_assets = [
            asset.asset
            for asset in sorted(
                assets,
                key=lambda item: max(item.expected_move_pct_1d or 0, item.expected_move_pct_1w or 0),
                reverse=True,
            )[:4]
        ]

        actions = [
            "Keep DXY / EURUSD / US10Y gross lighter if a high-importance USD event is inside 72h.",
            "Use GOLD or front-end rates as hedge when headline risk flips to stressed.",
            "Press trend only when 1D and 1W bias agree; otherwise cut size and wait for confirmation.",
        ]
        if label == "RISK-ON USD FADE":
            actions[1] = "Lean into pro-risk assets only if rates stop firming; hedge with DXY or short-duration rates."
        elif label == "RATE-LED USD BULL":
            actions[0] = "Favor USD strength against low-yield FX; keep equity/crypto risk trimmed while yields rise."

        summary = (
            f"{label} with {headline_risk} headline risk. "
            f"USD bias is {usd_bias}, cross-asset vol is {volatility_state}, "
            f"and haven tone is {direction_to_text(haven_pressure)}."
        )

        return QuantRegime(
            label=label,
            usd_bias=usd_bias,
            volatility_state=volatility_state,
            headline_risk=headline_risk,
            confidence=confidence,
            summary=summary,
            actions=actions,
            focus_assets=focus_assets,
        )

    def _merge_drivers(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        output: list[str] = []
        for value in values:
            cleaned = value.strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            output.append(cleaned)
        return output
