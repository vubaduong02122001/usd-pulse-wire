from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import log, sqrt, tanh
from statistics import pstdev

from app.config import Settings
from app.models import (
    AssetQuote,
    NewsItem,
    PriceBar,
    QuantAssetOutlook,
    QuantFactorLeg,
    QuantMicrostructure,
    QuantRegime,
    QuantRiskBudget,
    QuantSnapshot,
    ScheduledEvent,
    SpeechTapeItem,
)
from app.services.calendar_data import EconomicCalendarService
from app.services.hub import NewsHub
from app.services.market_data import TRACKED_ASSETS, MarketDataService
from app.services.speech_data import SpeechTapeService


UTC = timezone.utc
MODEL_VERSION = "QES-2"
ASSET_ORDER = [asset.label for asset in TRACKED_ASSETS]
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
VOL_THRESHOLDS = {
    "Dollar": (0.22, 0.65),
    "FX": (0.3, 0.95),
    "Rates": (0.38, 1.1),
    "Commodity": (0.9, 2.3),
    "Equity": (0.7, 1.7),
    "Crypto": (1.9, 4.8),
}
EVENT_IMPORTANCE_WEIGHTS = {"high": 1.2, "medium": 0.75, "watch": 0.45}
CROSS_ASSET_EXPOSURES = {
    "DXY": {"US10Y": 0.9, "SPX": -0.4, "GOLD": -0.55, "BTCUSD": -0.22, "WTI": -0.08},
    "EURUSD": {"DXY": -1.0, "US10Y": -0.35, "SPX": 0.12, "GOLD": 0.16},
    "USDJPY": {"DXY": 0.65, "US10Y": 0.78, "SPX": 0.15, "GOLD": -0.2},
    "GBPUSD": {"DXY": -0.92, "US10Y": -0.28, "SPX": 0.1, "GOLD": 0.08},
    "GOLD": {"DXY": -0.7, "US10Y": -0.6, "SPX": -0.12, "WTI": 0.1, "BTCUSD": 0.12},
    "WTI": {"DXY": -0.25, "SPX": 0.72, "GOLD": -0.05, "US10Y": 0.08},
    "US10Y": {"DXY": 0.72, "SPX": -0.25, "GOLD": -0.45, "WTI": 0.12},
    "SPX": {"DXY": -0.55, "US10Y": -0.42, "BTCUSD": 0.26, "WTI": 0.3, "GOLD": -0.12},
    "BTCUSD": {"DXY": -0.45, "US10Y": -0.35, "SPX": 0.38, "GOLD": 0.12, "ETHUSD": 0.32},
    "ETHUSD": {"DXY": -0.48, "US10Y": -0.32, "SPX": 0.34, "BTCUSD": 0.5},
}
HEDGE_MAP = {
    "DXY": ["GOLD", "SPX"],
    "EURUSD": ["DXY", "US10Y"],
    "USDJPY": ["US10Y", "GOLD"],
    "GBPUSD": ["DXY", "US10Y"],
    "GOLD": ["DXY", "US10Y"],
    "WTI": ["DXY", "SPX"],
    "US10Y": ["GOLD", "SPX"],
    "SPX": ["DXY", "US10Y"],
    "BTCUSD": ["DXY", "US10Y"],
    "ETHUSD": ["DXY", "BTCUSD"],
}


@dataclass(frozen=True, slots=True)
class FeatureBundle:
    realized_vol: float
    parkinson_vol: float
    atr_pct: float
    efficiency: float
    trend_score: float
    reversion_score: float
    price_zscore: float
    range_expansion: float
    volume_zscore: float | None
    close_location: float
    wick_skew: float
    last_return: float
    week_return: float
    month_return: float
    ma_gap: float


@dataclass(frozen=True, slots=True)
class OrderBookStats:
    spread_bps: float
    imbalance: float
    liquidity_state: str


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


def pct_returns(bars: list[PriceBar], *, window: int | None = None) -> list[float]:
    if len(bars) < 2:
        return []
    sliced = bars[-window:] if window else bars
    output: list[float] = []
    for previous, current in zip(sliced, sliced[1:], strict=False):
        if previous.close == 0:
            continue
        output.append((current.close - previous.close) / previous.close * 100)
    return output


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


def realized_vol_pct(bars: list[PriceBar], *, window: int = 36) -> float:
    returns = pct_returns(bars, window=window)
    if len(returns) < 3:
        return 0.0
    annualizer = sqrt(max(6, min(24, len(returns))))
    return pstdev(returns) * annualizer


def parkinson_vol_pct(bars: list[PriceBar], *, window: int = 24) -> float:
    sample = bars[-window:] if len(bars) > window else bars
    values = []
    for bar in sample:
        if bar.low <= 0 or bar.high <= 0 or bar.high <= bar.low:
            continue
        values.append(log(bar.high / bar.low) ** 2)
    if not values:
        return 0.0
    return sqrt(average(values) / (4 * log(2))) * 100


def true_range_pct(bar: PriceBar, previous_close: float | None) -> float:
    if bar.close == 0:
        return 0.0
    points = [bar.high - bar.low]
    if previous_close is not None:
        points.append(abs(bar.high - previous_close))
        points.append(abs(bar.low - previous_close))
    return max(points) / bar.close * 100


def atr_pct(bars: list[PriceBar], *, window: int = 14) -> float:
    if not bars:
        return 0.0
    sample = bars[-window:] if len(bars) > window else bars
    tr_values = []
    previous_close: float | None = None
    for bar in sample:
        tr_values.append(true_range_pct(bar, previous_close))
        previous_close = bar.close
    return average(tr_values)


def efficiency_ratio(bars: list[PriceBar], *, window: int = 20) -> float:
    sample = bars[-window:] if len(bars) > window else bars
    if len(sample) < 2:
        return 0.0
    path = sum(abs(current.close - previous.close) for previous, current in zip(sample, sample[1:], strict=False))
    if path == 0:
        return 0.0
    return abs(sample[-1].close - sample[0].close) / path


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


def zscore_of_last(values: list[float], *, window: int = 20) -> float:
    if len(values) < 4:
        return 0.0
    sample = values[-window:] if len(values) > window else values
    sigma = stddev(sample[:-1] or sample)
    if sigma == 0:
        return 0.0
    baseline = average(sample[:-1] or sample)
    return (sample[-1] - baseline) / sigma


def volatility_label(group: str, value: float) -> str:
    calm_threshold, stressed_threshold = VOL_THRESHOLDS.get(group, (0.5, 1.5))
    if value >= stressed_threshold:
        return "stressed"
    if value <= calm_threshold:
        return "compressed"
    return "normal"


def bias_label_from_return(expected_return_pct: float, expected_move_pct: float) -> str:
    threshold = max(0.06, expected_move_pct * 0.22)
    if expected_return_pct >= threshold:
        return "bullish"
    if expected_return_pct <= -threshold:
        return "bearish"
    return "neutral"


def direction_to_text(score: float) -> str:
    if score > 0:
        return "up"
    if score < 0:
        return "down"
    return "flat"


def state_from_score(score: float) -> str:
    if score >= 1.2:
        return "strong positive"
    if score >= 0.35:
        return "positive"
    if score <= -1.2:
        return "strong negative"
    if score <= -0.35:
        return "negative"
    return "neutral"


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

            order_book_tasks = [self._fetch_coinbase_order_book(asset) for asset in COINBASE_PRODUCTS]
            order_book_results = await asyncio.gather(*order_book_tasks, return_exceptions=True)
            order_books = {
                asset: None if isinstance(result, Exception) else result
                for asset, result in zip(COINBASE_PRODUCTS, order_book_results, strict=True)
            }

            recent_news = [item for item in news_items if now - item.published_at <= timedelta(days=5)]
            recent_speeches = [item for item in speeches.items if now - item.published_at <= timedelta(days=45)]
            upcoming_events = [
                item
                for item in calendar.schedule
                if item.scheduled_at is not None and item.scheduled_at >= now - timedelta(hours=8)
            ]

            feature_map = {
                asset: self._compute_features(five_day_by_asset.get(asset, []), one_month_by_asset.get(asset, []))
                for asset in ASSET_ORDER
            }
            leader_signals = {
                asset: self._leader_signal(asset, quotes_by_asset.get(asset), feature_map.get(asset))
                for asset in ASSET_ORDER
            }

            assets = [
                self._build_asset_outlook(
                    asset=asset,
                    quote=quotes_by_asset.get(asset),
                    features=feature_map.get(asset),
                    news_items=recent_news,
                    speeches=recent_speeches,
                    upcoming_events=upcoming_events,
                    leader_signals=leader_signals,
                    order_book=order_books.get(asset),
                )
                for asset in ASSET_ORDER
            ]
            regime = self._build_regime(now, assets, recent_news, upcoming_events)

            self._snapshot = QuantSnapshot(
                updated_at=now,
                source="QES-2 ensemble: multi-horizon OHLCV + news macro + cross-asset + microstructure",
                regime=regime,
                assets=assets,
            )
            return self._snapshot

    def _compute_features(self, bars_5d: list[PriceBar], bars_1m: list[PriceBar]) -> FeatureBundle:
        reference = bars_5d or bars_1m
        if not reference:
            return FeatureBundle(
                realized_vol=0.0,
                parkinson_vol=0.0,
                atr_pct=0.0,
                efficiency=0.0,
                trend_score=0.0,
                reversion_score=0.0,
                price_zscore=0.0,
                range_expansion=1.0,
                volume_zscore=None,
                close_location=0.0,
                wick_skew=0.0,
                last_return=0.0,
                week_return=0.0,
                month_return=0.0,
                ma_gap=0.0,
            )

        closes_ref = [bar.close for bar in reference]
        last_bar = reference[-1]
        realized = max(realized_vol_pct(bars_5d, window=36), 0.08)
        park = parkinson_vol_pct(bars_5d or bars_1m, window=24)
        atr_value = atr_pct(bars_5d or bars_1m, window=14)
        efficiency = efficiency_ratio(reference, window=20)
        ma_gap = moving_average_gap_pct(bars_1m or reference, fast=8, slow=21)
        last_return = pct_returns(reference, window=3)
        last_return_value = last_return[-1] if last_return else 0.0
        week_return = period_return_pct(bars_5d or reference, lookback=min(24, max(1, len(bars_5d or reference) - 1)))
        month_return = period_return_pct(bars_1m or reference, lookback=min(18, max(1, len(bars_1m or reference) - 1)))
        price_z = zscore_of_last(closes_ref, window=20)
        range_mult = range_expansion(reference)
        vol_z = volume_zscore(reference, window=18)
        clv = close_location(last_bar)
        wick = wick_skew(last_bar)

        norm = max(realized, atr_value, 0.15)
        trend_core = (week_return / norm) * 0.72 + (month_return / max(norm * 1.6, 0.2)) * 0.34
        persistence = (efficiency - 0.5) * 2.4
        momentum_burst = (last_return_value / norm) * 0.42
        trend_score = clamp(trend_core + persistence + momentum_burst + ma_gap * 6.4, -4.8, 4.8)

        reversion_score = clamp(
            -price_z * (1.15 if efficiency < 0.45 else 0.7)
            - (range_mult - 1) * 0.38 * (1 if last_return_value > 0 else -1 if last_return_value < 0 else 0),
            -3.8,
            3.8,
        )

        return FeatureBundle(
            realized_vol=round(realized, 3),
            parkinson_vol=round(park, 3),
            atr_pct=round(atr_value, 3),
            efficiency=round(efficiency, 3),
            trend_score=round(trend_score, 3),
            reversion_score=round(reversion_score, 3),
            price_zscore=round(price_z, 3),
            range_expansion=round(range_mult, 3),
            volume_zscore=None if vol_z is None else round(vol_z, 3),
            close_location=round(clv, 3),
            wick_skew=round(wick, 3),
            last_return=round(last_return_value, 3),
            week_return=round(week_return, 3),
            month_return=round(month_return, 3),
            ma_gap=round(ma_gap, 3),
        )

    def _leader_signal(
        self,
        asset: str,
        quote: AssetQuote | None,
        features: FeatureBundle | None,
    ) -> float:
        if quote is None or features is None:
            return 0.0
        intraday = quote.percent_change or 0.0
        norm = max(features.realized_vol, features.atr_pct, 0.2)
        signal = intraday / norm + features.week_return / max(norm * 1.2, 0.25) * 0.6 + features.trend_score * 0.32
        return clamp(signal, -4.5, 4.5)

    def _build_asset_outlook(
        self,
        asset: str,
        quote: AssetQuote | None,
        features: FeatureBundle | None,
        news_items: list[NewsItem],
        speeches: list[SpeechTapeItem],
        upcoming_events: list[ScheduledEvent],
        leader_signals: dict[str, float],
        order_book: OrderBookStats | None,
    ) -> QuantAssetOutlook:
        config = self.market_data.resolve_asset(asset)
        label = quote.label if quote is not None else config.label
        group = quote.group if quote is not None else config.group
        spot = quote.last if quote is not None else None
        feature = features or self._compute_features([], [])

        cross_asset_score, cross_drivers = self._score_cross_asset(asset, leader_signals)
        news_score, news_drivers = self._score_news(asset, news_items, speeches)
        microstructure = self._build_microstructure(asset, feature, order_book)
        anchor = feature.trend_score * 0.45 + news_score * 0.55 + cross_asset_score * 0.35 + microstructure[0] * 0.5
        event_score, event_drivers = self._score_events(asset, upcoming_events, directional_anchor=anchor)

        vol_regime = volatility_label(group, max(feature.realized_vol, feature.parkinson_vol))
        model_state = self._model_state(feature, vol_regime, event_score)
        weights = self._factor_weights(feature, vol_regime, order_book, upcoming_events)

        factor_specs = [
            ("Trend", feature.trend_score, weights["trend"], f"ER {feature.efficiency:.2f}, MA gap {feature.ma_gap:+.2f}%"),
            ("Mean reversion", feature.reversion_score, weights["reversion"], f"Z-score {feature.price_zscore:+.2f}"),
            ("News macro", news_score, weights["news"], "; ".join(news_drivers[:2]) or "No dominant macro pulse"),
            ("Cross-asset", cross_asset_score, weights["cross"], "; ".join(cross_drivers[:2]) or "Cross tape muted"),
            ("Microstructure", microstructure[0], weights["micro"], microstructure[1].note or microstructure[1].pressure),
            ("Event risk", event_score, weights["event"], "; ".join(event_drivers[:2]) or "Calendar quiet"),
        ]
        factors = [
            QuantFactorLeg(
                name=name,
                score=round(score, 3),
                weight=round(weight, 3),
                contribution=round(score * weight, 3),
                state=state_from_score(score),
                note=note or None,
            )
            for name, score, weight, note in factor_specs
        ]

        ensemble_score = round(sum(factor.contribution for factor in factors), 3)
        expected_move_1d = round(
            max(
                0.08,
                feature.realized_vol * 0.52
                + feature.atr_pct * 0.42
                + abs(microstructure[0]) * 0.14
                + abs(event_score) * 0.1,
            ),
            2,
        )
        expected_move_1w = round(
            max(expected_move_1d * sqrt(5) * 0.94, feature.atr_pct * 1.85 + abs(ensemble_score) * 0.18),
            2,
        )
        expected_return_1d = round(tanh(ensemble_score / 2.9) * expected_move_1d, 2)
        week_score = feature.trend_score * 0.36 + news_score * 0.2 + cross_asset_score * 0.16 + microstructure[0] * 0.08 + event_score * 0.08 - feature.reversion_score * 0.12
        expected_return_1w = round(tanh(week_score / 3.1) * expected_move_1w, 2)

        one_day_bias = bias_label_from_return(expected_return_1d, expected_move_1d)
        one_week_bias = bias_label_from_return(expected_return_1w, expected_move_1w)
        confidence = self._confidence(feature, factors, vol_regime, order_book)

        range_low, range_high = None, None
        stop_level, take_profit_level = None, None
        if spot is not None:
            range_center = spot * (1 + expected_return_1w / 100)
            half_width = spot * expected_move_1w * 0.58 / 100
            range_low = round(range_center - half_width, 4)
            range_high = round(range_center + half_width, 4)

            if one_day_bias == "bullish":
                stop_level = round(spot * (1 - max(expected_move_1d * 0.72, 0.18) / 100), 4)
                take_profit_level = round(spot * (1 + max(expected_move_1w * 0.82, 0.24) / 100), 4)
            elif one_day_bias == "bearish":
                stop_level = round(spot * (1 + max(expected_move_1d * 0.72, 0.18) / 100), 4)
                take_profit_level = round(spot * (1 - max(expected_move_1w * 0.82, 0.24) / 100), 4)
            else:
                stop_level = round(range_low, 4)
                take_profit_level = round(range_high, 4)

        risk_budget = self._risk_budget(
            asset=asset,
            vol_regime=vol_regime,
            expected_move_1d=expected_move_1d,
            confidence=confidence,
            one_day_bias=one_day_bias,
            one_week_bias=one_week_bias,
            feature=feature,
        )

        drivers = self._merge_drivers(
            [
                f"Trend {feature.week_return:+.2f}% / 5D and {feature.month_return:+.2f}% / 1M",
                f"Micro {microstructure[1].pressure} / {microstructure[1].liquidity_state}",
                *news_drivers,
                *cross_drivers,
                *event_drivers,
                f"VaR95 {risk_budget.var_95_1d_pct:.2f}% / risk budget {risk_budget.position_risk_pct:.2f}%",
            ]
        )
        driver_summary = " | ".join(drivers[:3])

        return QuantAssetOutlook(
            asset=asset,
            label=label,
            group=group,
            spot=spot,
            model_state=model_state,
            one_day_bias=one_day_bias,
            one_week_bias=one_week_bias,
            one_day_return_pct=expected_return_1d,
            one_week_return_pct=expected_return_1w,
            expected_move_pct_1d=expected_move_1d,
            expected_move_pct_1w=expected_move_1w,
            range_low=range_low,
            range_high=range_high,
            stop_level=stop_level,
            take_profit_level=take_profit_level,
            volatility_regime=vol_regime,
            risk_stance=risk_budget.guidance,
            confidence=confidence,
            trend_score=round(feature.trend_score, 2),
            mean_reversion_score=round(feature.reversion_score, 2),
            news_score=round(news_score, 2),
            event_score=round(event_score, 2),
            cross_asset_score=round(cross_asset_score, 2),
            microstructure_score=round(microstructure[0], 2),
            ensemble_score=round(ensemble_score, 2),
            driver_summary=driver_summary,
            drivers=drivers[:6],
            factors=factors,
            microstructure=microstructure[1],
            risk_budget=risk_budget,
        )

    def _score_news(
        self,
        asset: str,
        news_items: list[NewsItem],
        speeches: list[SpeechTapeItem],
    ) -> tuple[float, list[str]]:
        total = 0.0
        signal_counter: Counter[str] = Counter()
        now = datetime.now(UTC)

        for item in news_items[:48]:
            age_hours = max(0.0, (now - item.published_at).total_seconds() / 3600)
            decay = max(0.15, 1 - age_hours / 96)
            impact_weight = 0.52 + item.impact_score / 115
            asset_score = sum(
                SIGNAL_DIRECTION_RULES.get(signal, {}).get(asset, 0.0)
                for signal in item.matched_signals
            )
            if asset_score == 0:
                continue
            novelty = 1.08 if item.source_kind == "official" else 1.0
            signal_counter.update(item.matched_signals)
            total += asset_score * decay * impact_weight * novelty

        for item in speeches[:12]:
            age_days = max(0.0, (now - item.published_at).total_seconds() / 86400)
            decay = max(0.2, 1 - age_days / 30)
            asset_score = sum(
                SIGNAL_DIRECTION_RULES.get(signal, {}).get(asset, 0.0)
                for signal in item.matched_signals
            )
            if asset_score == 0:
                continue
            signal_counter.update(item.matched_signals)
            total += asset_score * decay * 0.72

        drivers = [f"Macro pulse: {signal}" for signal, _ in signal_counter.most_common(3)]
        return clamp(total, -4.6, 4.6), drivers

    def _score_cross_asset(
        self,
        asset: str,
        leader_signals: dict[str, float],
    ) -> tuple[float, list[str]]:
        exposures = CROSS_ASSET_EXPOSURES.get(asset, {})
        if not exposures:
            return 0.0, []

        total = 0.0
        drivers: list[str] = []
        for other, exposure in exposures.items():
            other_signal = leader_signals.get(other, 0.0)
            contribution = exposure * other_signal
            total += contribution
            if abs(contribution) >= 0.35:
                drivers.append(f"{other} {direction_to_text(contribution)} cross-factor")
        return clamp(total * 0.38, -3.6, 3.6), drivers[:3]

    def _build_microstructure(
        self,
        asset: str,
        feature: FeatureBundle,
        order_book: OrderBookStats | None,
    ) -> tuple[float, QuantMicrostructure]:
        base_score = (
            feature.close_location * 1.05
            + feature.wick_skew * 0.8
            + (feature.range_expansion - 1) * (0.65 if feature.last_return >= 0 else -0.65)
            + (0.28 * feature.volume_zscore if feature.volume_zscore is not None else 0.0)
        )
        mode = "bar-proxy"
        spread_bps = None
        imbalance = None
        liquidity_state = "normal"
        note = (
            f"CLV {feature.close_location:+.2f}, wick {feature.wick_skew:+.2f}, "
            f"range x{feature.range_expansion:.2f}"
        )
        if order_book is not None:
            mode = "order-book+bar"
            spread_bps = order_book.spread_bps
            imbalance = order_book.imbalance
            liquidity_state = order_book.liquidity_state
            base_score += order_book.imbalance * 1.8 - order_book.spread_bps * 0.015
            note = (
                f"Top-book imbalance {order_book.imbalance:+.2f}, spread {order_book.spread_bps:.1f} bps, "
                f"bar pressure {base_score:+.2f}"
            )
        else:
            range_proxy = feature.atr_pct * 8.0
            liquidity_state = "stressed" if range_proxy >= 16 else "thin" if range_proxy >= 9 else "normal"

        score = clamp(base_score, -3.6, 3.6)
        if score >= 0.9:
            pressure = "buy pressure"
        elif score <= -0.9:
            pressure = "sell pressure"
        else:
            pressure = "balanced tape"

        micro = QuantMicrostructure(
            mode=mode,
            pressure=pressure,
            liquidity_state=liquidity_state,
            spread_bps=None if spread_bps is None else round(spread_bps, 2),
            order_book_imbalance=None if imbalance is None else round(imbalance, 3),
            volume_zscore=feature.volume_zscore,
            close_location=feature.close_location,
            wick_skew=feature.wick_skew,
            range_efficiency=feature.efficiency,
            range_expansion=feature.range_expansion,
            note=note,
        )
        return score, micro

    def _score_events(
        self,
        asset: str,
        upcoming_events: list[ScheduledEvent],
        directional_anchor: float,
    ) -> tuple[float, list[str]]:
        total = 0.0
        drivers: list[str] = []
        now = datetime.now(UTC)
        anchor_direction = 1.0 if directional_anchor >= 0.4 else -1.0 if directional_anchor <= -0.4 else 0.0

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
            total += sensitivity * horizon_weight * importance * 0.38 * anchor_direction

            eta = "today" if hours_ahead <= 24 else f"{int(hours_ahead // 24)}d"
            drivers.append(f"Event risk: {event.title} {eta}")

        return clamp(total, -3.2, 3.2), drivers[:3]

    def _factor_weights(
        self,
        feature: FeatureBundle,
        vol_regime: str,
        order_book: OrderBookStats | None,
        upcoming_events: list[ScheduledEvent],
    ) -> dict[str, float]:
        event_weight = 0.08 if upcoming_events else 0.04
        weights = {
            "trend": 0.31 + (0.05 if feature.efficiency >= 0.55 else -0.03),
            "reversion": 0.1 + (0.08 if abs(feature.price_zscore) >= 1.0 or vol_regime == "stressed" else 0.0),
            "news": 0.18,
            "cross": 0.14,
            "micro": 0.15 + (0.05 if order_book is not None else 0.0),
            "event": event_weight,
        }
        if vol_regime == "stressed":
            weights["trend"] -= 0.05
            weights["reversion"] += 0.04
            weights["news"] += 0.02
        total = sum(weights.values())
        return {name: value / total for name, value in weights.items()}

    def _confidence(
        self,
        feature: FeatureBundle,
        factors: list[QuantFactorLeg],
        vol_regime: str,
        order_book: OrderBookStats | None,
    ) -> int:
        signed = [factor.score for factor in factors if abs(factor.score) >= 0.3]
        positives = sum(1 for value in signed if value > 0)
        negatives = sum(1 for value in signed if value < 0)
        agreement = max(positives, negatives) / len(signed) if signed else 0.5
        data_bonus = 5 if feature.volume_zscore is not None else 0
        data_bonus += 6 if order_book is not None else 0
        vol_penalty = 7 if vol_regime == "stressed" else 0
        confidence = 42 + agreement * 20 + abs(sum(factor.contribution for factor in factors)) * 8 + data_bonus - vol_penalty
        return int(clamp(confidence, 36, 95))

    def _risk_budget(
        self,
        asset: str,
        vol_regime: str,
        expected_move_1d: float,
        confidence: int,
        one_day_bias: str,
        one_week_bias: str,
        feature: FeatureBundle,
    ) -> QuantRiskBudget:
        vol = max(feature.realized_vol, feature.parkinson_vol * 0.8, feature.atr_pct * 0.7, 0.12)
        var_95 = round(1.65 * vol, 2)
        expected_shortfall = round(max(2.1 * vol, var_95 * 1.18), 2)
        regime_mult = 0.72 if vol_regime == "stressed" else 0.88 if vol_regime == "normal" else 1.0
        conviction_mult = 1.1 if one_day_bias == one_week_bias and one_day_bias != "neutral" else 0.82
        position_risk_pct = round(clamp((confidence / 140) * regime_mult * conviction_mult, 0.18, 1.35), 2)
        max_gross_pct = round(clamp(12 + confidence * 0.22 - expected_shortfall * 2.3, 6, 34), 2)
        hedge_ratio = round(clamp(expected_shortfall / max(expected_move_1d, 0.12) * 0.28, 0.15, 1.0), 2)
        guidance = self._risk_guidance(one_day_bias, one_week_bias, vol_regime, confidence)

        return QuantRiskBudget(
            regime=vol_regime,
            guidance=guidance,
            var_95_1d_pct=var_95,
            expected_shortfall_pct=expected_shortfall,
            position_risk_pct=position_risk_pct,
            max_gross_pct=max_gross_pct,
            hedge_ratio=hedge_ratio,
            hedge_assets=HEDGE_MAP.get(asset, ["DXY", "US10Y"]),
        )

    def _risk_guidance(
        self,
        one_day_bias: str,
        one_week_bias: str,
        vol_regime: str,
        confidence: int,
    ) -> str:
        if vol_regime == "stressed":
            if one_day_bias == one_week_bias and one_day_bias != "neutral" and confidence >= 70:
                return "Trade half-size, keep hedge ratio elevated"
            return "Reduce gross, prefer optionality and tighter stops"
        if one_day_bias != one_week_bias:
            return "Signals mixed; size down and wait for confirmation"
        if confidence >= 75 and one_day_bias != "neutral":
            return "Press trend with defined stop and trailing hedge"
        return "Range/risk-balanced posture; fade extremes"

    def _model_state(
        self,
        feature: FeatureBundle,
        vol_regime: str,
        event_score: float,
    ) -> str:
        if abs(event_score) >= 1.0:
            return "event-dominant"
        if vol_regime == "stressed":
            return "stress-repricing"
        if feature.efficiency >= 0.55:
            return "trend-following"
        if abs(feature.price_zscore) >= 1.2:
            return "mean-reversion"
        return "balanced"

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
            (dxy.ensemble_score if dxy else 0.0),
            (us10y.ensemble_score if us10y else 0.0),
        ])
        risk_pressure = average([
            (spx.ensemble_score if spx else 0.0),
            (btc.ensemble_score if btc else 0.0),
        ])
        haven_pressure = gold.ensemble_score if gold else 0.0
        dispersion = round(pstdev([asset.ensemble_score for asset in assets]) if len(assets) >= 2 else 0.0, 3)

        if usd_pressure >= 1.7 and risk_pressure <= 0.1:
            label = "RATE-LED USD BULL"
        elif usd_pressure <= -1.3 and risk_pressure >= 0.75:
            label = "RISK-ON USD FADE"
        elif haven_pressure >= 0.9 and risk_pressure <= 0:
            label = "DEFENSIVE HAVEN REGIME"
        else:
            label = "CROSS-ASSET TRANSITION"

        usd_bias = "bullish" if usd_pressure >= 0.9 else "bearish" if usd_pressure <= -0.9 else "neutral"
        stressed = sum(1 for asset in assets if asset.volatility_regime == "stressed")
        compressed = sum(1 for asset in assets if asset.volatility_regime == "compressed")
        if stressed >= 3:
            volatility_state = "stressed"
        elif compressed >= 4:
            volatility_state = "compressed"
        else:
            volatility_state = "normal"

        weighted_high_impact = sum(
            item.impact_score / 100
            for item in news_items
            if item.impact_level == "high" and now - item.published_at <= timedelta(days=2)
        )
        near_event_count = sum(
            1
            for event in upcoming_events
            if event.scheduled_at is not None and 0 <= (event.scheduled_at - now).total_seconds() <= 72 * 3600
        )
        event_intensity = round(weighted_high_impact + near_event_count * 0.9, 2)
        headline_risk = "high" if event_intensity >= 4.2 else "elevated" if event_intensity >= 2.2 else "contained"

        confidence = int(clamp(52 + abs(usd_pressure) * 10 + max(0, 1.5 - dispersion) * 8 - stressed * 2, 40, 92))
        focus_assets = [
            asset.asset
            for asset in sorted(
                assets,
                key=lambda item: abs(item.one_day_return_pct or 0) + abs(item.one_week_return_pct or 0),
                reverse=True,
            )[:4]
        ]
        actions = [
            "Trade only when ensemble, microstructure, and macro legs align; otherwise cut gross and let event risk clear.",
            "Use hedge ratios from the quant panel rather than fixed size. Stressed regime requires lower gross and wider optionality.",
            "Promote assets with high confidence and low cross-factor conflict; demote assets where trend and reversion are fighting.",
        ]
        if label == "RATE-LED USD BULL":
            actions[0] = "Favor USD longs against low-yield FX and keep equity/crypto risk hedged while rates drive the tape."
        elif label == "RISK-ON USD FADE":
            actions[1] = "Lean into pro-risk assets only while DXY and US10Y ensemble scores stay below neutral."

        summary = (
            f"{label} under {MODEL_VERSION}. "
            f"USD bias is {usd_bias}, cross-asset vol is {volatility_state}, "
            f"headline risk is {headline_risk}, and score dispersion sits at {dispersion:.2f}."
        )

        return QuantRegime(
            label=label,
            model_version=MODEL_VERSION,
            usd_bias=usd_bias,
            volatility_state=volatility_state,
            headline_risk=headline_risk,
            confidence=confidence,
            event_intensity=event_intensity,
            cross_asset_dispersion=dispersion,
            summary=summary,
            actions=actions,
            focus_assets=focus_assets,
        )

    async def _fetch_coinbase_order_book(self, asset: str) -> OrderBookStats | None:
        product_id = COINBASE_PRODUCTS.get(asset)
        client = self.market_data._client
        if product_id is None or client is None:
            return None

        response = await client.get(
            f"https://api.exchange.coinbase.com/products/{product_id}/book",
            params={"level": 2},
        )
        response.raise_for_status()
        payload = response.json()
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
