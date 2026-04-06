from __future__ import annotations

import asyncio
import csv
import io
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from app.config import Settings
from app.models import CalendarSnapshot, MacroIndicator, ScheduledEvent, TimelinePoint
from app.services.sources import clean_text


UTC = timezone.utc
EASTERN = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class IndicatorSpec:
    id: str
    series_id: str
    title: str
    category: str
    frequency: str
    source: str
    source_url: str
    unit: str
    digits: int
    signals: tuple[str, ...]
    history_limit: int
    multiplier: float = 1.0
    suffix: str = ""
    note: str = ""


@dataclass(frozen=True, slots=True)
class ScheduleSpec:
    id: str
    title: str
    category: str
    frequency: str
    importance: str
    source: str
    source_url: str
    signals: tuple[str, ...]
    summary: str
    keywords: tuple[str, ...]
    note: str = ""


INDICATOR_SPECS: tuple[IndicatorSpec, ...] = (
    IndicatorSpec(
        id="cpi",
        series_id="CPIAUCSL",
        title="CPI Index",
        category="Inflation",
        frequency="monthly",
        source="FRED / BLS",
        source_url="https://fred.stlouisfed.org/series/CPIAUCSL",
        unit="index",
        digits=1,
        signals=("Inflation",),
        history_limit=15,
        note="Monthly CPI history from St. Louis Fed / FRED.",
    ),
    IndicatorSpec(
        id="core-pce",
        series_id="PCEPILFE",
        title="Core PCE Index",
        category="Inflation",
        frequency="monthly",
        source="FRED / BEA",
        source_url="https://fred.stlouisfed.org/series/PCEPILFE",
        unit="index",
        digits=1,
        signals=("Inflation",),
        history_limit=15,
        note="Core PCE price index history from St. Louis Fed / FRED.",
    ),
    IndicatorSpec(
        id="unrate",
        series_id="UNRATE",
        title="Unemployment Rate",
        category="Labor",
        frequency="monthly",
        source="FRED / BLS",
        source_url="https://fred.stlouisfed.org/series/UNRATE",
        unit="%",
        digits=1,
        signals=("Labor",),
        history_limit=15,
        suffix="%",
        note="Monthly unemployment rate history from St. Louis Fed / FRED.",
    ),
    IndicatorSpec(
        id="payems",
        series_id="PAYEMS",
        title="Nonfarm Payrolls",
        category="Labor",
        frequency="monthly",
        source="FRED / BLS",
        source_url="https://fred.stlouisfed.org/series/PAYEMS",
        unit="mn jobs",
        digits=1,
        signals=("Labor", "Growth"),
        history_limit=15,
        multiplier=0.001,
        suffix="M",
        note="Total nonfarm payrolls history from St. Louis Fed / FRED.",
    ),
    IndicatorSpec(
        id="gdp-growth",
        series_id="A191RL1Q225SBEA",
        title="Real GDP Growth",
        category="Growth",
        frequency="quarterly",
        source="FRED / BEA",
        source_url="https://fred.stlouisfed.org/series/A191RL1Q225SBEA",
        unit="%",
        digits=1,
        signals=("Growth",),
        history_limit=12,
        suffix="%",
        note="Quarterly real GDP annualized growth from St. Louis Fed / FRED.",
    ),
)


BEA_SCHEDULE_SPECS: tuple[ScheduleSpec, ...] = (
    ScheduleSpec(
        id="gdp-release",
        title="GDP Release Calendar",
        category="Growth",
        frequency="quarterly",
        importance="high",
        source="BEA Schedule",
        source_url="https://www.bea.gov/news/schedule",
        signals=("Growth",),
        summary="Advance, second, and third GDP estimates.",
        keywords=("gdp", "gross domestic product"),
        note="Official BEA release schedule.",
    ),
    ScheduleSpec(
        id="pce-release",
        title="Personal Income / PCE",
        category="Inflation",
        frequency="monthly",
        importance="high",
        source="BEA Schedule",
        source_url="https://www.bea.gov/news/schedule",
        signals=("Inflation", "Growth"),
        summary="Personal income, spending, and PCE release cadence.",
        keywords=("personal income and outlays",),
        note="Official BEA release schedule.",
    ),
    ScheduleSpec(
        id="trade-release",
        title="U.S. Trade Balance",
        category="Dollar Flows",
        frequency="monthly",
        importance="medium",
        source="BEA Schedule",
        source_url="https://www.bea.gov/news/schedule",
        signals=("Growth", "FX / rates"),
        summary="Trade in goods and services release cadence.",
        keywords=("trade in goods and services",),
        note="Official BEA release schedule.",
    ),
)


def format_number(value: float, digits: int) -> str:
    return f"{value:,.{digits}f}"


def format_indicator_value(spec: IndicatorSpec, value: float) -> str:
    adjusted = value * spec.multiplier
    return f"{format_number(adjusted, spec.digits)}{spec.suffix}"


def format_period_label(moment: datetime, frequency: str) -> str:
    if frequency == "quarterly":
        quarter = (moment.month - 1) // 3 + 1
        return f"Q{quarter} {moment.year}"
    if frequency == "yearly":
        return str(moment.year)
    return moment.strftime("%b %Y")


def parse_schedule_datetime(date_text: str, time_text: str, year: int) -> datetime:
    parsed = datetime.strptime(
        f"{date_text} {year} {time_text}",
        "%B %d %Y %I:%M %p",
    )
    return parsed.replace(tzinfo=EASTERN).astimezone(UTC)


def parse_fomc_end_datetime(year: int, month_text: str, date_text: str) -> datetime:
    cleaned = date_text.replace("*", "").strip()
    last_day = cleaned.split("-")[-1]
    parsed = datetime.strptime(
        f"{month_text} {last_day} {year} 2:00 PM",
        "%B %d %Y %I:%M %p",
    )
    return parsed.replace(tzinfo=EASTERN).astimezone(UTC)


def run_curl(url: str, timeout_seconds: int) -> str:
    executable = shutil.which("curl") or shutil.which("curl.exe")
    if executable is None:
        raise RuntimeError("curl is not available in PATH.")

    result = subprocess.run(
        [
            executable,
            "-L",
            "--silent",
            "--show-error",
            "--max-time",
            str(timeout_seconds),
            url,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = clean_text(result.stderr) or "curl request failed"
        raise RuntimeError(stderr)
    return result.stdout


class EconomicCalendarService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = asyncio.Lock()
        self._client: httpx.AsyncClient | None = None
        self._snapshot: CalendarSnapshot | None = None

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

    async def snapshot(self, *, force: bool = False) -> CalendarSnapshot:
        async with self._lock:
            if self._client is None:
                raise RuntimeError("HTTP client is not initialized.")

            now = datetime.now(UTC)
            if (
                not force
                and self._snapshot is not None
                and now - self._snapshot.updated_at < timedelta(minutes=15)
            ):
                return self._snapshot

            indicators, schedule = await asyncio.gather(
                self._fetch_indicators(now),
                self._fetch_schedule(now),
            )
            self._snapshot = CalendarSnapshot(
                updated_at=now,
                source="Official Fed / BEA schedules with FRED indicator history",
                indicators=indicators,
                schedule=schedule,
            )
            return self._snapshot

    async def _fetch_text(self, url: str) -> str:
        if "fred.stlouisfed.org" in url:
            try:
                return await asyncio.to_thread(
                    run_curl,
                    url,
                    max(25, self.settings.http_timeout_seconds * 2),
                )
            except Exception:
                pass

        if self._client is None:
            raise RuntimeError("HTTP client is not initialized.")
        response = await self._client.get(url)
        response.raise_for_status()
        return response.text

    async def _fetch_indicators(self, now: datetime) -> list[MacroIndicator]:
        indicators = await asyncio.gather(
            *(self._fetch_indicator(spec, now) for spec in INDICATOR_SPECS)
        )
        return [indicator for indicator in indicators if indicator is not None]

    async def _fetch_indicator(
        self,
        spec: IndicatorSpec,
        now: datetime,
    ) -> MacroIndicator | None:
        start_year = now.year - 5
        csv_text = await self._fetch_text(
            f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={spec.series_id}&cosd={start_year}-01-01"
        )

        reader = csv.DictReader(io.StringIO(csv_text))
        history: list[TimelinePoint] = []

        for row in reader:
            raw_value = clean_text(row.get(spec.series_id))
            if raw_value in {"", ".", "NaN"}:
                continue

            try:
                numeric_value = float(raw_value)
            except ValueError:
                continue

            observation_text = clean_text(row.get("observation_date"))
            if not observation_text:
                continue

            observed_at = datetime.fromisoformat(observation_text).replace(tzinfo=UTC)
            history.append(
                TimelinePoint(
                    at=observed_at,
                    label=format_period_label(observed_at, spec.frequency),
                    display_value=format_indicator_value(spec, numeric_value),
                    numeric_value=numeric_value,
                )
            )

        if not history:
            return None

        history.sort(key=lambda point: point.at, reverse=True)
        limited_history = history[: spec.history_limit]
        current = limited_history[0]
        previous = limited_history[1] if len(limited_history) > 1 else None

        return MacroIndicator(
            id=spec.id,
            title=spec.title,
            category=spec.category,
            frequency=spec.frequency,
            source=spec.source,
            source_url=spec.source_url,
            unit=spec.unit,
            signals=list(spec.signals),
            current_display=current.display_value or "--",
            previous_display=previous.display_value if previous else None,
            current_value=current.numeric_value,
            previous_value=previous.numeric_value if previous else None,
            updated_at=now,
            history=limited_history,
            note=spec.note,
        )

    async def _fetch_schedule(self, now: datetime) -> list[ScheduledEvent]:
        bea_events, fomc_event = await asyncio.gather(
            self._fetch_bea_schedule(now),
            self._fetch_fomc_schedule(now),
        )
        events = [*bea_events]
        if fomc_event is not None:
            events.append(fomc_event)

        def sort_key(event: ScheduledEvent) -> tuple[int, datetime]:
            if event.scheduled_at is not None:
                return (0, event.scheduled_at)
            if event.last_release_at is not None:
                return (1, event.last_release_at)
            return (2, now)

        events.sort(key=sort_key)
        return events

    async def _fetch_bea_schedule(self, now: datetime) -> list[ScheduledEvent]:
        html = await self._fetch_text("https://www.bea.gov/news/schedule")
        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one("table")
        if table is None:
            return []

        table_text = clean_text(table.get_text(" ", strip=True))
        year_match = re.search(r"\bYear\s+(\d{4})\b", table_text)
        release_year = int(year_match.group(1)) if year_match else now.year

        grouped_points: dict[str, list[TimelinePoint]] = {
            spec.id: [] for spec in BEA_SCHEDULE_SPECS
        }

        for row in table.select("tr.scheduled-releases-type-press"):
            date_node = row.select_one(".release-date")
            time_node = row.select_one("small.text-muted")
            title_node = row.select_one(".release-title")
            if date_node is None or time_node is None or title_node is None:
                continue

            date_text = clean_text(date_node.get_text(" ", strip=True))
            time_text = clean_text(time_node.get_text(" ", strip=True))
            title = clean_text(title_node.get_text(" ", strip=True))
            if not date_text or not time_text or not title:
                continue

            scheduled_at = parse_schedule_datetime(date_text, time_text, release_year)
            lower_title = title.casefold()
            for spec in BEA_SCHEDULE_SPECS:
                if any(keyword in lower_title for keyword in spec.keywords):
                    grouped_points[spec.id].append(
                        TimelinePoint(
                            at=scheduled_at,
                            label=title,
                            display_value=scheduled_at.astimezone(EASTERN).strftime(
                                "%d %b %Y %H:%M ET"
                            ),
                        )
                    )

        events: list[ScheduledEvent] = []
        for spec in BEA_SCHEDULE_SPECS:
            history = sorted(grouped_points[spec.id], key=lambda point: point.at, reverse=True)
            if not history:
                continue

            next_release = next(
                (point for point in sorted(history, key=lambda point: point.at) if point.at >= now),
                None,
            )
            last_release = next((point for point in history if point.at < now), None)
            events.append(
                ScheduledEvent(
                    id=spec.id,
                    title=spec.title,
                    category=spec.category,
                    frequency=spec.frequency,
                    importance=spec.importance,
                    source=spec.source,
                    source_url=spec.source_url,
                    signals=list(spec.signals),
                    summary=spec.summary,
                    scheduled_at=next_release.at if next_release else None,
                    last_release_at=last_release.at if last_release else None,
                    history=history[:8],
                    note=spec.note,
                )
            )

        return events

    async def _fetch_fomc_schedule(self, now: datetime) -> ScheduledEvent | None:
        html = await self._fetch_text(
            "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
        )
        soup = BeautifulSoup(html, "html.parser")
        history: list[TimelinePoint] = []

        for heading in soup.select("h4"):
            heading_text = clean_text(heading.get_text(" ", strip=True))
            match = re.match(r"(\d{4})\s+FOMC Meetings", heading_text)
            if match is None:
                continue

            year = int(match.group(1))
            sibling = heading.find_next_sibling()
            while sibling is not None and sibling.name != "h4":
                classes = sibling.get("class", [])
                if "fomc-meeting" in classes:
                    month_node = sibling.select_one(".fomc-meeting__month")
                    date_node = sibling.select_one(".fomc-meeting__date")
                    if month_node is not None and date_node is not None:
                        month_text = clean_text(month_node.get_text(" ", strip=True))
                        date_text = clean_text(date_node.get_text(" ", strip=True))
                        end_at = parse_fomc_end_datetime(year, month_text, date_text)
                        history.append(
                            TimelinePoint(
                                at=end_at,
                                label=f"{month_text} {date_text.replace('*', '')}",
                                display_value=end_at.astimezone(EASTERN).strftime(
                                    "%d %b %Y %H:%M ET"
                                ),
                                note="SEP / press conference" if "*" in date_text else None,
                            )
                        )
                sibling = sibling.find_next_sibling()

        if not history:
            return None

        history.sort(key=lambda point: point.at, reverse=True)
        next_release = next(
            (point for point in sorted(history, key=lambda point: point.at) if point.at >= now),
            None,
        )
        last_release = next((point for point in history if point.at < now), None)
        return ScheduledEvent(
            id="fomc",
            title="FOMC Rate Decision",
            category="Monetary Policy",
            frequency="8x yearly",
            importance="high",
            source="Federal Reserve",
            source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
            signals=["Fed policy", "FX / rates"],
            summary="Meeting-end dates, statements, SEP, and press conference cadence.",
            scheduled_at=next_release.at if next_release else None,
            last_release_at=last_release.at if last_release else None,
            history=history[:10],
            note="FOMC meeting end dates shown at 14:00 ET.",
        )
