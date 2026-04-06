from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timezone

import httpx

from app.config import Settings
from app.models import HubStatus, NewsItem, SourceStatus
from app.services.impact import assess_raw_item
from app.services.sources import NewsSource


class NewsHub:
    def __init__(self, sources: list[NewsSource], settings: Settings) -> None:
        self.sources = sources
        self.settings = settings
        self._items: dict[str, NewsItem] = {}
        self._source_statuses: dict[str, SourceStatus] = {
            source.name: SourceStatus(
                name=source.name,
                kind=source.kind,
                homepage=source.homepage,
            )
            for source in sources
        }
        self._subscribers: set[asyncio.Queue[dict]] = set()
        self._state_lock = asyncio.Lock()
        self._refresh_lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None
        self.last_sync_at: datetime | None = None
        self.last_new_item_at: datetime | None = None
        self.last_refresh_count: int = 0

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.settings.http_timeout_seconds,
                headers={
                    "User-Agent": self.settings.request_user_agent,
                    "Accept": "text/html,application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
                },
                follow_redirects=True,
            )

        await self.refresh()

        if self._task is None:
            self._task = asyncio.create_task(self._run_loop(), name="usd-pulse-wire-loop")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _run_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.poll_interval_seconds)
            await self.refresh()

    async def refresh(self) -> list[NewsItem]:
        async with self._refresh_lock:
            if self._client is None:
                raise RuntimeError("HTTP client is not initialized.")

            now = datetime.now(timezone.utc)
            gathered = await asyncio.gather(
                *(source.fetch(self._client) for source in self.sources),
                return_exceptions=True,
            )

            candidate_items: list[NewsItem] = []
            next_statuses: dict[str, SourceStatus] = {}

            for source, result in zip(self.sources, gathered, strict=True):
                current = self._source_statuses[source.name]

                if isinstance(result, Exception):
                    next_statuses[source.name] = current.model_copy(
                        update={"last_error": str(result)}
                    )
                    continue

                next_statuses[source.name] = current.model_copy(
                    update={
                        "last_success_at": now,
                        "last_error": None,
                        "last_item_count": len(result),
                    }
                )

                for raw_item in result:
                    scored = assess_raw_item(raw_item, now=now)
                    if scored is not None:
                        candidate_items.append(scored)

            candidate_items.sort(key=lambda item: item.published_at)

            inserted: list[NewsItem] = []
            async with self._state_lock:
                self._source_statuses.update(next_statuses)
                self.last_sync_at = now

                for item in candidate_items:
                    if item.id in self._items:
                        continue
                    self._items[item.id] = item
                    inserted.append(item)

                self.last_refresh_count = len(inserted)
                if inserted:
                    self.last_new_item_at = inserted[-1].published_at
                    self._prune_history_locked()

            if inserted:
                await self._broadcast(inserted)

            return inserted

    def subscribe(self) -> asyncio.Queue[dict]:
        queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=50)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict]) -> None:
        self._subscribers.discard(queue)

    async def snapshot(self) -> list[NewsItem]:
        async with self._state_lock:
            return sorted(
                self._items.values(),
                key=lambda item: item.published_at,
                reverse=True,
            )

    async def status_snapshot(self) -> HubStatus:
        async with self._state_lock:
            statuses = sorted(self._source_statuses.values(), key=lambda status: status.name)
            connected = sum(1 for status in statuses if status.last_error is None)
            return HubStatus(
                poll_interval_seconds=self.settings.poll_interval_seconds,
                connected_sources=connected,
                subscriber_count=len(self._subscribers),
                tracked_items=len(self._items),
                last_sync_at=self.last_sync_at,
                last_new_item_at=self.last_new_item_at,
                last_refresh_count=self.last_refresh_count,
                sources=statuses,
            )

    async def _broadcast(self, items: list[NewsItem]) -> None:
        dead_queues: list[asyncio.Queue[dict]] = []

        for item in items:
            payload = item.model_dump(mode="json")
            for queue in self._subscribers:
                try:
                    queue.put_nowait(payload)
                except asyncio.QueueFull:
                    with suppress(asyncio.QueueEmpty):
                        queue.get_nowait()
                    try:
                        queue.put_nowait(payload)
                    except asyncio.QueueFull:
                        dead_queues.append(queue)

        for queue in dead_queues:
            self._subscribers.discard(queue)

    def _prune_history_locked(self) -> None:
        ranked = sorted(
            self._items.values(),
            key=lambda item: item.published_at,
            reverse=True,
        )
        keep = {item.id for item in ranked[: self.settings.history_limit]}
        self._items = {item_id: item for item_id, item in self._items.items() if item_id in keep}
