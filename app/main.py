from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.services.asset_analysis import AssetAnalysisService
from app.services.calendar_data import EconomicCalendarService
from app.services.hub import NewsHub
from app.services.market_data import MarketDataService
from app.services.quant_outlook import QuantOutlookService
from app.services.speech_data import SpeechTapeService
from app.services.sources import build_default_sources


hub = NewsHub(build_default_sources(), settings)
market_data = MarketDataService(settings)
calendar_data = EconomicCalendarService(settings)
speech_data = SpeechTapeService(settings)
quant_outlook = QuantOutlookService(settings, hub, market_data, calendar_data, speech_data)
asset_analysis = AssetAnalysisService(settings, hub, market_data, calendar_data, speech_data)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await calendar_data.start()
    await market_data.start()
    await speech_data.start()
    await quant_outlook.start()
    await asset_analysis.start()
    await hub.start()
    try:
        yield
    finally:
        await hub.stop()
        await asset_analysis.stop()
        await quant_outlook.stop()
        await speech_data.stop()
        await market_data.stop()
        await calendar_data.stop()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=settings.static_dir), name="static")


def format_sse(event: str, payload: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"


def no_store_json(payload: dict) -> JSONResponse:
    return JSONResponse(
        content=payload,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
        },
    )


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(
        settings.static_dir / "index.html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
        },
    )


@app.get("/api/news")
async def list_news(
    limit: int = Query(default=settings.default_feed_limit, ge=1, le=250),
    impact: str = Query(default="all"),
    q: str = Query(default=""),
):
    items = await hub.snapshot()
    needle = q.casefold().strip()
    selected = []

    for item in items:
        if impact != "all" and item.impact_level != impact:
            continue

        if needle:
            haystack = " ".join(
                [item.title, item.summary, *item.categories, *item.matched_terms]
            ).casefold()
            if needle not in haystack:
                continue

        selected.append(item)
        if len(selected) >= limit:
            break

    return no_store_json({
        "items": [item.model_dump(mode="json") for item in selected],
        "status": (await hub.status_snapshot()).model_dump(mode="json"),
        "market": (await market_data.snapshot()).model_dump(mode="json"),
    })


@app.post("/api/refresh")
async def refresh_now():
    inserted = await hub.refresh()
    return no_store_json({
        "inserted": [item.model_dump(mode="json") for item in inserted],
        "status": (await hub.status_snapshot()).model_dump(mode="json"),
    })


@app.get("/api/status")
async def status():
    return no_store_json((await hub.status_snapshot()).model_dump(mode="json"))


@app.get("/api/market-snapshot")
async def market_snapshot(force: bool = Query(default=False)):
    return no_store_json((await market_data.snapshot(force=force)).model_dump(mode="json"))


@app.get("/api/calendar")
async def calendar_snapshot(force: bool = Query(default=False)):
    return no_store_json((await calendar_data.snapshot(force=force)).model_dump(mode="json"))


@app.get("/api/trump-tape")
async def trump_tape_snapshot(force: bool = Query(default=False)):
    if not force:
        return no_store_json((await speech_data.snapshot(force=False)).model_dump(mode="json"))

    def run_speech_cli() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "app.services.speech_cli"],
            capture_output=True,
            text=True,
            cwd=str(settings.project_dir),
            check=False,
        )

    result = await asyncio.to_thread(run_speech_cli)
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=(result.stderr or "speech subprocess failed").strip(),
        )

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail="Invalid speech snapshot payload.") from exc

    return no_store_json(payload)


@app.get("/api/quant-outlook")
async def quant_snapshot(force: bool = Query(default=False)):
    return no_store_json((await quant_outlook.snapshot(force=force)).model_dump(mode="json"))


@app.get("/api/asset-analysis")
async def asset_analysis_snapshot(
    asset: str = Query(..., min_length=1),
    force: bool = Query(default=False),
):
    try:
        snapshot = await asset_analysis.snapshot(asset=asset, force=force)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return no_store_json(snapshot.model_dump(mode="json"))


@app.get("/api/asset-chart")
async def asset_chart(
    asset: str = Query(..., min_length=1),
    timeframe: str = Query(default="5D"),
    force: bool = Query(default=False),
):
    try:
        chart = await market_data.chart(asset=asset, timeframe=timeframe, force=force)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return no_store_json(chart.model_dump(mode="json"))


@app.get("/health")
async def health():
    status_snapshot = await hub.status_snapshot()
    return no_store_json({
        "ok": True,
        "connected_sources": status_snapshot.connected_sources,
        "tracked_items": status_snapshot.tracked_items,
    })


@app.get("/stream")
async def stream(request: Request):
    queue = hub.subscribe()

    async def event_stream():
        yield format_sse("hello", {"status": "connected"})

        try:
            while True:
                if await request.is_disconnected():
                    break

                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=18)
                    yield format_sse("news", payload)
                except TimeoutError:
                    status_payload = (await hub.status_snapshot()).model_dump(mode="json")
                    yield format_sse("ping", status_payload)
        finally:
            hub.unsubscribe(queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
