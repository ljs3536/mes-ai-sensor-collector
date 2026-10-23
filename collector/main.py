"""센서 수집기.

현재 기능:
- MQTT 파형과 스칼라를 받아 InfluxDB에 저장. 계정이 있으면 그 계정으로 접속한다
- 파형 블록에서 RMS, peak, crest, kurtosis, 1x, 2x 특징값을 계산
- 최신 파형, 기간 검색, 한 시각의 파형, 스펙트럼, 특징값, 파형 SSE를 조회 API로 제공
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from .config import get_settings
from .features import decode_samples, fft_spectrum
from .mqtt import Ingest
from .schemas import FeatureOut, ScalarOut, WaveformOut, WaveformSummary
from .store import SensorStore, iso_kst

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

store: SensorStore | None = None
ingest: Ingest | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global store, ingest
    store = SensorStore()
    ingest = Ingest(store)
    ingest.start()
    yield
    ingest.stop()
    store.close()


app = FastAPI(title="MES Sensor Collector", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _store() -> SensorStore:
    if store is None:
        raise RuntimeError("store not ready")
    return store


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    ok = _store().ping()
    return JSONResponse(
        {"status": "ready" if ok else "degraded", "influx": ok, "mqtt": bool(ingest and ingest.connected())},
        status_code=200 if ok else 503,
    )


@app.get("/api/machines")
def machines():
    return {"machines": _store().machines()}


@app.get("/api/scalars", response_model=list[ScalarOut])
def scalars(minutes: int = Query(5, ge=1, le=1440)):
    return [
        ScalarOut(
            machine=r["machine"],
            sensor=r["sensor"],
            value=float(r["value"]),
            unit=str(r.get("unit") or ""),
            time=r["time"],
        )
        for r in _store().latest_scalars(minutes)
    ]


def _preset(labels: list[dict], ts) -> str:
    best = ""
    best_gap = 5.0
    for label in labels:
        try:
            gap = abs((label["ts"] - ts).total_seconds())
        except TypeError:
            continue
        if gap < best_gap:
            best_gap = gap
            best = label["preset"]
    return best


@app.get("/api/waveforms", response_model=list[WaveformSummary])
def waveforms(machine: str, start: datetime, stop: datetime, channel: str | None = None):
    """기간 안의 파형 블록 목록. 샘플은 포함하지 않는다."""
    if stop <= start or stop - start > timedelta(days=8):
        return JSONResponse({"detail": "검색 기간은 8일 이하여야 합니다"}, status_code=422)
    try:
        rows = _store().list_waveforms(machine, start, stop, channel)
        labels = _store().labels_between(machine, start, stop)
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=422)
    return [
        WaveformSummary(
            machine=row["machine"],
            channel=row["channel"],
            ts=row["ts"],
            sample_rate=row["sampleRate"],
            n=row["n"],
            rpm=row["rpm"],
            unit=row["unit"],
            preset=_preset(labels, row["ts"]),
        )
        for row in rows
    ]


@app.get("/api/waveform", response_model=WaveformOut)
def waveform(machine: str, channel: str | None = None, with_fft: bool = True, at: datetime | None = None):
    if at is not None:
        if not channel:
            return JSONResponse({"detail": "시각을 지정하면 채널이 필요합니다"}, status_code=422)
        try:
            row = _store().waveform_at(machine, channel, at)
        except ValueError as e:
            return JSONResponse({"detail": str(e)}, status_code=422)
    else:
        row = _store().latest_waveform(machine, channel)
    if row is None:
        return JSONResponse({"detail": "파형이 아직 없습니다"}, status_code=404)
    samples = decode_samples(row["samples"]).tolist()
    freqs, mag = fft_spectrum(samples, row["sampleRate"]) if with_fft else ([], [])
    return WaveformOut(
        machine=row["machine"],
        channel=row["channel"],
        ts=row["ts"],
        sample_rate=row["sampleRate"],
        n=row["n"],
        rpm=row["rpm"],
        unit=row["unit"],
        samples=samples,
        freqs=freqs,
        magnitudes=mag,
    )


@app.get("/api/features/at", response_model=FeatureOut)
def feature_at(machine: str, channel: str, at: datetime):
    try:
        row = _store().features_at(machine, channel, at)
    except ValueError as e:
        return JSONResponse({"detail": str(e)}, status_code=422)
    if row is None:
        return JSONResponse({"detail": "특징값이 없습니다"}, status_code=404)
    return FeatureOut(**row)


@app.get("/api/features", response_model=list[FeatureOut])
def features(machine: str, minutes: int = Query(30, ge=1, le=1440), channel: str | None = None):
    return [FeatureOut(**row) for row in _store().features(machine, minutes, channel)]


@app.get("/api/stream/waveform")
async def stream_waveform(request: Request, machine: str, channel: str | None = None):
    async def events():
        last = None
        while True:
            if await request.is_disconnected():
                break
            try:
                row = _store().latest_waveform(machine, channel)
            except Exception:
                row = None
            key = (iso_kst(row["ts"]), row["channel"]) if row else None
            if row and key != last:
                last = key
                samples = decode_samples(row["samples"]).tolist()
                freqs, mag = fft_spectrum(samples, row["sampleRate"])
                body = WaveformOut(
                    machine=row["machine"],
                    channel=row["channel"],
                    ts=row["ts"],
                    sample_rate=row["sampleRate"],
                    n=row["n"],
                    rpm=row["rpm"],
                    unit=row["unit"],
                    samples=samples,
                    freqs=freqs,
                    magnitudes=mag,
                ).model_dump(by_alias=True, mode="json")
                yield f"data: {json.dumps(body)}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})
