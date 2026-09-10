import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, closing
from pathlib import Path
from uuid import uuid4

import nats
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from pydantic import BaseModel, ConfigDict, Field, field_validator

from video_analyzer.frames import UnreadableVideoError, iter_frames
from video_analyzer.publisher import STREAM, publish

logging.basicConfig(level="INFO", format="%(levelname)s %(message)s")
log = logging.getLogger("video_analyzer")

VIDEOS_DIR = Path(os.environ.get("VIDEOS_DIR", "videos")).resolve()
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")

FRAMES_DISPATCHED = Counter("frames_dispatched_total", "Frames published to the stream")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    nc = await nats.connect(NATS_URL)
    app.state.js = nc.jetstream()
    await app.state.js.add_stream(STREAM)
    yield
    await nc.drain()


app = FastAPI(title="VideoAnalyzer", lifespan=lifespan)


class AnalyzeRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    file_path: str = Field(min_length=1)
    fps: int

    @field_validator("fps")
    @classmethod
    def fps_must_be_2_or_4(cls, value: int) -> int:
        if value not in (2, 4):
            raise ValueError("fps must be 2 or 4")
        return value


class AnalyzeResponse(BaseModel):
    video_id: str
    frames_dispatched: int


def resolve_video_path(file_path: str) -> Path:
    try:
        path = (VIDEOS_DIR / file_path).resolve()
        inside = path.is_relative_to(VIDEOS_DIR)
        is_file = path.is_file()
    except (ValueError, OSError) as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid file_path") from exc
    if not inside:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, f"file_path must be inside {VIDEOS_DIR}"
        )
    if not is_file:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"video not found: {file_path}")
    return path


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/analyze")
async def analyze(body: AnalyzeRequest, request: Request) -> AnalyzeResponse:
    path = resolve_video_path(body.file_path)
    video_id = f"{path.stem}-{uuid4().hex}"
    dispatched = 0
    try:
        with closing(iter_frames(path, body.fps)) as frames:
            while (frame := await asyncio.to_thread(next, frames, None)) is not None:
                await publish(request.app.state.js, video_id, frame)
                dispatched += 1
                FRAMES_DISPATCHED.inc()
    except UnreadableVideoError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except TimeoutError as exc:
        detail = f"{exc}; {dispatched} frames already dispatched as {video_id}"
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail) from exc
    except nats.errors.Error as exc:
        log.exception("nats failure video_id=%s frames=%d", video_id, dispatched)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "frame queue unavailable"
        ) from exc

    log.info("dispatched frames=%d video_id=%s fps=%d", dispatched, video_id, body.fps)
    return AnalyzeResponse(video_id=video_id, frames_dispatched=dispatched)
