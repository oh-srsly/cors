import logging
import os
from pathlib import Path
from uuid import uuid4

import redis
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from video_analyzer.frames import UnreadableVideoError, iter_frames
from video_analyzer.publisher import FramePublisher

logging.basicConfig(level="INFO", format="%(levelname)s %(message)s")
log = logging.getLogger("video_analyzer")

VIDEOS_DIR = Path(os.environ.get("VIDEOS_DIR", "videos")).resolve()
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
MAX_BACKLOG = int(os.environ.get("MAX_BACKLOG", "500"))
STREAM = "frames"

app = FastAPI(title="VideoAnalyzer")
publisher = FramePublisher(redis.Redis.from_url(REDIS_URL), STREAM, MAX_BACKLOG)


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


@app.post("/analyze")
def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    path = resolve_video_path(request.file_path)
    video_id = f"{path.stem}-{uuid4().hex[:8]}"
    try:
        dispatched = publisher.publish(video_id, iter_frames(path, request.fps))
    except UnreadableVideoError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    except redis.RedisError as exc:
        log.exception("redis failure while dispatching %s", video_id)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "frame queue unavailable"
        ) from exc

    log.info("dispatched %d frames of %s at %d fps", dispatched, video_id, request.fps)
    return AnalyzeResponse(video_id=video_id, frames_dispatched=dispatched)
