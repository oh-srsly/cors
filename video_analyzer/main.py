import logging
import os
from pathlib import Path

import redis
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from video_analyzer.frames import UnreadableVideoError, VideoSource
from video_analyzer.publisher import BacklogTimeoutError, FramePublisher

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s")
log = logging.getLogger("video_analyzer")

VIDEOS_DIR = Path(os.environ.get("VIDEOS_DIR", "videos")).resolve()
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
FRAMES_STREAM = os.environ.get("FRAMES_STREAM", "frames")
MAX_BACKLOG = int(os.environ.get("MAX_BACKLOG", "500"))

app = FastAPI(title="VideoAnalyzer")
publisher = FramePublisher(redis.Redis.from_url(REDIS_URL), FRAMES_STREAM, MAX_BACKLOG)


class AnalyzeRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    file_path: str = Field(min_length=1, description=f"Video path, relative to {VIDEOS_DIR}")
    fps: StrictInt = Field(description="Frames per second to extract: 2 or 4")

    @field_validator("fps")
    @classmethod
    def fps_must_be_2_or_4(cls, value: int) -> int:
        if value not in (2, 4):
            raise ValueError("fps must be 2 or 4")
        return value


class AnalyzeResponse(BaseModel):
    video_id: str
    source_fps: float
    fps: int
    frames_dispatched: int


def resolve_video_path(file_path: str) -> Path:
    path = (VIDEOS_DIR / file_path).resolve()
    if not path.is_relative_to(VIDEOS_DIR):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"file_path must be inside {VIDEOS_DIR}")
    if not path.is_file():
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"video not found: {file_path}")
    return path


@app.post("/analyze", response_model=AnalyzeResponse)
def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    path = resolve_video_path(request.file_path)
    video_id = path.stem
    dispatched = 0
    try:
        with VideoSource(path) as video:
            for frame in video.frames(request.fps):
                publisher.publish(video_id, frame)
                dispatched += 1
            source_fps = video.fps
    except UnreadableVideoError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except BacklogTimeoutError as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    except redis.RedisError as exc:
        log.exception("redis failure after %d frames of %s", dispatched, video_id)
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "frame queue unavailable") from exc

    log.info("dispatched %d frames of %s at %d fps", dispatched, video_id, request.fps)
    return AnalyzeResponse(
        video_id=video_id, source_fps=source_fps, fps=request.fps, frames_dispatched=dispatched
    )
