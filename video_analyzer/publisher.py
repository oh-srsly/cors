import time
from collections.abc import Iterable

import redis

from video_analyzer.frames import Frame

CHECK_EVERY = 16
MAX_WAIT_SECONDS = 60
POLL_SECONDS = 0.05


class FramePublisher:
    def __init__(self, client: redis.Redis, stream: str, max_backlog: int) -> None:
        self._client = client
        self._stream = stream
        self._max_backlog = max_backlog

    def publish(self, video_id: str, frames: Iterable[Frame]) -> int:
        published = 0
        for frame in frames:
            if published % CHECK_EVERY == 0:
                self._wait_for_capacity()
            self._client.xadd(
                self._stream,
                {"video_id": video_id, "frame_id": frame.index, "jpeg": frame.jpeg},
            )
            published += 1
        return published

    def _wait_for_capacity(self) -> None:
        for _ in range(int(MAX_WAIT_SECONDS / POLL_SECONDS)):
            # consumers delete entries once processed, so length == unprocessed backlog
            if self._client.xlen(self._stream) < self._max_backlog:
                return
            time.sleep(POLL_SECONDS)
        raise TimeoutError(
            f"backlog stayed above {self._max_backlog} for {MAX_WAIT_SECONDS}s"
        )
