import time

import redis

from video_analyzer.frames import Frame


class BacklogTimeoutError(Exception):
    pass


class FramePublisher:
    """Publishes frames to a Redis Stream, blocking while backlog > max_backlog."""

    def __init__(
        self,
        client: redis.Redis,
        stream: str,
        max_backlog: int,
        max_wait_seconds: float = 60.0,
        poll_interval_seconds: float = 0.05,
    ) -> None:
        self._client = client
        self._stream = stream
        self._max_backlog = max_backlog
        self._max_wait = max_wait_seconds
        self._poll_interval = poll_interval_seconds

    def publish(self, video_id: str, frame: Frame) -> None:
        self._wait_for_capacity()
        self._client.xadd(
            self._stream,
            {"video_id": video_id, "frame_id": frame.index, "jpeg": frame.jpeg},
        )

    def _wait_for_capacity(self) -> None:
        deadline = time.monotonic() + self._max_wait
        while self._client.xlen(self._stream) >= self._max_backlog:
            if time.monotonic() >= deadline:
                raise BacklogTimeoutError(
                    f"stream {self._stream!r} stayed above {self._max_backlog} entries "
                    f"for {self._max_wait:.0f}s"
                )
            time.sleep(self._poll_interval)
