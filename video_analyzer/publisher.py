import os
import time

import redis
from prometheus_client import Counter

from video_analyzer.frames import Frame

STREAM = "frames"
MAX_BACKLOG = int(os.environ.get("MAX_BACKLOG", "500"))
MAX_WAIT_SECONDS = 60
POLL_SECONDS = 0.05

BACKLOG_WAIT_SECONDS = Counter(
    "backlog_wait_seconds_total", "Time spent waiting on backlog"
)


def publish(client: redis.Redis, video_id: str, frame: Frame) -> None:
    pipe = client.pipeline(transaction=False)
    pipe.xadd(
        STREAM, {"video_id": video_id, "frame_id": frame.index, "jpeg": frame.jpeg}
    )
    pipe.xlen(STREAM)
    _, backlog = pipe.execute()
    if backlog >= MAX_BACKLOG:
        wait_for_capacity(client)


def wait_for_capacity(client: redis.Redis) -> None:
    for _ in range(int(MAX_WAIT_SECONDS / POLL_SECONDS)):
        # consumers delete entries once processed, so length == unprocessed backlog
        if client.xlen(STREAM) < MAX_BACKLOG:
            return
        time.sleep(POLL_SECONDS)
        BACKLOG_WAIT_SECONDS.inc(POLL_SECONDS)
    raise TimeoutError(f"backlog stayed above {MAX_BACKLOG} for {MAX_WAIT_SECONDS}s")
