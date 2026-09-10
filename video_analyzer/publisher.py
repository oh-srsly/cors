import asyncio
import os

from nats.js import JetStreamContext
from nats.js.api import DiscardPolicy, RetentionPolicy, StreamConfig
from nats.js.errors import APIError

from video_analyzer.frames import Frame

SUBJECT = "frames"
MAX_BACKLOG = int(os.environ.get("MAX_BACKLOG", "500"))
MAX_WAIT_SECONDS = 60
POLL_SECONDS = 0.05
STREAM_FULL = 10077  # generic store-failed code; with only max_msgs set it means full

STREAM = StreamConfig(
    name=SUBJECT,
    subjects=[SUBJECT],
    max_msgs=MAX_BACKLOG,
    discard=DiscardPolicy.NEW,
    retention=RetentionPolicy.WORK_QUEUE,
)


async def publish(js: JetStreamContext, video_id: str, frame: Frame) -> None:
    headers = {"video_id": video_id, "frame_id": str(frame.index)}
    for _ in range(int(MAX_WAIT_SECONDS / POLL_SECONDS)):
        try:
            await js.publish(SUBJECT, frame.jpeg, headers=headers)
            return
        except APIError as exc:
            if exc.err_code != STREAM_FULL:
                raise
            await asyncio.sleep(POLL_SECONDS)
    raise TimeoutError(
        f"stream stayed full at {MAX_BACKLOG} frames for {MAX_WAIT_SECONDS}s"
    )
