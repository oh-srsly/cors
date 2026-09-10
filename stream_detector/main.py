import logging
import os
import signal
import socket
from collections.abc import Callable
from types import FrameType
from typing import Any

import cv2
import numpy as np
import redis

from stream_detector.detector import StreamFaceDetector
from stream_detector.detector_response_handling import (
    RespObject,
    send_results_next_service,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"), format="%(levelname)s %(message)s"
)
log = logging.getLogger("stream_detector")

StreamEntry = tuple[bytes, dict[bytes, bytes]]


class FrameConsumer:
    """One worker in a Redis Streams consumer group; run several to scale out."""

    def __init__(
        self,
        client: redis.Redis,
        detector: StreamFaceDetector,
        stream: str,
        group: str,
        consumer: str,
        batch_size: int = 16,
        block_ms: int = 1000,
        claim_idle_ms: int = 60_000,
    ) -> None:
        self._client = client
        self._detector = detector
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._batch_size = batch_size
        # keep below redis-py's 5s default socket_timeout
        self._block_ms = block_ms
        self._claim_idle_ms = claim_idle_ms

    def ensure_group(self) -> None:
        try:
            self._client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def run(self, should_stop: Callable[[], bool]) -> None:
        self.ensure_group()
        log.info(
            "consumer %s listening on %s/%s", self._consumer, self._stream, self._group
        )
        while not should_stop():
            self.process_once()

    def process_once(self) -> int:
        entries = self._claim_stale() or self._read_new()
        if not entries:
            return 0
        results = [
            result for entry in entries if (result := self._detect(entry)) is not None
        ]
        if results:
            send_results_next_service(results)
        ids = [entry_id for entry_id, _ in entries]
        pipe = self._client.pipeline()
        pipe.xack(self._stream, self._group, *ids)
        pipe.xdel(self._stream, *ids)
        pipe.execute()
        log.info("processed %d frames, %d results forwarded", len(ids), len(results))
        return len(ids)

    def _read_new(self) -> list[StreamEntry]:
        response: Any = self._client.xreadgroup(
            self._group,
            self._consumer,
            {self._stream: ">"},
            count=self._batch_size,
            block=self._block_ms,
        )
        return response[0][1] if response else []

    def _claim_stale(self) -> list[StreamEntry]:
        """Take over entries a crashed consumer left pending."""
        response: Any = self._client.xautoclaim(
            self._stream,
            self._group,
            self._consumer,
            min_idle_time=self._claim_idle_ms,
            count=self._batch_size,
        )
        return response[1]

    def _detect(self, entry: StreamEntry) -> RespObject | None:
        entry_id, fields = entry
        try:
            video_id = fields[b"video_id"].decode()
            frame_id = int(fields[b"frame_id"])
            image = cv2.imdecode(
                np.frombuffer(fields[b"jpeg"], np.uint8), cv2.IMREAD_COLOR
            )
        except (KeyError, ValueError, UnicodeDecodeError):
            log.warning("dropping malformed entry %s", entry_id.decode())
            return None
        if image is None:
            log.warning("dropping undecodable frame %s of %s", frame_id, video_id)
            return None
        faces = self._detector.detect_faces(image)
        log.debug("%s frame %d: %d faces", video_id, frame_id, len(faces))
        return RespObject(faces=faces, video_id=video_id, frame_id=frame_id)


def main() -> None:
    stopping = False

    def request_stop(signum: int, _frame: FrameType | None) -> None:
        nonlocal stopping
        log.info("received signal %d, stopping after current batch", signum)
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    consumer = FrameConsumer(
        client=redis.Redis.from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        ),
        detector=StreamFaceDetector(),
        stream=os.environ.get("FRAMES_STREAM", "frames"),
        group=os.environ.get("CONSUMER_GROUP", "detectors"),
        consumer=os.environ.get("CONSUMER_NAME", socket.gethostname()),
        batch_size=int(os.environ.get("BATCH_SIZE", "16")),
    )
    consumer.run(should_stop=lambda: stopping)


if __name__ == "__main__":
    main()
