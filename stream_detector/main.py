import logging
import os
import signal
import socket
import time
from types import FrameType

import cv2
import numpy as np
import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

from stream_detector.detector import StreamFaceDetector
from stream_detector.detector_response_handling import (
    RespObject,
    send_results_next_service,
)

logging.basicConfig(level="INFO", format="%(levelname)s %(message)s")
log = logging.getLogger("stream_detector")

STREAM = "frames"
GROUP = "detectors"
BLOCK_MS = 1000
CLAIM_IDLE_MS = (
    60_000  # entries are read one at a time, so this bounds one detect + send
)
RETRY_SECONDS = 2

StreamEntry = tuple[bytes, dict[bytes, bytes]]


class FrameConsumer:
    def __init__(
        self, client: redis.Redis, detector: StreamFaceDetector, consumer: str
    ) -> None:
        self._client = client
        self._detector = detector
        self._consumer = consumer
        self._stopping = False
        self._next_claim_at = 0.0

    def ensure_group(self) -> None:
        try:
            self._client.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)
        log.info("consumer=%s listening on %s/%s", self._consumer, STREAM, GROUP)
        while not self._stopping:
            try:
                self.ensure_group()
                while not self._stopping:
                    self.process_once()
            except redis.RedisError:
                log.exception("redis error, retrying in %ds", RETRY_SECONDS)
                time.sleep(RETRY_SECONDS)

    def process_once(self) -> int:
        entries = self._claim_stale() or self._read_new()
        for entry_id, fields in entries:
            self._handle(entry_id, fields)
            pipe = self._client.pipeline()
            pipe.xack(STREAM, GROUP, entry_id)
            pipe.xdel(STREAM, entry_id)
            pipe.execute()
        return len(entries)

    def _read_new(self) -> list[StreamEntry]:
        response = self._client.xreadgroup(
            GROUP, self._consumer, {STREAM: ">"}, count=1, block=BLOCK_MS
        )
        return response[0][1] if response else []

    def _claim_stale(self) -> list[StreamEntry]:
        now = time.monotonic()
        if now < self._next_claim_at:
            return []
        self._next_claim_at = now + CLAIM_IDLE_MS / 4000
        response = self._client.xautoclaim(
            STREAM, GROUP, self._consumer, min_idle_time=CLAIM_IDLE_MS, count=1
        )
        return response[1]

    def _handle(self, entry_id: bytes, fields: dict[bytes, bytes]) -> None:
        try:
            video_id = fields[b"video_id"].decode()
            frame_id = int(fields[b"frame_id"])
            image = cv2.imdecode(
                np.frombuffer(fields[b"jpeg"], np.uint8), cv2.IMREAD_COLOR
            )
            if image is None:
                raise ValueError("undecodable jpeg")
            faces = self._detector.detect_faces(image)
            send_results_next_service(
                [RespObject(faces=faces, video_id=video_id, frame_id=frame_id)]
            )
        except Exception:
            log.exception("dropping entry=%s", entry_id.decode())

    def _request_stop(self, signum: int, _frame: FrameType | None) -> None:
        self._stopping = True


def main() -> None:
    client = redis.Redis.from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        socket_timeout=BLOCK_MS / 1000 + 1,
        retry=Retry(ExponentialBackoff(), 3),
    )
    FrameConsumer(client, StreamFaceDetector(), socket.gethostname()).run()


if __name__ == "__main__":
    main()
