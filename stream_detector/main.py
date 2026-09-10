import logging
import os
import signal
import socket
import time
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

logging.basicConfig(level="INFO", format="%(levelname)s %(message)s")
log = logging.getLogger("stream_detector")

STREAM = "frames"
GROUP = "detectors"
BATCH_SIZE = 16
BLOCK_MS = 1000
CLAIM_IDLE_MS = 60_000  # must exceed one inference; entries are acked one at a time
RETRY_SECONDS = 2

StreamEntry = tuple[bytes, dict[bytes, bytes]]


class FrameConsumer:
    def __init__(
        self,
        client: redis.Redis,
        detector: StreamFaceDetector,
        consumer: str,
        claim_idle_ms: int = CLAIM_IDLE_MS,
    ) -> None:
        self._client = client
        self._detector = detector
        self._consumer = consumer
        self._claim_idle_ms = claim_idle_ms
        self._stopping = False

    def ensure_group(self) -> None:
        try:
            self._client.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)
        log.info("consumer %s listening on %s/%s", self._consumer, STREAM, GROUP)
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
            result = self._detect(entry_id, fields)
            if result is not None:
                send_results_next_service([result])
            pipe = self._client.pipeline()
            pipe.xack(STREAM, GROUP, entry_id)
            pipe.xdel(STREAM, entry_id)
            pipe.execute()
        if entries:
            log.info("processed %d frames", len(entries))
        return len(entries)

    def _read_new(self) -> list[StreamEntry]:
        response: Any = self._client.xreadgroup(
            GROUP, self._consumer, {STREAM: ">"}, count=BATCH_SIZE, block=BLOCK_MS
        )
        return response[0][1] if response else []

    def _claim_stale(self) -> list[StreamEntry]:
        response: Any = self._client.xautoclaim(
            STREAM,
            GROUP,
            self._consumer,
            min_idle_time=self._claim_idle_ms,
            count=BATCH_SIZE,
        )
        return response[1]

    def _detect(self, entry_id: bytes, fields: dict[bytes, bytes]) -> RespObject | None:
        try:
            video_id = fields[b"video_id"].decode()
            frame_id = int(fields[b"frame_id"])
            image = cv2.imdecode(
                np.frombuffer(fields[b"jpeg"], np.uint8), cv2.IMREAD_COLOR
            )
            if image is None:
                raise ValueError("undecodable jpeg")
            faces = self._detector.detect_faces(image)
        except Exception:  # noqa: BLE001 - one bad frame must not take the worker down
            log.exception("dropping entry %s", entry_id.decode())
            return None
        return RespObject(faces=faces, video_id=video_id, frame_id=frame_id)

    def _request_stop(self, signum: int, _frame: FrameType | None) -> None:
        self._stopping = True


def main() -> None:
    client = redis.Redis.from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        socket_timeout=BLOCK_MS / 1000 + 1,
    )
    FrameConsumer(client, StreamFaceDetector(), socket.gethostname()).run()


if __name__ == "__main__":
    main()
