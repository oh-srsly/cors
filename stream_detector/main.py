import asyncio
import logging
import os

import cv2
import nats
import numpy as np
from nats.aio.msg import Msg
from nats.js.api import ConsumerConfig
from prometheus_client import Counter, Histogram, start_http_server

from stream_detector.detector import StreamFaceDetector, make_detector
from stream_detector.detector_response_handling import (
    RespObject,
    send_results_next_service,
)

logging.basicConfig(level="INFO", format="%(levelname)s %(message)s")
log = logging.getLogger("stream_detector")

SUBJECT = "frames"
CONSUMER = "detectors"
ACK_WAIT_SECONDS = 60  # must exceed one detect + send
RETRY_SECONDS = 2

FRAMES_PROCESSED = Counter("frames_processed_total", "Frames detected and forwarded")
FRAMES_DROPPED = Counter("frames_dropped_total", "Frames dropped after a failure")
DETECT_SECONDS = Histogram("detect_seconds", "detect_faces latency")


async def handle(msg: Msg, detector: StreamFaceDetector) -> None:
    try:
        video_id = msg.headers["video_id"]
        frame_id = int(msg.headers["frame_id"])
        image = cv2.imdecode(np.frombuffer(msg.data, np.uint8), cv2.IMREAD_COLOR)
        if image is None:
            raise ValueError("undecodable jpeg")
        with DETECT_SECONDS.time():
            faces = await asyncio.to_thread(detector.detect_faces, image)
        send_results_next_service(
            [RespObject(faces=faces, video_id=video_id, frame_id=frame_id)]
        )
    except Exception:
        FRAMES_DROPPED.inc()
        log.exception("dropping frame headers=%s", msg.headers)
        await msg.term()
        return
    FRAMES_PROCESSED.inc()
    await msg.ack()


async def consume(detector: StreamFaceDetector) -> None:
    nc = await nats.connect(os.environ.get("NATS_URL", "nats://localhost:4222"))
    try:
        js = nc.jetstream()
        sub = await js.pull_subscribe(
            SUBJECT, durable=CONSUMER, config=ConsumerConfig(ack_wait=ACK_WAIT_SECONDS)
        )
        log.info("listening on %s", SUBJECT)
        while True:
            try:
                msgs = await sub.fetch(1, timeout=1)
            except nats.errors.TimeoutError:
                continue
            for msg in msgs:
                await handle(msg, detector)
    finally:
        await nc.close()


async def main() -> None:
    detector = make_detector(os.environ.get("DETECTOR", "mock"))
    start_http_server(9100)
    while True:
        try:
            await consume(detector)
        except nats.errors.Error as exc:
            log.warning("nats: %s, retrying in %ds", exc, RETRY_SECONDS)
            await asyncio.sleep(RETRY_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
