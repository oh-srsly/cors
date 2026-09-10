import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import cv2

log = logging.getLogger("video_analyzer")

JPEG_QUALITY = 75


class UnreadableVideoError(Exception):
    pass


@dataclass(frozen=True)
class Frame:
    index: int
    jpeg: bytes


def iter_frames(path: Path, target_fps: int) -> Iterator[Frame]:
    """Sample by presentation time, so 25 -> 2 fps and VFR sources stay accurate."""
    capture = cv2.VideoCapture(str(path))
    try:
        if not capture.isOpened():
            raise UnreadableVideoError(f"cannot open video: {path.name}")
        source_fps = capture.get(cv2.CAP_PROP_FPS)
        if not source_fps > 0:
            raise UnreadableVideoError(f"cannot determine frame rate: {path.name}")
        interval_ms = 1000 / target_fps
        next_wanted_ms = 0.0
        index = 0
        # grab() alone tells EOF apart from a frame that fails to decode in retrieve()
        while capture.grab():
            timestamp_ms = (
                capture.get(cv2.CAP_PROP_POS_MSEC) or index * 1000 / source_fps
            )
            if timestamp_ms >= next_wanted_ms:
                frame = _encode(capture, index)
                if frame is None:
                    log.warning("skipping undecodable frame %d of %s", index, path.name)
                else:
                    yield frame
                while next_wanted_ms <= timestamp_ms:
                    next_wanted_ms += interval_ms
            index += 1
    finally:
        capture.release()


def _encode(capture: cv2.VideoCapture, index: int) -> Frame | None:
    ok, image = capture.retrieve()
    if not ok:
        return None
    ok, buffer = cv2.imencode(".jpg", image, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
    return Frame(index=index, jpeg=buffer.tobytes()) if ok else None
