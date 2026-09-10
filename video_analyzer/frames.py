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
    capture = cv2.VideoCapture(str(path))
    try:
        source_fps = capture.get(cv2.CAP_PROP_FPS)
        if not capture.isOpened() or not source_fps > 0:
            raise UnreadableVideoError(f"cannot read video: {path.name}")
        step = source_fps / target_fps  # 25 fps -> 2 fps: every 12.5th frame
        sample = 0
        index = 0
        # grab() alone tells EOF apart from a frame that fails to decode in retrieve()
        while capture.grab():
            if index >= sample * step:
                frame = _encode(capture, index)
                if frame is None:
                    log.warning(
                        "skipping undecodable frame=%d file=%s", index, path.name
                    )
                else:
                    yield frame
                sample += 1
            index += 1
    finally:
        capture.release()


def _encode(capture: cv2.VideoCapture, index: int) -> Frame | None:
    ok, image = capture.retrieve()
    if not ok:
        return None
    ok, buffer = cv2.imencode(".jpg", image, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
    return Frame(index=index, jpeg=buffer.tobytes()) if ok else None
