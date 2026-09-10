import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import cv2

log = logging.getLogger("video_analyzer")


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
        step = source_fps / target_fps
        sample = 0
        index = 0
        # grab() alone tells EOF apart from a frame that fails to decode in retrieve()
        while capture.grab():
            if index >= sample * step:
                sample += 1
                ok, image = capture.retrieve()
                if ok:
                    _, buffer = cv2.imencode(
                        ".jpg", image, [cv2.IMWRITE_JPEG_QUALITY, 75]
                    )
                    yield Frame(index=index, jpeg=buffer.tobytes())
                else:
                    log.warning(
                        "skipping undecodable frame=%d file=%s", index, path.name
                    )
            index += 1
    finally:
        capture.release()
