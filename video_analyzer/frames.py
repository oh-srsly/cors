from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

import cv2


class UnreadableVideoError(Exception):
    pass


@dataclass(frozen=True)
class Frame:
    index: int
    jpeg: bytes


class VideoSource:
    def __init__(self, path: Path, jpeg_quality: int = 90) -> None:
        self._capture = cv2.VideoCapture(str(path))
        if not self._capture.isOpened():
            raise UnreadableVideoError(f"cannot open video: {path}")
        self.fps: float = self._capture.get(cv2.CAP_PROP_FPS)
        if not self.fps > 0:
            self.close()
            raise UnreadableVideoError(f"cannot determine frame rate: {path}")
        self._encode_params = [cv2.IMWRITE_JPEG_QUALITY, jpeg_quality]

    def frames(self, target_fps: int) -> Iterator[Frame]:
        """Frames sampled by timestamp, so 25 -> 2 fps stays accurate."""
        step = self.fps / target_fps
        sample = 0
        next_wanted = 0
        index = 0
        # grab() only demuxes; retrieve() decodes selected frames only.
        while self._capture.grab():
            if index >= next_wanted:
                ok, image = self._capture.retrieve()
                if not ok:
                    raise UnreadableVideoError(f"failed to decode frame {index}")
                ok, buffer = cv2.imencode(".jpg", image, self._encode_params)
                if not ok:
                    raise UnreadableVideoError(f"failed to encode frame {index}")
                yield Frame(index=index, jpeg=buffer.tobytes())
                while next_wanted <= index:
                    sample += 1
                    next_wanted = int(sample * step)
            index += 1

    def close(self) -> None:
        self._capture.release()

    def __enter__(self) -> "VideoSource":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
