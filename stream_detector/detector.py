import cv2
import numpy as np
from pydantic import BaseModel


class BoundingBox(BaseModel):
    x: float
    y: float
    w: float
    h: float


class StreamFaceDetector:
    def __init__(self) -> None:
        pass

    def detect_faces(self, frame: np.ndarray) -> list[BoundingBox]:
        """
        This is a mock function, in reality this function
        would perform an heavy ML model inference.
        """
        return [
            BoundingBox(x=0, y=0, w=100, h=100),
            BoundingBox(x=100, y=100, w=100, h=100),
        ]


class HaarFaceDetector(StreamFaceDetector):
    def __init__(self) -> None:
        path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
        self._cascade = cv2.CascadeClassifier(path)
        if self._cascade.empty():
            raise RuntimeError(f"cascade not found: {path}")

    def detect_faces(self, frame: np.ndarray) -> list[BoundingBox]:
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        boxes = self._cascade.detectMultiScale(
            gray, scaleFactor=1.1, minNeighbors=5, minSize=(40, 40)
        )
        return [BoundingBox(x=x, y=y, w=w, h=h) for x, y, w, h in boxes]


def make_detector(name: str) -> StreamFaceDetector:
    if name == "haar":
        return HaarFaceDetector()
    if name == "mock":
        return StreamFaceDetector()
    raise ValueError(f"unknown DETECTOR {name!r}, expected mock or haar")
