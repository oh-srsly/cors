from pydantic import BaseModel

from stream_detector.detector import BoundingBox


class RespObject(BaseModel):
    faces: list[BoundingBox]
    video_id: str
    frame_id: int


def send_results_next_service(results: list[RespObject]) -> None:
    """
    You can assume that this function sends the results to the next
    Service in the pipeline.
    """
    pass
