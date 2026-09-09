FROM python:3.12-slim
WORKDIR /app
COPY stream_detector/requirements.txt stream_detector/
RUN pip install --no-cache-dir -r stream_detector/requirements.txt
COPY stream_detector/ stream_detector/
CMD ["python", "-m", "stream_detector.main"]
