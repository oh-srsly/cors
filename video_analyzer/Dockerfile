FROM python:3.12-slim
WORKDIR /app
COPY video_analyzer/requirements.txt video_analyzer/
RUN pip install --no-cache-dir -r video_analyzer/requirements.txt
COPY video_analyzer/ video_analyzer/
EXPOSE 8000
CMD ["uvicorn", "video_analyzer.main:app", "--host", "0.0.0.0", "--port", "8000"]
