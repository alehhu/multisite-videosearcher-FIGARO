# Build from the repository root: docker build -f dispatcher/Dockerfile .
FROM python:3.11-slim

WORKDIR /app

COPY dispatcher/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/*.py ./
COPY dispatcher/dispatcher.py .

ENV PYTHONUNBUFFERED=1

CMD ["python3", "dispatcher.py"]
