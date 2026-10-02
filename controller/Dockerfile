# Build from the repository root: docker build -f controller/Dockerfile .
FROM python:3.11-slim

WORKDIR /app

COPY controller/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/*.py ./
COPY controller/*.py ./

ENV PYTHONUNBUFFERED=1

CMD ["python3", "local_controller.py"]
