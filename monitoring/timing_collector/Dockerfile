FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt

COPY collector.py /app/collector.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "/app/collector.py"]
