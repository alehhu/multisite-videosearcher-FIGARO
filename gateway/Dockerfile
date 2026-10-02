FROM python:3.11-slim

WORKDIR /app

COPY gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/*.py ./
COPY gateway/gateway.py .

EXPOSE 8080

CMD ["python3", "gateway.py"]
