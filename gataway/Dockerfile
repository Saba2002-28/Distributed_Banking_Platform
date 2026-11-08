FROM python:3.12-slim

WORKDIR /app

COPY gateway/requirements.txt .
RUN pip install --no-cache-dir --default-timeout=120 --retries=10 -r requirements.txt

COPY gateway/app.py .
COPY gateway/shard_router.py .
COPY config/ ./config/

EXPOSE 5000

CMD ["python", "app.py"]