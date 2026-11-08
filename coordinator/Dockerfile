FROM python:3.12-slim

WORKDIR /app

COPY coordinator/requirements.txt .

RUN pip install --no-cache-dir --default-timeout=120 --retries=10 -r requirements.txt

COPY coordinator/ .

EXPOSE 5000

CMD ["python", "app.py"]