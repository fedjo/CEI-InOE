FROM python:3.12-slim

WORKDIR /app

COPY ingestor/requirements.txt .
COPY shared/ /app/shared/

RUN pip install --no-cache-dir -r requirements.txt

COPY ingestor/app/ .

CMD ["python", "main.py"]
