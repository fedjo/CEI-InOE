FROM python:3.12-slim

WORKDIR /app

COPY ingestor/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ /app/shared/
RUN pip install --no-cache-dir -e /app/shared/  # Install shared code as a package

COPY ingestor/conf/ /app/conf/
COPY ingestor/app/ .

CMD ["python", "main.py"]
