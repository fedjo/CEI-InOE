FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY api/requirements.txt .
COPY shared/ /app/shared/

RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY api/app/ ./app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
