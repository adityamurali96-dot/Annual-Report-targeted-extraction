FROM python:3.11-slim

# Install system dependencies required by pdf2image (poppler-utils)
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

# Gunicorn with 300s timeout for large/scanned PDF processing
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "300", "--workers", "2", "app:app"]
