FROM python:3.11-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer cache optimisation)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . /app

# Create data layer directories
RUN mkdir -p data/bronze data/silver data/gold data/reports data/quarantine

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default command: run the scraper
CMD ["python", "src/scraper/scraper.py", "--config", "src/scraper/config.yaml", "--out", "data/bronze"]
