FROM python:3.11-slim

WORKDIR /app

# Install system deps for akshare/numpy compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make libc-dev curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps (akshare pinned to avoid breaking changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY app.py fin_db.py strategies.json filter_config.json watchlist.json ./
COPY static/ ./static/

# Create data directory for financial.db
RUN mkdir -p /app/data

# Expose port
EXPOSE 5678

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:5678/ || exit 1

# Start with entrypoint script for better error reporting
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
