FROM python:3.11-slim

WORKDIR /app

# Install system deps for akshare/numpy compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ make libc-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py fin_db.py strategies.json filter_config.json watchlist.json ./
COPY static/ ./static/

EXPOSE 5678

CMD ["gunicorn", "--bind", "0.0.0.0:5678", "--workers", "2", "--timeout", "120", "app:app"]
