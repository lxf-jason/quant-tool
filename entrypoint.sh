#!/bin/bash
set -e

echo "=========================================="
echo "  A股量化选股工具 v5.2 - 启动中..."
echo "=========================================="

# Check required files exist
for f in app.py fin_db.py static/index.html strategies.json; do
    if [ ! -f "/app/$f" ]; then
        echo "ERROR: Missing required file: /app/$f"
        exit 1
    fi
done

echo "[OK] All required files present"

# Test import
python -c "
import flask, requests, pandas, numpy, akshare, gunicorn
print('[OK] All Python dependencies loaded')
" 2>&1 || {
    echo "[ERROR] Python dependency check failed"
    exit 1
}

echo "[OK] Starting gunicorn on port 5678..."
exec gunicorn --bind 0.0.0.0:5678 --workers 2 --timeout 120 --access-logfile - --error-logfile - app:app
