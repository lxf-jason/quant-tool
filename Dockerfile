FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖（akshare 需要的部分库）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl \
    && rm -rf /var/lib/apt/lists/*

# 安装 Python 依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY app.py .
COPY fin_db.py .
COPY strategies.json .
COPY filter_config.json .
COPY watchlist.json .
COPY static/ ./static/

# 暴露端口（Hugging Face 默认 7860）
EXPOSE 7860

# 启动命令
CMD ["gunicorn", "app:app", "--host", "0.0.0.0", "--port", "7860", "--workers", "1", "--threads", "4", "--timeout", "120"]
