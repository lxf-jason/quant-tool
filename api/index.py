"""
Vercel Serverless Function 入口
使用 vercel-wsgi 桥接 Flask WSGI app
"""
import sys
import os
import traceback

# 将项目根目录加入 Python 路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# 导入 vercel-wsgi 桥接器
from vercel_wsgi import wsgi_app as vercel_handler

# 导入 Flask app（延迟导入，确保路径先设置好）
from app import app

# Vercel serverless 入口：将 WSGI app 转为 Vercel 格式
handler = vercel_handler(app)
