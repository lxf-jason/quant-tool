"""
Vercel Serverless Function 入口
使用 asgiref WsgiToAsgi 将 Flask WSGI 转为 ASGI 供 Vercel Python Runtime 调用
"""
import sys
import os

# 将项目根目录加入 Python 路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

# 导入 Flask app
from app import app

# 将 WSGI 转为 ASGI（Vercel Python Runtime 期望 ASGI）
from asgiref.wsgi import WsgiToAsgi

handler = WsgiToAsgi(app)
