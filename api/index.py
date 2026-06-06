"""
Vercel Serverless Function 入口
将 Flask app 暴露给 Vercel 的 serverless runtime
"""
import sys
import os

# 将项目根目录加入 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app

# Vercel serverless 入口
handler = app
