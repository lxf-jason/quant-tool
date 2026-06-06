"""
Vercel Serverless Function 入口
将 Flask WSGI app 适配为 Vercel serverless handler
"""
import sys
import os
import io
import urllib.parse

# 将项目根目录加入 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app

def handler(request):
    """Vercel serverless 入口函数"""
    # 构造 WSGI environ
    method = request.get('method', 'GET')
    path = request.get('path', '/')
    query = request.get('query', {})
    headers = request.get('headers', {})
    body = request.get('body', '')

    query_string = urllib.parse.urlencode(query) if query else ''

    environ = {
        'REQUEST_METHOD': method,
        'PATH_INFO': urllib.parse.unquote(path),
        'QUERY_STRING': query_string,
        'SERVER_NAME': 'localhost',
        'SERVER_PORT': '443',
        'HTTP_HOST': headers.get('host', 'localhost'),
        'SERVER_PROTOCOL': 'HTTP/1.1',
        'wsgi.version': (1, 0),
        'wsgi.url_scheme': headers.get('x-forwarded-proto', 'https'),
        'wsgi.input': io.BytesIO(body.encode() if body else b''),
        'wsgi.errors': sys.stderr,
        'wsgi.multithread': False,
        'wsgi.multiprocess': False,
        'wsgi.run_once': True,
    }

    # 添加 HTTP 头
    for key, value in headers.items():
        key = key.upper().replace('-', '_')
        if key not in ('CONTENT_TYPE', 'CONTENT_LENGTH'):
            key = 'HTTP_' + key
        environ[key] = value

    # WSGI 响应收集
    status = None
    response_headers = []
    body_parts = []

    def start_response(status_line, headers_list):
        nonlocal status, response_headers
        status = status_line
        response_headers = headers_list
        return body_parts.append

    # 调用 Flask app
    response = app(environ, start_response)
    for part in response:
        if part:
            body_parts.append(part)

    # 合并响应体
    response_body = b''.join(body_parts)

    # 解析状态码
    status_code = int(status.split(' ', 1)[0]) if status else 200

    # 构建 Vercel 响应格式
    response_dict = {
        'statusCode': status_code,
        'headers': {k: v for k, v in response_headers},
        'body': response_body.decode('utf-8', errors='replace'),
    }

    return response_dict
