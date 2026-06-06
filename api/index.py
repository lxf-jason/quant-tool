"""
Vercel Serverless Function 入口
将 Flask WSGI app 适配为 Vercel serverless handler
"""
import sys
import os
import io
import traceback
import urllib.parse

# 将项目根目录加入 Python 路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

def _error_response(message, detail=""):
    return {
        "statusCode": 500,
        "headers": {"Content-Type": "text/plain; charset=utf-8"},
        "body": f"Error: {message}\n{detail}",
    }

def handler(request):
    """Vercel serverless 入口函数"""
    try:
        # 延迟导入，确保路径已设置
        from app import app

        # 解析请求
        if hasattr(request, 'method'):
            method = request.method
        else:
            method = request.get('method', 'GET') if isinstance(request, dict) else 'GET'

        if hasattr(request, 'url'):
            url = request.url
            parsed = urllib.parse.urlparse(url)
            path = parsed.path
            query_string = parsed.query
        else:
            path = request.get('path', '/') if isinstance(request, dict) else '/'
            query = request.get('query', {}) if isinstance(request, dict) else {}
            query_string = urllib.parse.urlencode(query) if query else ''

        if hasattr(request, 'headers'):
            headers = dict(request.headers) if hasattr(request.headers, 'items') else {}
        else:
            headers = request.get('headers', {}) if isinstance(request, dict) else {}

        body = ""
        if hasattr(request, 'body'):
            try:
                body = request.body.decode() if isinstance(request.body, bytes) else str(request.body)
            except:
                body = str(request.body) if hasattr(request.body, '__str__') else ""
        elif isinstance(request, dict):
            body = request.get('body', '')

        # 构造 WSGI environ
        environ = {
            'REQUEST_METHOD': method,
            'PATH_INFO': urllib.parse.unquote(path),
            'QUERY_STRING': query_string,
            'SERVER_NAME': headers.get('host', 'localhost').split(':')[0],
            'SERVER_PORT': headers.get('host', 'localhost').split(':')[1] if ':' in headers.get('host', '') else '443',
            'SERVER_PROTOCOL': 'HTTP/1.1',
            'wsgi.version': (1, 0),
            'wsgi.url_scheme': headers.get('x-forwarded-proto', 'https'),
            'wsgi.input': io.BytesIO(body.encode('utf-8') if body else b''),
            'wsgi.errors': sys.stderr,
            'wsgi.multithread': False,
            'wsgi.multiprocess': False,
            'wsgi.run_once': True,
        }

        # 添加 HTTP 头到 environ
        for key, value in headers.items():
            key_upper = key.upper().replace('-', '_')
            if key_upper not in ('CONTENT_TYPE', 'CONTENT_LENGTH'):
                key_upper = 'HTTP_' + key_upper
            environ[key_upper] = value

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
        headers_dict = {}
        for k, v in response_headers:
            headers_dict[k] = v

        # 判断是否为二进制响应
        content_type = headers_dict.get('Content-Type', '')
        is_binary = 'image' in content_type or 'application/octet-stream' in content_type

        if is_binary:
            import base64
            return {
                'statusCode': status_code,
                'headers': headers_dict,
                'body': base64.b64encode(response_body).decode('ascii'),
                'isBase64Encoded': True,
            }
        else:
            return {
                'statusCode': status_code,
                'headers': headers_dict,
                'body': response_body.decode('utf-8', errors='replace'),
            }

    except Exception as e:
        tb = traceback.format_exc()
        return _error_response(str(e), tb)
