# A股量化选股工具 v5.2 银级

## 项目简介
基于 Python + Flask 的 A股量化选股工具，内置 8 套选股策略，支持实时行情、财务数据、自选股管理。

## 技术栈
- **后端**: Python 3.11 + Flask
- **数据源**: akshare（A股实时行情 + 财务数据）
- **前端**: 原生 HTML/CSS/JS（深色主题）
- **数据库**: SQLite（本地财务缓存）

## 文件说明
| 文件 | 说明 |
|------|------|
| `app.py` | Flask 后端 API（1300+ 行，核心逻辑） |
| `fin_db.py` | 本地财务数据库模块 |
| `static/index.html` | 前端界面（深色主题 SPA） |
| `strategies.json` | 8 套预设选股策略配置 |
| `filter_config.json` | 筛选条件配置 |
| `watchlist.json` | 自选股列表 |
| `requirements.txt` | Python 依赖 |
| `Dockerfile` | Docker 部署配置 |

## 本地运行
```bash
pip install -r requirements.txt
python app.py
# 访问 http://localhost:5678
```

## Docker 部署
```bash
docker build -t quant-tool .
docker run -p 7860:7860 quant-tool
# 访问 http://localhost:7860
```

## 部署到公网
请将此项目部署到任意支持 Docker 或 Python 的云平台：
- **Hugging Face Spaces**（Docker 模式，推荐）
- **Railway.app**（Docker 部署）
- **Fly.io**（Docker 部署）
- **Render.com**（Docker 部署）
- **阿里云/腾讯云**（国内服务器推荐）

### Hugging Face Spaces
1. 创建 Space → 选 Docker SDK
2. 关联此 GitHub 仓库
3. 自动构建部署，端口 7860

### Railway / Fly.io
```bash
# 安装 CLI 后
railway init
railway up
```

## 注意事项
- `akshare` 数据源为国内金融网站，海外服务器可能访问受限
- 如部署在海外，建议使用国内云服务器（阿里云/腾讯云）
- 免费部署方案推荐国内平台，数据拉取更稳定
