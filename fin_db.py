"""
本地财务数据库模块（SQLite）
替代逐只 akshare HTTP 请求，让 R3 筛选从 10-60 秒降到毫秒级

用法：
  fin_db.init_db()         # 首次初始化
  fin_db.build_snapshot()  # 盘前全量构建（需 2-5 分钟，后台运行）
  fin_db.batch_query(codes) # 毫秒级批量查询
  fin_db.get_stats()       # 查看数据库状态
"""
import sqlite3, os, time, datetime, json, threading, concurrent.futures
import sys, io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import akshare as ak
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), 'financial.db')
BUILD_LOCK = threading.Lock()
_is_building = False

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS financial (
            code TEXT PRIMARY KEY,
            roe REAL,
            net_profit_growth REAL,
            revenue_growth REAL,
            gross_margin REAL,
            debt_ratio REAL,
            report_date TEXT,
            updated_at TEXT
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_roe ON financial(roe)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_net_profit ON financial(net_profit_growth)')
    conn.commit()
    conn.close()
    print('[fin_db] 数据库初始化完成')


def _safe_float(v, default=None):
    """安全转浮点。注意：增长率/负债率可以合法为 0%，不再过滤 0 值。"""
    try:
        if v is None or str(v).strip() in ('--', 'nan', 'None', 'NaN', '-', ''):
            return default
        s = str(v).replace('%', '').replace(',', '').strip()
        return float(s)
    except:
        return default


def _fetch_one_financial(code: str) -> dict:
    """获取单只股票财务数据（akshare 同花顺）"""
    result = {}
    for indicator in ["按报告期", "按年度"]:
        try:
            df = ak.stock_financial_abstract_ths(symbol=code, indicator=indicator)
            if df is None or len(df) == 0:
                continue
            row = df.iloc[-1]  # 最新一期数据

            # ROE
            roe_col = next((c for c in df.columns if '净资产收益率' in c and '摊薄' not in c), None)
            if roe_col and result.get('roe') is None:
                result['roe'] = _safe_float(row.get(roe_col))

            # 净利润增长率（ak 实际列名：净利润同比增长率）
            for kw in ['净利润同比增长率', '归母净利润同比增长率', '净利润增长率', '归母净利润增长率']:
                col = next((c for c in df.columns if kw in c), None)
                if col and result.get('net_profit_growth') is None:
                    result['net_profit_growth'] = _safe_float(row.get(col))
                    break

            # 营收增长率（ak 实际列名：营业总收入同比增长率）
            for kw in ['营业总收入同比增长率', '营业收入同比增长率', '营收增长率', '营业总收入增长率']:
                col = next((c for c in df.columns if kw in c), None)
                if col and result.get('revenue_growth') is None:
                    result['revenue_growth'] = _safe_float(row.get(col))
                    break

            # 毛利率
            for kw in ['毛利率', '销售毛利率']:
                col = next((c for c in df.columns if kw in c), None)
                if col and result.get('gross_margin') is None:
                    result['gross_margin'] = _safe_float(row.get(col))
                    break

            # 资产负债率
            for kw in ['资产负债率', '负债率']:
                col = next((c for c in df.columns if kw in c), None)
                if col and result.get('debt_ratio') is None:
                    result['debt_ratio'] = _safe_float(row.get(col))
                    break

            if result.get('roe') is not None:
                break
        except Exception:
            continue

    # 反转负债率（akshare 可能返回正值，我们需要实际负债率）
    if result.get('debt_ratio') is not None and result['debt_ratio'] > 100:
        result['debt_ratio'] = result['debt_ratio'] / 100 if result['debt_ratio'] < 10000 else None

    return result


def build_snapshot(codes: list = None, max_workers=50, progress_callback=None, incremental=True):
    """
    全量构建财务数据库快照（后台批处理）
    
    参数:
        codes: 股票代码列表，不传则自动从行情接口获取全量
        max_workers: 并发线程数（50线程下 4000 只约 2-3 分钟）
        progress_callback: 进度回调 fn(pct, msg)
    """
    global _is_building
    with BUILD_LOCK:
        if _is_building:
            return {'success': False, 'error': '正在构建中，请稍后'}
        _is_building = True

    def log(msg):
        print(f'[fin_db] {msg}')
        if progress_callback:
            progress_callback(msg)

    try:
        init_db()
        t0 = time.time()

        # 获取代码列表
        if not codes:
            # 用 akshare 获取沪深A股代码列表，多源兜底
            codes = []
            errors = []

            # 沪市主板 + 科创板
            try:
                df_sh = ak.stock_info_sh_name_code(symbol='主板A股')
                codes.extend(df_sh['证券代码'].astype(str).str.zfill(6).tolist())
            except Exception as e:
                errors.append(f'沪市主板: {e}')

            try:
                df_kc = ak.stock_info_sh_name_code(symbol='科创板')
                codes.extend(df_kc['证券代码'].astype(str).str.zfill(6).tolist())
            except Exception as e:
                errors.append(f'科创板: {e}')

            # 深市主板 + 创业板 + 科创板
            for symbol in ['A股列表', '主板', '创业板', '科创板']:
                try:
                    df_sz = ak.stock_info_sz_name_code(symbol=symbol)
                    col = 'A股代码' if 'A股代码' in df_sz.columns else df_sz.columns[0]
                    codes.extend(df_sz[col].astype(str).str.zfill(6).tolist())
                except Exception as e:
                    errors.append(f'深市{symbol}: {e}')

            # 去重 + 过滤：6位纯数字，6/0/3 开头（沪深A股）
            seen = set()
            unique = []
            for c in codes:
                if c not in seen and c.isdigit() and len(c) == 6 \
                        and (c[0] in ('6', '0', '3')):
                    seen.add(c)
                    unique.append(c)
            codes = unique

            if errors:
                log(f'代码获取部分失败: {errors}')
            log(f'akshare 获取代码 {len(codes)} 只')

        # 增量模式：跳过已在数据库中的股票
        if incremental:
            existing = batch_query(codes)
            if existing:
                before = len(codes)
                codes = [c for c in codes if c not in existing]
                log(f'增量模式: {before}只全量, {len(existing)}只已有, 需补 {len(codes)}只')

        if not codes:
            _is_building = False
            return {'success': False, 'error': '无法获取股票代码列表'}

        total = len(codes)
        log(f'开始构建财务快照，共 {total} 只，{max_workers} 并发线程...')

        completed = [0]
        lock = threading.Lock()
        batch_data = {}

        def fetch_one(code):
            try:
                fin = _fetch_one_financial(code)
                if fin:
                    with lock:
                        batch_data[code] = fin
                with lock:
                    completed[0] += 1
                    if completed[0] % 200 == 0:
                        pct = int(completed[0] / total * 100)
                        log(f'进度: {completed[0]}/{total} ({pct}%)')
                        if progress_callback:
                            progress_callback(f'财务快照: {completed[0]}/{total} ({pct}%)')
            except Exception as e:
                with lock:
                    completed[0] += 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            list(ex.map(fetch_one, codes))

        # 批量写入 SQLite
        if batch_data:
            conn = sqlite3.connect(DB_PATH)
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            inserted = 0
            for code, fin in batch_data.items():
                try:
                    conn.execute('''
                        INSERT OR REPLACE INTO financial 
                        (code, roe, net_profit_growth, revenue_growth, gross_margin, debt_ratio, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (code, fin.get('roe'), fin.get('net_profit_growth'),
                          fin.get('revenue_growth'), fin.get('gross_margin'),
                          fin.get('debt_ratio'), now))
                    inserted += 1
                except:
                    pass
            conn.commit()
            conn.close()
            log(f'写入 {inserted} 条记录到数据库')

        elapsed = time.time() - t0
        stats = get_stats()
        log(f'财务快照构建完成！共 {stats["count"]} 只，耗时 {elapsed:.1f}s')

        _is_building = False
        return {
            'success': True,
            'count': stats['count'],
            'elapsed': round(elapsed, 1),
            'updated_at': stats['latest_update']
        }

    except Exception as e:
        _is_building = False
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}


def batch_query(codes: list) -> dict:
    """
    从本地数据库批量查询财务数据 → 毫秒级
    
    返回: {code: {roe, net_profit_growth, revenue_growth, gross_margin, debt_ratio}}
    """
    if not codes:
        return {}
    try:
        conn = sqlite3.connect(DB_PATH)
        placeholders = ','.join(['?' for _ in codes])
        rows = conn.execute(
            f'SELECT code, roe, net_profit_growth, revenue_growth, gross_margin, debt_ratio '
            f'FROM financial WHERE code IN ({placeholders})', codes
        ).fetchall()
        conn.close()
        result = {}
        for row in rows:
            result[row[0]] = {
                'roe': row[1],
                'net_profit_growth': row[2],
                'revenue_growth': row[3],
                'gross_margin': row[4],
                'debt_ratio': row[5],
            }
        return result
    except Exception as e:
        print(f'[fin_db] batch_query error: {e}')
        return {}



def upsert(code: str, fin: dict, updated_at: str = None):
    """写入单条财务数据（供app.py get_financial()回调）"""
    if not updated_at:
        import datetime as _dt
        updated_at = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""INSERT OR REPLACE INTO financial 
            (code, roe, net_profit_growth, revenue_growth, gross_margin, debt_ratio, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (code, fin.get("roe"), fin.get("net_profit_growth"),
              fin.get("revenue_growth"), fin.get("gross_margin"),
              fin.get("debt_ratio"), updated_at))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"[fin_db] upsert error for {code}: {e}")
        return False

def get_stats() -> dict:
    """获取数据库统计信息"""
    try:
        if not os.path.exists(DB_PATH):
            return {'count': 0, 'latest_update': None, 'exists': False}
        conn = sqlite3.connect(DB_PATH)
        count = conn.execute('SELECT COUNT(*) FROM financial').fetchone()[0]
        latest = conn.execute('SELECT MAX(updated_at) FROM financial').fetchone()[0]
        conn.close()
        return {'count': count, 'latest_update': latest, 'exists': True}
    except:
        return {'count': 0, 'latest_update': None, 'exists': False}


def is_stale(hours=24) -> bool:
    """检查数据库是否过期（默认超过24小时视为过期）"""
    stats = get_stats()
    if not stats['exists'] or not stats['latest_update']:
        return True
    try:
        updated = datetime.datetime.strptime(stats['latest_update'], '%Y-%m-%d %H:%M:%S')
        return (datetime.datetime.now() - updated).total_seconds() > hours * 3600
    except:
        return True


# 启动时自动初始化
init_db()
