"""
A股量化选股工具 - 后端 Flask API v5
数据来源:
  - 实时行情+PE/PB/换手率/市值: 腾讯行情 API（单次请求500只，全量10批并发，<2秒）
  - ROE/财务指标: akshare 同花顺（最近季报 > 最新年报）
  - 技术指标K线: 新浪财经日K线（仅对R1/R2/R3筛后候选股计算，大幅削减请求量）

v5 性能大改造:
  1. 行情+估值合并为单数据源（腾讯），一次请求同时获得价格/PE/PB/市值/换手率
  2. 腾讯接口每批500只，全量10批完全并发，<2秒拿到所有数据
  3. 彻底移除 baostock（串行C库，慢且不稳定）
  4. K线改用新浪财经接口（并发100线程，只对候选股计算），单只0.17s
  5. 筛选漏斗更严格（先价格→PE/PB→ROE→技术指标），层层过滤减少K线请求量
  6. 行情缓存TTL=60秒（盘中实时），估值/财务TTL=1小时
"""
import sys, os, json, time, copy, threading, datetime, traceback
import concurrent.futures

import io
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import requests
from flask import Flask, jsonify, request, send_from_directory
import akshare as ak
import pandas as pd
import fin_db  # 本地财务数据库，替代逐只HTTP查询

# 项目根目录（兼容本地运行和 Docker/Codespaces 部署）
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=os.path.join(PROJECT_ROOT, 'static'))

# ── 全局缓存 ──────────────────────────────────────────
_cache = {}
_cache_time = {}
_cache_lock = threading.Lock()

def cache_get(key, ttl=300):
    with _cache_lock:
        if key in _cache and time.time() - _cache_time.get(key, 0) < ttl:
            return _cache[key]
    return None

def cache_set(key, val):
    with _cache_lock:
        _cache[key] = val
        _cache_time[key] = time.time()

def safe_float(v, default=None):
    """安全转浮点。0 值合法（增长率/换手率等均可为 0），不再过滤。"""
    try:
        if v is None or str(v).strip() in ('--', 'nan', 'None', 'NaN', '-', '', 'False', 'True', 'false', 'true'):
            return default
        s = str(v).replace('%', '').replace(',', '').strip()
        return float(s)
    except:
        return default

def safe_float_allow_zero(v, default=None):
    """允许0值的安全转换"""
    try:
        if v is None or str(v).strip() in ('--', 'nan', 'None', 'NaN', '-', ''):
            return default
        return float(str(v).replace('%', '').replace(',', '').strip())
    except:
        return default


# ══════════════════════════════════════════════════════
# 核心数据源1：腾讯行情接口
# 单次请求500只，全量10批，完全并发，<2秒
# 包含：价格/涨跌幅/成交量/换手率/PE(TTM)/PB/总市值
# ══════════════════════════════════════════════════════

TENCENT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://finance.qq.com/'
}
TENCENT_BATCH = 500
TENCENT_URL   = 'https://qt.gtimg.cn/q='

# 腾讯字段索引（基于实测）
# 字段以~分割，index从0开始
# [1]=股票名 [2]=代码 [3]=当前价格 [4]=昨收 [5]=今开
# [6]=成交量(手) [31]=涨跌额 [32]=涨跌幅% [37]=成交额(万)
# [38]=换手率% [39]=PE(TTM) [44]=总市值(亿) [46]=PB(市净率)
TQ_NAME    = 1
TQ_CODE    = 2
TQ_PRICE   = 3
TQ_YCLOSE  = 4
TQ_OPEN    = 5
TQ_VOL     = 6   # 手
TQ_PCT_CHG = 32  # 涨跌幅%
TQ_AMOUNT  = 37  # 成交额(万元)
TQ_TURNOVER= 38  # 换手率%
TQ_PE_TTM  = 39  # PE(TTM)
TQ_MV_TOTAL= 44  # 总市值(亿)
TQ_MV_CIRC = 45  # 流通市值(亿)
TQ_PB      = 46  # 市净率(PB)

def _parse_tencent_line(line: str) -> dict | None:
    """解析腾讯行情单行"""
    try:
        if '~' not in line:
            return None
        f = line.strip().split('~')
        if len(f) < 50:
            return None
        code = f[TQ_CODE].strip().zfill(6)
        if not code or not code.isdigit():
            return None
        price = safe_float_allow_zero(f[TQ_PRICE])
        if not price or price <= 0:
            return None
        pre_close = safe_float_allow_zero(f[TQ_YCLOSE])
        pct_chg = safe_float_allow_zero(f[TQ_PCT_CHG])
        return {
            'code':        code,
            'name':        f[TQ_NAME],
            'price':       price,
            'pre_close':   pre_close,
            'open':        safe_float_allow_zero(f[TQ_OPEN]),
            'volume':      safe_float_allow_zero(f[TQ_VOL]),      # 手
            'amount':      safe_float_allow_zero(f[TQ_AMOUNT]),   # 万元
            'pct_chg':     pct_chg,
            'turnover':    safe_float(f[TQ_TURNOVER]),
            'pe_ttm':      safe_float(f[TQ_PE_TTM]),              # 可能为None（亏损股）
            'market_cap':  safe_float(f[TQ_MV_TOTAL]),            # 亿元
            'float_mv':    safe_float(f[TQ_MV_CIRC]),             # 亿元
            'pb':          safe_float(f[TQ_PB]),
        }
    except:
        return None


def _fetch_tencent_batch(codes: list) -> list:
    """并发安全的单批腾讯行情请求"""
    sina_codes = []
    for c in codes:
        if c.startswith('6'):
            sina_codes.append(f'sh{c}')
        else:
            sina_codes.append(f'sz{c}')
    url = TENCENT_URL + ','.join(sina_codes)
    result = []
    try:
        resp = requests.get(url, headers=TENCENT_HEADERS, timeout=15)
        resp.encoding = 'gbk'
        for line in resp.text.split(';'):
            item = _parse_tencent_line(line)
            if item:
                result.append(item)
    except Exception as e:
        print(f'[WARN] tencent batch error: {e}')
    return result


def get_all_codes_from_baostock() -> list:
    """从 baostock 获取全量A股代码（只用于初始化，后续缓存）"""
    cached = cache_get('all_codes', ttl=7200)
    if cached:
        return cached
    try:
        import baostock as bs
        lg = bs.login()
        codes = []
        rs = bs.query_stock_basic()
        while rs.next():
            row = rs.get_row_data()
            code_full = row[0]
            stock_type = row[4]
            if stock_type == '1':
                parts = code_full.split('.')
                if len(parts) == 2:
                    code = parts[1]
                    if code.startswith(('6', '0', '3')):
                        codes.append(code)
        bs.logout()
        cache_set('all_codes', codes)
        print(f'[INFO] baostock全量A股代码: {len(codes)} 只')
        return codes
    except Exception as e:
        print(f'[WARN] baostock获取代码失败: {e}，使用行情数据代码')
        return []


def fetch_quotes_for_codes(codes: list) -> list:
    """定向查询指定股票行情（仅请求需要的代码，毫秒级）"""
    if not codes:
        return []
    # 去重并格式化
    seen = set()
    unique = []
    for c in codes:
        c = str(c).strip().zfill(6)
        if c not in seen and c.isdigit() and len(c) == 6:
            seen.add(c)
            unique.append(c)
    if not unique:
        return []
    # 单批直接请求（通常自选股不超过50只）
    return _fetch_tencent_batch(unique)


def fetch_tencent_market() -> list:
    """
    从腾讯接口获取全量A股实时行情（含PE/PB/市值/换手率）
    全量10批完全并发，<2秒
    TTL=60秒（盘中实时刷新）
    """
    cached = cache_get('tencent_market', ttl=15)
    if cached is not None:
        return cached

    # 获取代码列表
    codes = cache_get('all_codes', ttl=7200)
    if not codes:
        # 先用腾讯接口自动发现代码（通过沪深市场分类请求）
        codes = _discover_codes_via_tencent()
        if codes:
            cache_set('all_codes', codes)
        else:
            # fallback: baostock
            codes = get_all_codes_from_baostock()

    if not codes:
        return []

    # 分批并发
    batches = [codes[i:i+TENCENT_BATCH] for i in range(0, len(codes), TENCENT_BATCH)]
    all_stocks = []

    t = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(batches)) as ex:
        futures = [ex.submit(_fetch_tencent_batch, b) for b in batches]
        for fut in concurrent.futures.as_completed(futures):
            all_stocks.extend(fut.result())

    # 过滤掉ST/停牌
    all_stocks = [s for s in all_stocks if s.get('price', 0) > 0]
    print(f'[INFO] 腾讯行情: {len(all_stocks)} 只，耗时 {time.time()-t:.2f}s')

    cache_set('tencent_market', all_stocks)
    return all_stocks


def _discover_codes_via_tencent() -> list:
    """通过腾讯接口按市场分类获取全量代码"""
    # 腾讯行情列表接口（不同于报价接口）
    # 沪市: http://qt.gtimg.cn/q=sh_slist&page=1&sortby=0&asc=0&mod=1
    # 深市: http://qt.gtimg.cn/q=sz_slist&page=1&sortby=0&asc=0&mod=1
    # 改用：直接查新浪全量代码（更稳定）
    codes = []
    try:
        # 使用 akshare 获取代码列表（很快，不涉及数据请求）
        df_sh = ak.stock_info_sh_name_code(symbol='主板A股')
        sh_codes = df_sh['证券代码'].astype(str).str.zfill(6).tolist()
        codes.extend(sh_codes)
    except Exception as e:
        print(f'[WARN] 沪市代码获取失败: {e}')

    try:
        for symbol in ['A股列表', 'B股列表', '科创板']:
            try:
                df_sz = ak.stock_info_sz_name_code(symbol=symbol)
                sz_codes = df_sz['A股代码'].astype(str).str.zfill(6).tolist()
                codes.extend([c for c in sz_codes if c.strip() and c != '000000'])
            except:
                pass
    except Exception as e:
        print(f'[WARN] 深市代码获取失败: {e}')

    # 去重
    codes = list(set(c for c in codes if c.isdigit() and len(c) == 6
                     and (c.startswith('6') or c.startswith('0') or c.startswith('3'))))
    print(f'[INFO] 代码发现: {len(codes)} 只')
    return codes


# ══════════════════════════════════════════════════════
# 核心数据源2：ROE + 财务指标（同花顺，仅对候选股查询）
# ══════════════════════════════════════════════════════

# 财务关键字段（全部 5 个必须齐全才算有效）
FIN_KEY_FIELDS = ['roe', 'net_profit_growth', 'revenue_growth', 'gross_margin', 'debt_ratio']

def get_financial(code: str) -> dict:
    """获取单只股票财务指标（优先从本地DB查询，毫秒级）"""
    # 先查本地数据库 — 需要所有 5 个关键字段都齐全才直接返回
    db_result = fin_db.batch_query([code])
    if code in db_result:
        d = db_result[code]
        if all(d.get(f) is not None for f in FIN_KEY_FIELDS):
            return d

    # fallback: 先查内存缓存（需字段齐全才复用）
    cached = cache_get(f'fin_{code}', ttl=3600)
    if cached is not None and all(cached.get(f) is not None for f in FIN_KEY_FIELDS):
        return cached

    result = {}
    for indicator in ["按报告期", "按年度"]:
        try:
            df = ak.stock_financial_abstract_ths(symbol=code, indicator=indicator)
            if df is None or len(df) == 0:
                continue
            row = df.iloc[-1]  # 最新一期数据（非最早）

            roe_col = next((c for c in df.columns if '净资产收益率' in c and '摊薄' not in c), None)
            if roe_col and result.get('roe') is None:
                result['roe'] = safe_float(row.get(roe_col))

            # ak 实际列名：净利润同比增长率、营业总收入同比增长率
            for kw in ['净利润同比增长率', '归母净利润同比增长率', '净利润增长率', '归母净利润增长率']:
                col = next((c for c in df.columns if kw in c), None)
                if col and result.get('net_profit_growth') is None:
                    result['net_profit_growth'] = safe_float(row.get(col))
                    break

            for kw in ['营业总收入同比增长率', '营业收入同比增长率', '营收增长率', '营业总收入增长率']:
                col = next((c for c in df.columns if kw in c), None)
                if col and result.get('revenue_growth') is None:
                    result['revenue_growth'] = safe_float(row.get(col))
                    break

            for kw in ['毛利率', '销售毛利率']:
                col = next((c for c in df.columns if kw in c), None)
                if col and result.get('gross_margin') is None:
                    result['gross_margin'] = safe_float(row.get(col))
                    break

            for kw in ['资产负债率', '负债率']:
                col = next((c for c in df.columns if kw in c), None)
                if col and result.get('debt_ratio') is None:
                    result['debt_ratio'] = safe_float(row.get(col))
                    break

            if result.get('roe') is not None:
                break
        except Exception:
            continue

    cache_set(f'fin_{code}', result)
    # 持久化到本地 SQLite，下次查询毫秒级
    if result:
        fin_db.upsert(code, result)
    return result


def batch_get_financial(codes: list, max_workers=30) -> dict:
    """
    批量获取财务数据（优先本地SQLite，毫秒级；fallback akshare并发）
    """
    # 先批量从本地DB查
    db_results = fin_db.batch_query(codes)
    # 检查是否所有关键财务字段都齐全，缺失任一字段触发 akshare fallback
    FIN_KEY_FIELDS = ['roe', 'net_profit_growth', 'revenue_growth', 'gross_margin', 'debt_ratio']
    missing = [c for c in codes if c not in db_results or
               any(db_results[c].get(f) is None for f in FIN_KEY_FIELDS)]

    # 对于本地没有的，用 akshare fallback
    if missing:
        fallback = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(missing))) as ex:
            future_map = {ex.submit(get_financial, c): c for c in missing}
            for future in concurrent.futures.as_completed(future_map):
                code = future_map[future]
                try:
                    fallback[code] = future.result()
                except:
                    fallback[code] = {}
        db_results.update(fallback)

    return db_results


# ══════════════════════════════════════════════════════
# 核心数据源3：技术指标（新浪日K线，纯HTTP并发）
# 彻底移除 baostock，改为新浪财经接口
# 0.17s/只，100并发下5000只约26秒
# 但漏斗策略保证进入此轮的股票通常<200只
# ══════════════════════════════════════════════════════

SINA_KLINE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://finance.sina.com.cn/'
}


def _fetch_sina_kline(code: str, datalen=80) -> list | None:
    """
    新浪日K线，返回 [{day, open, high, low, close, volume}, ...]
    scale=240 实际等于日线数据（新浪参数，240分钟=日线）
    """
    prefix = 'sh' if code.startswith('6') else 'sz'
    url = (f'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/'
           f'CN_MarketData.getKLineData?symbol={prefix}{code}'
           f'&scale=240&datalen={datalen}&ma=no')
    try:
        resp = requests.get(url, headers=SINA_KLINE_HEADERS, timeout=10)
        if resp.status_code != 200:
            return None
        data = json.loads(resp.text)
        if not isinstance(data, list) or len(data) == 0:
            return None
        return data
    except:
        return None


def compute_indicators_fast(codes: list, max_workers=100) -> dict:
    """
    高速计算技术指标（纯HTTP并发，无串行锁）
    指标：MA5/MA10/MA20位置、MACD金叉、KDJ金叉、RSI、量比、52周新高、连续上涨
    """
    result = {}

    def compute_one(code):
        try:
            raw = _fetch_sina_kline(code, datalen=80)
            if not raw or len(raw) < 10:
                return code, {}

            closes = [float(r['close']) for r in raw]
            highs  = [float(r['high'])  for r in raw]
            lows   = [float(r['low'])   for r in raw]
            vols   = [float(r['volume']) for r in raw]
            n = len(closes)

            ind = {}

            # ── MA5/10/20 位置
            for period in [5, 10, 20]:
                if n >= period:
                    ma = sum(closes[-period:]) / period
                    ind[f'ma{period}'] = round(ma, 3)
                    ind[f'ma{period}_above'] = bool(closes[-1] > ma)

            # ── MACD(12,26,9) 金叉
            if n >= 35:
                s = pd.Series(closes)
                ema12 = s.ewm(span=12, adjust=False).mean()
                ema26 = s.ewm(span=26, adjust=False).mean()
                dif = ema12 - ema26
                dea = dif.ewm(span=9, adjust=False).mean()
                ind['macd_gc'] = bool(
                    float(dif.iloc[-1]) > float(dea.iloc[-1]) and
                    float(dif.iloc[-2]) <= float(dea.iloc[-2])
                )
                ind['dif'] = round(float(dif.iloc[-1]), 4)
                ind['dea'] = round(float(dea.iloc[-1]), 4)

            # ── KDJ(9,3,3) 金叉
            if n >= 9:
                hs = pd.Series(highs)
                ls = pd.Series(lows)
                low9  = ls.rolling(9, min_periods=9).min()
                high9 = hs.rolling(9, min_periods=9).max()
                rsv = (pd.Series(closes) - low9) / (high9 - low9 + 1e-9) * 100
                K = rsv.ewm(com=2, adjust=False).mean()
                D = K.ewm(com=2, adjust=False).mean()
                J = 3*K - 2*D
                ind['kdj_k'] = round(float(K.iloc[-1]), 2)
                ind['kdj_d'] = round(float(D.iloc[-1]), 2)
                ind['kdj_j'] = round(float(J.iloc[-1]), 2)
                ind['kdj_gc'] = bool(
                    float(K.iloc[-1]) > float(D.iloc[-1]) and
                    float(K.iloc[-2]) <= float(D.iloc[-2])
                )

            # ── RSI(14)
            if n >= 15:
                cs = pd.Series(closes)
                delta = cs.diff()
                gain = delta.clip(lower=0).rolling(14).mean()
                loss = (-delta.clip(upper=0)).rolling(14).mean()
                rsi = 100 - (100 / (1 + gain / (loss + 1e-9)))
                ind['rsi14'] = round(float(rsi.iloc[-1]), 2)

            # ── 量比（今日量 / 近5日均量）
            if n >= 6 and vols[-1] > 0:
                avg5 = sum(vols[-6:-1]) / 5
                if avg5 > 0:
                    ind['vol_ratio'] = round(vols[-1] / avg5, 2)

            # ── 52周新高
            if n >= 52:
                high52 = max(highs[-52:])
                ind['is_52w_high'] = bool(closes[-1] >= high52 * 0.99)

            # ── 连续上涨天数
            streak = 0
            for i in range(n-1, 0, -1):
                if closes[i] > closes[i-1]:
                    streak += 1
                else:
                    break
            ind['up_streak'] = streak

            return code, ind
        except Exception:
            return code, {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(compute_one, c): c for c in codes}
        for future in concurrent.futures.as_completed(future_map):
            code, data = future.result()
            result[code] = data

    return result


# ══════════════════════════════════════════════════════
# 筛选引擎
# ══════════════════════════════════════════════════════

def apply_condition(val, op, threshold):
    if val is None:
        return False
    if op == '<':   return val < threshold
    if op == '>':   return val > threshold
    if op == '<=':  return val <= threshold
    if op == '>=':  return val >= threshold
    if op == '==':  return abs(float(val) - threshold) < 1e-9
    return False


# 字段归类
PRICE_FIELDS     = {'price', 'pct_chg', 'volume', 'amount', 'high', 'low'}
VALUATION_FIELDS = {'pe_ttm', 'pb', 'market_cap', 'float_mv', 'turnover'}
FINANCIAL_FIELDS = {'roe', 'net_profit_growth', 'revenue_growth', 'gross_margin', 'debt_ratio'}
INDICATOR_FIELDS = {'ma5_above', 'ma10_above', 'ma20_above',
                    'macd_gc', 'kdj_gc', 'rsi14', 'vol_ratio',
                    'is_52w_high', 'up_streak'}


def screen(conditions: list, sample_size=None, progress_callback=None) -> list:
    """
    主筛选函数 v5 — 4轮高速漏斗
    R1: 行情+估值初筛（腾讯接口，<2秒完成）
    R2: PE/PB/市值/换手率筛选（数据已在R1中，无额外请求）
    R3: 财务指标筛选（仅对候选股，并发30线程）
    R4: 技术指标筛选（仅对候选股，并发100线程，新浪K线）
    """
    def log(msg):
        print(msg)
        if progress_callback:
            progress_callback(msg)

    # ★ 深拷贝，防止缓存污染
    t_start = time.time()
    log('R1 拉取实时行情（腾讯）...')
    raw_stocks = fetch_tencent_market()
    if not raw_stocks:
        log('行情获取失败，请检查网络')
        return []

    stocks = copy.deepcopy(raw_stocks)
    if sample_size and sample_size > 0:
        stocks = stocks[:sample_size]

    log(f'R1 行情加载: {len(stocks)} 只，耗时 {time.time()-t_start:.1f}s')

    price_conds     = [c for c in conditions if c['field'] in PRICE_FIELDS]
    val_conds       = [c for c in conditions if c['field'] in VALUATION_FIELDS]
    fin_conds       = [c for c in conditions if c['field'] in FINANCIAL_FIELDS]
    indicator_conds = [c for c in conditions if c['field'] in INDICATOR_FIELDS]

    # ═══ R1: 实时行情初筛 ═══
    # 过滤停牌/无价格
    candidates = [s for s in stocks if s.get('price', 0) > 0]
    if price_conds:
        candidates = [s for s in candidates if all(
            apply_condition(s.get(c['field']), c['op'], c['value']) for c in price_conds
        )]
    log(f'R1 行情筛选后: {len(candidates)} 只')
    if not candidates:
        return []

    # ═══ R2: PE/PB/市值/换手率（已内嵌在行情数据中，无需额外请求）═══
    if val_conds:
        # 特殊处理 pe_ttm > 0 的逻辑（过滤亏损股）
        def val_pass(s):
            for c in val_conds:
                field = c['field']
                val = s.get(field)
                # PE为负或None，视为不符合任何PE条件（亏损股）
                if field == 'pe_ttm' and (val is None or val <= 0):
                    return False
                if not apply_condition(val, c['op'], c['value']):
                    return False
            return True
        candidates = [s for s in candidates if val_pass(s)]
        log(f'R2 PE/PB/市值筛选后: {len(candidates)} 只')
        if not candidates:
            return []

    # ═══ R3: 财务指标（ROE/增长/毛利/负债）═══
    # ★ 始终拉取财务数据（不管有没有财务条件），保证结果指标完整
    codes = [s['code'] for s in candidates]
    log(f'R3 查询财务指标（{len(codes)} 只，并发30线程）...')
    t3 = time.time()
    fin_map = batch_get_financial(codes, max_workers=30)
    for s in candidates:
        f = fin_map.get(s['code'], {})
        s['roe']               = f.get('roe')
        s['net_profit_growth'] = f.get('net_profit_growth')
        s['revenue_growth']    = f.get('revenue_growth')
        s['gross_margin']      = f.get('gross_margin')
        s['debt_ratio']        = f.get('debt_ratio')
    if fin_conds:
        candidates = [s for s in candidates if all(
            apply_condition(s.get(c['field']), c['op'], c['value']) for c in fin_conds
        )]
        log(f'R3 财务筛选后: {len(candidates)} 只，耗时 {time.time()-t3:.1f}s')
        if not candidates:
            return []
    else:
        log(f'R3 财务数据填充: {len(candidates)} 只，耗时 {time.time()-t3:.1f}s')

    # ═══ R4: 技术指标（新浪K线，高并发）═══
    # ★ 始终计算技术指标（不管有没有技术条件），保证结果指标完整
    if len(candidates) > 0:
        codes = [s['code'] for s in candidates]
        log(f'R4 计算技术指标（{len(codes)} 只，并发100线程）...')
        t4 = time.time()
        ind_map = compute_indicators_fast(codes, max_workers=min(100, len(codes)+1))
        for s in candidates:
            ind = ind_map.get(s['code'], {})
            for k in ['ma5_above', 'ma10_above', 'ma20_above',
                      'macd_gc', 'kdj_gc', 'rsi14', 'vol_ratio',
                      'is_52w_high', 'up_streak', 'ma5', 'ma10', 'ma20',
                      'dif', 'dea', 'kdj_k', 'kdj_d', 'kdj_j']:
                s[k] = ind.get(k)
        if indicator_conds:
            candidates = [s for s in candidates if all(
                apply_condition(
                    (1 if s.get(c['field']) is True else
                     (0 if s.get(c['field']) is False else s.get(c['field']))),
                    c['op'], c['value']
                ) for c in indicator_conds
            )]
            log(f'R4 技术指标筛选后: {len(candidates)} 只，耗时 {time.time()-t4:.1f}s')
        else:
            log(f'R4 技术指标计算: {len(candidates)} 只，耗时 {time.time()-t4:.1f}s')

    total_time = time.time() - t_start
    log(f'筛选完成，总耗时 {total_time:.1f}s，命中 {len(candidates)} 只')
    return candidates


# ══════════════════════════════════════════════════════
# 异步筛选任务管理
# ══════════════════════════════════════════════════════

_screen_tasks = {}
_task_lock = threading.Lock()
_task_counter = [0]

@app.route('/api/screen/start', methods=['POST'])
def screen_start():
    body = request.get_json() or {}
    conditions = body.get('conditions', [])
    sample_size = body.get('sample_size', None)
    with _task_lock:
        task_id = str(_task_counter[0])
        _task_counter[0] += 1
    _screen_tasks[task_id] = {
        'status': 'running', 'progress': '启动筛选...', 'result': None,
        'start_time': time.time()
    }

    def run():
        try:
            t0 = time.time()
            def on_progress(msg):
                _screen_tasks[task_id]['progress'] = msg
            results = screen(conditions, sample_size=sample_size, progress_callback=on_progress)
            elapsed = time.time() - t0
            _screen_tasks[task_id].update({
                'status': 'done',
                'progress': f'筛选完成，命中 {len(results)} 只',
                'result': results,
                'elapsed': round(elapsed, 1),
                'db_hit': fin_db.get_stats().get('count', 0)
            })
        except Exception as e:
            traceback.print_exc()
            _screen_tasks[task_id].update({
                'status': 'error', 'progress': f'筛选失败: {str(e)}', 'result': None
            })

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'success': True, 'task_id': task_id})


@app.route('/api/screen/status/<task_id>')
def screen_status(task_id):
    task = _screen_tasks.get(task_id)
    if not task:
        return jsonify({'success': False, 'error': '任务不存在'})
    elapsed = time.time() - task.get('start_time', time.time())
    return jsonify({
        'success': True,
        'status': task['status'],
        'progress': task['progress'],
        'elapsed': round(elapsed, 1)
    })


@app.route('/api/screen/result/<task_id>')
def screen_result(task_id):
    task = _screen_tasks.get(task_id)
    if not task:
        return jsonify({'success': False, 'error': '任务不存在'})
    if task['status'] != 'done':
        return jsonify({'success': False, 'error': '任务尚未完成'})
    results = task['result'] or []
    FIELDS = ['code', 'name', 'price', 'pct_chg', 'pe_ttm', 'pb', 'market_cap', 'float_mv',
              'turnover', 'volume', 'amount',
              'roe', 'net_profit_growth', 'revenue_growth', 'gross_margin', 'debt_ratio',
              'ma5', 'ma5_above', 'ma10', 'ma10_above', 'ma20', 'ma20_above',
              'macd_gc', 'dif', 'dea', 'kdj_gc', 'kdj_k', 'kdj_d', 'kdj_j',
              'rsi14', 'vol_ratio', 'is_52w_high', 'up_streak']
    output = [{f: r.get(f) for f in FIELDS} for r in results]
    output.sort(key=lambda x: (x.get('roe') or -999), reverse=True)
    # 缓存最近一次筛选结果（供导出 API 使用）
    global _last_screen_result
    _last_screen_result = output
    return jsonify({
        'success': True, 'count': len(output), 'data': output,
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'elapsed': round(_screen_tasks[task_id].get('elapsed', 0), 1),
        'db_hit': _screen_tasks[task_id].get('db_hit', 0)
    })


# ══════════════════════════════════════════════════════
# 其他 API
# ══════════════════════════════════════════════════════

@app.route('/')
def index():
    return send_from_directory(os.path.join(PROJECT_ROOT, 'static'), 'index.html')


@app.route('/api/market_overview')
def market_overview():
    try:
        result = {}
        try:
            stocks = fetch_tencent_market()
            if stocks:
                result['total_stocks'] = len(stocks)
                pcts = [s['pct_chg'] for s in stocks if s.get('pct_chg') is not None]
                result['rising']  = sum(1 for p in pcts if p > 0)
                result['falling'] = sum(1 for p in pcts if p < 0)
                result['flat']    = sum(1 for p in pcts if p == 0)
                result['avg_pct'] = round(sum(pcts)/len(pcts), 2) if pcts else 0
        except Exception as e:
            print(f'market_overview stocks error: {e}')
        try:
            pe_df = ak.stock_market_pe_lg(symbol="sh")
            if len(pe_df) > 0:
                latest = pe_df.iloc[-1]
                result['sh_pe'] = {
                    'date': str(latest['日期']),
                    'pe': float(latest['市盈率'])
                }
        except: pass
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/prefetch')
def prefetch():
    try:
        stocks = fetch_tencent_market()
        return jsonify({
            'success': True, 'count': len(stocks),
            'message': f'已加载 {len(stocks)} 只股票（含PE/PB/市值/换手率）'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/stock_detail/<code>')
def stock_detail(code):
    try:
        stocks = cache_get('tencent_market', ttl=60) or []
        snap = next((s for s in stocks if s['code'] == code), {})
        fin = get_financial(code)
        result = {**snap, **fin}

        # 尝试获取K线
        try:
            kline = _fetch_sina_kline(code, datalen=30)
            if kline:
                result['history'] = kline[-30:]
        except: pass

        return jsonify({'success': True, 'code': code, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/config', methods=['GET', 'POST'])
def config():
    config_path = os.path.join(os.path.dirname(__file__), 'filter_config.json')
    if request.method == 'GET':
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return jsonify({'success': True, 'data': json.load(f)})
        except:
            return jsonify({'success': True, 'data': []})
    else:
        try:
            data = request.get_json()
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})


# ══════════════════════════════════════════════════════
# 策略系统 API（v5.2 银级）
# ══════════════════════════════════════════════════════

STRATEGIES_FILE = os.path.join(os.path.dirname(__file__), 'strategies.json')

def _load_strategies() -> dict:
    try:
        with open(STRATEGIES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {'presets': [], 'custom': []}

def _save_strategies(data: dict):
    with open(STRATEGIES_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@app.route('/api/strategies', methods=['GET'])
def strategies_list():
    """获取所有策略（预设 + 自定义）"""
    data = _load_strategies()
    return jsonify({'success': True, 'data': data})


@app.route('/api/strategies/save', methods=['POST'])
def strategies_save():
    """保存自定义策略"""
    body = request.get_json() or {}
    name = body.get('name', '').strip()
    conditions = body.get('conditions', [])
    if not name or not conditions:
        return jsonify({'success': False, 'error': '策略名称和条件不能为空'})

    data = _load_strategies()
    custom_id = f"custom_{int(time.time())}"
    data['custom'].append({
        'id': custom_id,
        'name': name,
        'icon': body.get('icon', '📋'),
        'desc': body.get('desc', ''),
        'conditions': conditions
    })
    _save_strategies(data)
    return jsonify({'success': True, 'id': custom_id})


@app.route('/api/strategies/delete/<strategy_id>', methods=['DELETE'])
def strategies_delete(strategy_id):
    """删除自定义策略"""
    data = _load_strategies()
    data['custom'] = [s for s in data['custom'] if s['id'] != strategy_id]
    _save_strategies(data)
    return jsonify({'success': True})


# ══════════════════════════════════════════════════════
# 财务数据库管理 API
# ══════════════════════════════════════════════════════

@app.route('/api/fin_db/stats')
def fin_db_stats():
    """查看本地财务数据库状态"""
    stats = fin_db.get_stats()
    return jsonify({'success': True, 'data': stats})


@app.route('/api/fin_db/build', methods=['POST'])
def fin_db_build():
    """手动触发生成财务快照（异步后台）"""
    body = request.get_json() or {}
    max_workers = body.get('max_workers', 50)
    full_rebuild = body.get('full_rebuild', False)

    def run_build():
        fin_db.build_snapshot(max_workers=max_workers, incremental=not full_rebuild)

    threading.Thread(target=run_build, daemon=True).start()
    return jsonify({
        'success': True,
        'message': f'财务快照构建已启动（{max_workers}线程，{"全量重建" if full_rebuild else "增量更新"}），约需2-5分钟'
    })


@app.route('/api/data_freshness')
def data_freshness():
    """各数据源的实时时效性"""
    now = time.time()
    tencent_age = now - _cache_time.get('tencent_market', 0)
    fin_db_stats = fin_db.get_stats()

    return jsonify({
        'success': True,
        'data': {
            'tencent_market': {
                'age_seconds': round(tencent_age, 1),
                'fresh': tencent_age < 30
            },
            'financial_db': {
                'count': fin_db_stats.get('count', 0),
                'updated': fin_db_stats.get('latest_update'),
                'stale': fin_db.is_stale(24),
            },
            'server_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    })


# ══════════════════════════════════════════════════════
# 自选股管理 API
# ══════════════════════════════════════════════════════

WATCHLIST_FILE = os.path.join(os.path.dirname(__file__), 'watchlist.json')

def _load_watchlist() -> list:
    try:
        with open(WATCHLIST_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return []

def _save_watchlist(data: list):
    with open(WATCHLIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@app.route('/api/watchlist', methods=['GET'])
def watchlist_list():
    """获取自选股列表（含实时行情 + 财务数据）"""
    try:
        wl = _load_watchlist()
        codes = [item['code'] for item in wl]
        stocks_map = {}
        if codes:
            stocks = fetch_quotes_for_codes(codes)  # 定向查询，仅请求自选股（毫秒级）
            stocks_map = {s['code']: s for s in stocks}
        # 批量获取财务数据（本地DB毫秒级）
        fin_map = {}
        if codes:
            fin_map = batch_get_financial(codes)
        result = []
        for item in wl:
            quote = stocks_map.get(item['code'], {})
            fin = fin_map.get(item['code'], {})
            # ★ 优先用腾讯行情返回的真实名称（item.name 可能是用户输入的代码占位符）
            real_name = quote.get('name') or item.get('name', '--')
            result.append({
                'code': item['code'],
                'name': real_name,
                'added_at': item.get('added_at', ''),
                'note': item.get('note', ''),
                'price': quote.get('price'),
                'pct_chg': quote.get('pct_chg'),
                'pe_ttm': quote.get('pe_ttm'),
                'pb': quote.get('pb'),
                'market_cap': quote.get('market_cap'),
                'volume': quote.get('volume'),
                'amount': quote.get('amount'),
                'turnover': quote.get('turnover'),
                # 财务指标
                'roe': fin.get('roe'),
                'net_profit_growth': fin.get('net_profit_growth'),
                'revenue_growth': fin.get('revenue_growth'),
                'gross_margin': fin.get('gross_margin'),
                'debt_ratio': fin.get('debt_ratio'),
            })
        return jsonify({'success': True, 'data': result, 'count': len(result)})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/watchlist', methods=['POST'])
def watchlist_add():
    """添加自选股（支持批量），返回完整自选股列表（含行情+财务），省去前端二次请求"""
    try:
        body = request.get_json() or {}
        codes_input = body.get('codes', [])  # [{'code':'600036','name':'招商银行'}]
        if isinstance(codes_input, str):
            codes_input = [{'code': c.strip()} for c in codes_input.split(',') if c.strip()]

        wl = _load_watchlist()
        existing = {item['code'] for item in wl}
        added = 0
        for entry in codes_input:
            code = entry.get('code', '').strip().zfill(6)
            if not code or not code.isdigit() or len(code) != 6:
                continue
            if code in existing:
                continue
            name = entry.get('name', '') or code
            wl.append({
                'code': code,
                'name': name,
                'added_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'note': entry.get('note', '')
            })
            existing.add(code)
            added += 1

        _save_watchlist(wl)

        # ★ 直接返回完整自选股列表（同 GET），前端无需二次加载
        codes = [item['code'] for item in wl]
        stocks_map = {}
        if codes:
            stocks = fetch_quotes_for_codes(codes)
            stocks_map = {s['code']: s for s in stocks}
        fin_map = {}
        if codes:
            fin_map = batch_get_financial(codes)
        result = []
        for item in wl:
            quote = stocks_map.get(item['code'], {})
            fin = fin_map.get(item['code'], {})
            # ★ 优先用腾讯行情返回的真实名称
            real_name = quote.get('name') or item.get('name', '--')
            result.append({
                'code': item['code'],
                'name': real_name,
                'added_at': item.get('added_at', ''),
                'note': item.get('note', ''),
                'price': quote.get('price'),
                'pct_chg': quote.get('pct_chg'),
                'pe_ttm': quote.get('pe_ttm'),
                'pb': quote.get('pb'),
                'market_cap': quote.get('market_cap'),
                'volume': quote.get('volume'),
                'amount': quote.get('amount'),
                'turnover': quote.get('turnover'),
                'roe': fin.get('roe'),
                'net_profit_growth': fin.get('net_profit_growth'),
                'revenue_growth': fin.get('revenue_growth'),
                'gross_margin': fin.get('gross_margin'),
                'debt_ratio': fin.get('debt_ratio'),
            })

        return jsonify({
            'success': True, 'added': added, 'total': len(wl),
            'data': result, 'count': len(result)
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/watchlist/<code>', methods=['DELETE'])
def watchlist_remove(code):
    """删除自选股"""
    try:
        wl = _load_watchlist()
        wl = [item for item in wl if item['code'] != code]
        _save_watchlist(wl)
        return jsonify({'success': True, 'total': len(wl)})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/watchlist/<code>', methods=['PUT'])
def watchlist_update_note(code):
    """更新自选股备注"""
    try:
        body = request.get_json() or {}
        wl = _load_watchlist()
        for item in wl:
            if item['code'] == code:
                item['note'] = body.get('note', '')
                break
        _save_watchlist(wl)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/watchlist/refresh', methods=['GET'])
def watchlist_refresh():
    """强制刷新自选股行情"""
    cache_set('tencent_market', None)  # 清除缓存
    return watchlist_list()


# ══════════════════════════════════════════════════════
# 导出 API（Excel + PDF）
# ══════════════════════════════════════════════════════

_last_screen_result = []

@app.route('/api/export/excel', methods=['POST'])
def export_excel():
    """导出筛选结果为 Excel"""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter
    except ImportError:
        return jsonify({'success': False, 'error': '需要安装 openpyxl: pip install openpyxl'})

    body = request.get_json() or {}
    data = body.get('data', _last_screen_result)
    if not data:
        return jsonify({'success': False, 'error': '无数据可导出'})

    title = body.get('title', 'A股量化选股结果')
    
    wb = Workbook()
    ws = wb.active
    ws.title = '选股结果'

    # 样式
    header_font = Font(name='微软雅黑', bold=True, color='FFFFFF', size=11)
    header_fill = PatternFill(start_color='1F6FEB', end_color='1F6FEB', fill_type='solid')
    header_align = Alignment(horizontal='center', vertical='center')
    thin_border = Border(
        left=Side(style='thin', color='D0D0D0'),
        right=Side(style='thin', color='D0D0D0'),
        top=Side(style='thin', color='D0D0D0'),
        bottom=Side(style='thin', color='D0D0D0'),
    )
    red_font = Font(name='微软雅黑', color='CC0000', size=11)
    green_font = Font(name='微软雅黑', color='008800', size=11)
    normal_font = Font(name='微软雅黑', size=11)

    # 标题行
    row_start = 1
    if title and title != 'A股量化选股结果':
        cell = ws.cell(row=1, column=1, value=title)
        cell.font = Font(name='微软雅黑', bold=True, color='1F6FEB', size=14)
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=19)
        cell.alignment = Alignment(horizontal='center', vertical='center')
        ws.row_dimensions[1].height = 30
        row_start = 2

    # 表头
    headers = [
        ('代码', 10), ('名称', 12), ('当前价', 9), ('涨跌幅%', 9),
        ('PE(TTM)', 10), ('PB', 8), ('总市值(亿)', 12),
        ('ROE%', 9), ('净利增长%', 10), ('营收增长%', 10),
        ('毛利率%', 9), ('负债率%', 9), ('换手率%', 9),
        ('量比', 7), ('MACD金叉', 9), ('KDJ金叉', 9),
        ('RSI', 7), ('52W新高', 9), ('连涨天数', 9),
    ]

    for col_idx, (hdr, width) in enumerate(headers, 1):
        cell = ws.cell(row=row_start, column=col_idx, value=hdr)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    # 数据行
    fields = ['code', 'name', 'price', 'pct_chg', 'pe_ttm', 'pb', 'market_cap',
              'roe', 'net_profit_growth', 'revenue_growth', 'gross_margin', 'debt_ratio',
              'turnover', 'vol_ratio', 'macd_gc', 'kdj_gc', 'rsi14', 'is_52w_high', 'up_streak']

    for row_idx, row_data in enumerate(data, row_start + 1):
        for col_idx, field in enumerate(fields, 1):
            val = row_data.get(field)
            cell = ws.cell(row=row_idx, column=col_idx)
            cell.border = thin_border
            cell.alignment = Alignment(horizontal='center', vertical='center')

            if isinstance(val, bool):
                cell.value = '✓' if val else '✗'
                cell.font = green_font if val else Font(name='微软雅黑', color='999999', size=11)
            elif val is None:
                cell.value = '--'
                cell.font = Font(name='微软雅黑', color='999999', size=11)
            else:
                cell.value = val
                if field == 'pct_chg':
                    cell.font = red_font if val > 0 else (green_font if val < 0 else normal_font)
                elif field in ('roe', 'net_profit_growth', 'revenue_growth'):
                    cell.font = red_font if val > 0 else normal_font
                else:
                    cell.font = normal_font

    # 冻结标题行+表头行
    ws.freeze_panes = f'A{row_start + 1}'
    ws.auto_filter.ref = ws.dimensions

    # 自适应行高
    for row in ws.iter_rows(min_row=row_start + 1, max_row=len(data) + row_start):
        ws.row_dimensions[row[0].row].height = 22

    filename = f'选股结果_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    filepath = os.path.join(os.path.dirname(__file__), filename)
    wb.save(filepath)

    return jsonify({'success': True, 'filename': filename, 'filepath': filepath})


@app.route('/api/export/pdf', methods=['POST'])
def export_pdf():
    """导出筛选结果为 PDF——生成 HTML 后由前端/Bash 完成 Edge 转换"""
    body = request.get_json() or {}
    data = body.get('data', _last_screen_result)
    if not data:
        return jsonify({'success': False, 'error': '无数据可导出'})

    title = body.get('title', 'A股量化选股结果')

    # 生成 HTML
    html_parts = []
    html_parts.append('''<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8">
<style>
  body { font-family: "Microsoft YaHei","PingFang SC",sans-serif; font-size:12px; color:#333; margin:20px; }
  h1 { text-align:center; font-size:20px; color:#1f6feb; margin-bottom:4px; }
  .subtitle { text-align:center; font-size:11px; color:#888; margin-bottom:16px; }
  table { width:100%; border-collapse:collapse; margin-top:10px; }
  th { background:#1f6feb; color:#fff; padding:6px 4px; font-size:10px; text-align:center; }
  td { padding:5px 4px; text-align:center; border-bottom:1px solid #eee; font-size:11px; }
  tr:nth-child(even) { background:#f8f9fa; }
  .up { color:#cc0000; font-weight:bold; }
  .down { color:#008800; font-weight:bold; }
  .code { font-family:monospace; background:#e8ecf0; padding:1px 4px; border-radius:2px; }
  .badge-ok { color:#1a7f37; }
  .badge-no { color:#ccc; }
  .footer { text-align:center; font-size:10px; color:#aaa; margin-top:16px; }
  @media print { @page { size: A4 landscape; margin: 10mm; } }
</style></head><body>''')
    html_parts.append(f'<h1>{title}</h1>')
    html_parts.append(f'<div class="subtitle">共 {len(data)} 只 | 生成时间：{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>')

    headers = ['代码','名称','价格','涨跌%','PE','PB','市值亿','ROE%','净利增长%','营收增长%','毛利率%','负债率%','换手%','量比','MACD','KDJ','RSI','52W高','连涨']
    fields  = ['code','name','price','pct_chg','pe_ttm','pb','market_cap','roe','net_profit_growth','revenue_growth','gross_margin','debt_ratio','turnover','vol_ratio','macd_gc','kdj_gc','rsi14','is_52w_high','up_streak']

    html_parts.append('<table><thead><tr>')
    for h in headers:
        html_parts.append(f'<th>{h}</th>')
    html_parts.append('</tr></thead><tbody>')

    for row in data:
        html_parts.append('<tr>')
        for i, f in enumerate(fields):
            v = row.get(f)
            if isinstance(v, bool):
                html_parts.append(f'<td><span class="{"badge-ok" if v else "badge-no"}">{"✓" if v else "✗"}</span></td>')
            elif v is None:
                html_parts.append('<td>--</td>')
            elif f == 'pct_chg':
                cls = 'up' if v > 0 else ('down' if v < 0 else '')
                html_parts.append(f'<td class="{cls}">{v:+.2f}%</td>')
            elif f == 'code':
                html_parts.append(f'<td><span class="code">{v}</span></td>')
            elif f in ('pe_ttm', 'pb', 'rsi14'):
                html_parts.append(f'<td>{v:.2f}</td>' if isinstance(v,(int,float)) else f'<td>{v}</td>')
            elif f in ('roe', 'net_profit_growth', 'revenue_growth', 'gross_margin', 'debt_ratio', 'turnover'):
                html_parts.append(f'<td>{v:.1f}%</td>' if isinstance(v,(int,float)) else f'<td>{v}</td>')
            else:
                html_parts.append(f'<td>{v}</td>')
        html_parts.append('</tr>')

    html_parts.append('</tbody></table>')
    html_parts.append(f'<div class="footer">数据来源：腾讯行情 + 同花顺财务 + 新浪K线 | A股量化选股工具 v5.1</div>')
    html_parts.append('</body></html>')

    html_content = '\n'.join(html_parts)

    # 保存 HTML 和 PDF 路径
    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    html_path = os.path.join(os.path.dirname(__file__), f'选股结果_{ts}.html')
    pdf_path = os.path.join(os.path.dirname(__file__), f'选股结果_{ts}.pdf')

    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    return jsonify({
        'success': True,
        'html_path': html_path,
        'pdf_path': pdf_path,
        'filename': os.path.basename(pdf_path),
        'note': 'HTML已生成，需用Edge headless转换为PDF'
    })


@app.route('/api/export/pdf_view')
def pdf_view():
    """提供 PDF 预览页面（浏览器打印→另存为PDF）"""
    html_path = request.args.get('path', '')
    if not html_path or not os.path.exists(html_path):
        return '<h1>文件不存在</h1>', 404
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()
    # 插入自动打印脚本
    content = content.replace('</body>', '''
<script>
  window.onload = function() {
    setTimeout(function() { window.print(); }, 800);
  };
</script>
</body>''')
    return content, 200, {'Content-Type': 'text/html; charset=utf-8'}


if __name__ == '__main__':
    print("=" * 60)
    print("  A股量化选股工具 v5.2 (银级版)")
    print("  本机访问: http://localhost:5678")
    print("  数据源: 腾讯行情(15s刷新) + SQLite财务DB(毫秒级) + 新浪K线(技术)")
    print("  新增: 8套预设策略 | 本地财务DB | 实时数据 | 自定义策略")
    print("=" * 60)

    # ── 启动时预热：行情 + 检查财务DB是否有数据 ──
    def _warmup():
        try:
            fetch_tencent_market()
        except Exception as e:
            print(f'[WARN] 预热失败: {e}')
    threading.Thread(target=_warmup, daemon=True).start()
    fin_stats = fin_db.get_stats()
    if fin_stats['count'] == 0:
        print("\n  ⚠ 本地财务数据库为空！")
        print("  首次使用请在浏览器中点击「构建财务快照」，约需2-5分钟")
        print("  构建完成后筛选速度从30秒降到3秒以内\n")
    else:
        update_time = fin_stats.get('latest_update', '未知')
        print(f"\n  ✓ 本地财务数据库: {fin_stats['count']} 只 (更新于 {update_time})\n")

    app.run(host='0.0.0.0', port=5678, debug=False, threaded=True)
