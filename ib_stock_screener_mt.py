"""
美股筛选器 v10（纯 reqFundamentalData + SQLite 缓存，无 reqMktData）

数据来源（仅两个 reqFundamentalData 请求，均走缓存）：

  reqFundamentalData("ReportsFinSummary")
    <EPSs currency='CNY'>
      <EPS reportType='P'   period='12M'> ← 分析师全年预测 EPS（真正 Forward）
      <EPS reportType='TTM' period='12M'> ← 滚动12个月实际 EPS（备用）

  reqFundamentalData("ReportSnapshot")
    <Ratios ExchangeRate='0.14618' ReportingCurrency='CNY' PriceCurrency='USD'>
      Group 'Price and Volume': NPRICE, EV
      Group 'Income Statement': MKTCAP
      Group 'Per share data':   QCSHPS（每股现金含短期投资）, QBVPS（每股账面价值）
      Group 'Other Ratios':     PEEXCLXOR（TTM PE 兜底）

  净现金计算（经 PDD/AAPL/BABA 验证，误差<1%）：
    shares   = MKTCAP / NPRICE
    cash_m   = QCSHPS × shares          ← 现金+短期投资（百万USD）
    debt_m   = QTOTD2EQ/100 × QBVPS × shares ← 总债务（百万USD）
    net_cash = cash_m - debt_m

Forward PE 计算（经验证）：
  PDD:  P-12M EPS 无数据 → TTM=73.36 CNY × 0.14618 = 10.72 USD → PE=105.39/10.72=9.83
  AAPL: P-12M EPS=7.49 USD × 1.0 → Forward PE=272.95/7.49=36.4

EPS 优先级：
  1. reportType='P' period='12M'  最新日期   ← 分析师全年预测（真正 Forward）
  2. reportType='TTM' period='12M' 最新日期  ← 滚动实际（历史）
  3. PEEXCLXOR（fundamentalRatios）          ← 最终兜底

筛选条件：
  1. Forward PE < FORWARD_PE_MAX
  2. -NetDebt_I > 0，即现金及等价物 + 短期投资 > 总债务（净现金为正）

依赖：pip install ib_insync pandas tqdm
缓存：自动创建 ib_cache.db（SQLite，无需额外安装）
"""

import asyncio
import time
import queue
import logging
import threading
import xml.etree.ElementTree as ET
import sqlite3
import json
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from tqdm import tqdm
from ib_insync import IB, Stock

# ─── 配置 ──────────────────────────────────────────────────────────────────────
IB_HOST        = "127.0.0.1"
IB_PORT        = 4001
BASE_CLIENT_ID = 10

TICKER_FILE    = "all_tickers.txt"
EXCHANGE       = "SMART"
CURRENCY       = "USD"

FORWARD_PE_MAX = 20.0
NUM_WORKERS    = 18
REQUEST_DELAY  = 0.5
MAX_RETRY      = 2
RETRY_DELAY    = 3.0

OUTPUT_CSV     = "screened_stocks.csv"
CACHE_DB       = "ib_cache.db"     # SQLite 缓存文件
CACHE_TTL_DAYS = 45                # 缓存有效期（天），建议 30~60
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)



# ─── SQLite 缓存 ─────────────────────────────────────────────────────────────────
class FundamentalCache:
    """
    线程安全的 SQLite 缓存。
    存储三类原始 XML/数据，按 ticker + report_type 作为 key。
    过期时间由 CACHE_TTL_DAYS 控制。
    """

    def __init__(self, db_path: str, ttl_days: int):
        self._db    = db_path
        self._ttl   = timedelta(days=ttl_days)
        self._lock  = threading.Lock()
        self._init_db()

    def _conn(self):
        # check_same_thread=False + 外部锁保证线程安全
        return sqlite3.connect(self._db, check_same_thread=False)

    def _init_db(self):
        with self._lock, self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache (
                    ticker      TEXT NOT NULL,
                    report_type TEXT NOT NULL,
                    data        TEXT NOT NULL,
                    fetched_at  TEXT NOT NULL,
                    PRIMARY KEY (ticker, report_type)
                )
            """)
            conn.commit()

    def get(self, ticker: str, report_type: str) -> Optional[str]:
        """返回未过期的缓存数据，否则返回 None。"""
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT data, fetched_at FROM cache WHERE ticker=? AND report_type=?",
                (ticker, report_type)
            ).fetchone()
        if row is None:
            return None
        data, fetched_at = row
        age = datetime.utcnow() - datetime.fromisoformat(fetched_at)
        if age > self._ttl:
            log.debug(f"[cache] {ticker}/{report_type} 已过期({age.days}天)")
            return None
        return data

    def set(self, ticker: str, report_type: str, data: str):
        """写入或更新缓存。"""
        now = datetime.utcnow().isoformat()
        with self._lock, self._conn() as conn:
            conn.execute(
                """INSERT INTO cache(ticker, report_type, data, fetched_at)
                   VALUES(?,?,?,?)
                   ON CONFLICT(ticker, report_type) DO UPDATE
                   SET data=excluded.data, fetched_at=excluded.fetched_at""",
                (ticker, report_type, data, now)
            )
            conn.commit()

    def invalidate(self, ticker: str):
        """手动清除某只股票的所有缓存（调试用）。"""
        with self._lock, self._conn() as conn:
            conn.execute("DELETE FROM cache WHERE ticker=?", (ticker,))
            conn.commit()
        log.info(f"[cache] 已清除 {ticker} 的缓存")

    def stats(self) -> dict:
        """返回缓存统计信息。"""
        with self._lock, self._conn() as conn:
            total  = conn.execute("SELECT COUNT(*) FROM cache").fetchone()[0]
            fresh  = conn.execute(
                "SELECT COUNT(*) FROM cache WHERE fetched_at > ?",
                ((datetime.utcnow() - self._ttl).isoformat(),)
            ).fetchone()[0]
        return {"total": total, "fresh": fresh, "expired": total - fresh}


_cache = FundamentalCache(CACHE_DB, CACHE_TTL_DAYS)

# ─── 令牌桶 ─────────────────────────────────────────────────────────────────────
class TokenBucket:
    def __init__(self, rate: float, capacity: float):
        self._rate, self._capacity = rate, capacity
        self._tokens = capacity
        self._lock   = threading.Lock()
        self._last   = time.monotonic()

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            self._tokens = min(self._capacity,
                               self._tokens + (now - self._last) * self._rate)
            self._last = now
            if self._tokens < 1:
                time.sleep((1 - self._tokens) / self._rate)
                self._tokens = 0
            else:
                self._tokens -= 1

_limiter = TokenBucket(rate=NUM_WORKERS * 1.2, capacity=NUM_WORKERS * 2)


# ─── 数据结构 ───────────────────────────────────────────────────────────────────
@dataclass
class StockResult:
    ticker       : str
    forward_pe   : Optional[float] = None
    eps_local    : Optional[float] = None   # EPS（报表货币）
    eps_usd      : Optional[float] = None   # EPS（USD）
    eps_type     : str             = ""     # P=预测/TTM=滚动/PEEXCLXOR=兜底
    eps_date     : str             = ""     # EPS 所属日期
    eps_currency : str             = ""     # 报表货币
    exchange_rate: Optional[float] = None   # 汇率
    price        : Optional[float] = None
    net_debt_m   : Optional[float] = None   # NetDebt_I = 总债务−(现金+短期投资)，百万USD；正=净债务
    net_cash_m   : Optional[float] = None   # -NetDebt_I = 现金+短期投资−总债务；正=净现金 ✅筛选用
    mktcap_m     : Optional[float] = None
    pe_ttm       : Optional[float] = None   # PEEXCLXOR 参考
    status       : str             = "pending"
    reason       : str             = ""


# ─── ReportsFinSummary 解析：提取 EPS 序列 ──────────────────────────────────────
def parse_finsummary(xml_str: str) -> Optional[dict]:
    """
    从 ReportsFinSummary XML 提取：
      - EPS 货币（<EPSs currency='CNY'>）
      - Projection 12M EPS（reportType='P', period='12M'，取最新日期）← Forward
      - TTM 12M EPS（reportType='TTM', period='12M'，取最新日期）← 备用
    """
    if not xml_str or len(xml_str) < 50:
        return None
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return None

    eps_node = root.find("EPSs")
    if eps_node is None:
        return None

    currency = eps_node.get("currency", "USD")
    proj_12m = []
    ttm_12m  = []

    for e in eps_node.findall("EPS"):
        try:
            val = float(e.text)
        except (TypeError, ValueError):
            continue
        rtype  = e.get("reportType", "")
        period = e.get("period", "")
        date   = e.get("asofDate", "")

        if period == "12M":
            if rtype == "P":
                proj_12m.append((date, val))
            elif rtype == "TTM":
                ttm_12m.append((date, val))

    proj_12m.sort(reverse=True)
    ttm_12m.sort(reverse=True)

    return dict(
        currency  = currency,
        proj_eps  = proj_12m[0][1] if proj_12m else None,
        proj_date = proj_12m[0][0] if proj_12m else None,
        ttm_eps   = ttm_12m[0][1]  if ttm_12m  else None,
        ttm_date  = ttm_12m[0][0]  if ttm_12m  else None,
    )


# ─── ReportSnapshot 解析：汇率 + Ratio 字段 ──────────────────────────────────────
def parse_snapshot(xml_str: str) -> Optional[dict]:
    """
    从 ReportSnapshot XML 一次性提取：
      - ExchangeRate, ReportingCurrency
      - NPRICE, MKTCAP, EV（企业价值）, PEEXCLXOR
    所有 Ratio 值均为 USD（PriceCurrency=USD）。
    净现金 = MKTCAP - EV（企业价值）
    验证：PDD 149616-88437=61179M ✅  AAPL 4013103-4036705=-23602M ✅
    """
    if not xml_str or len(xml_str) < 50:
        return None
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return None

    ratios_node = root.find(".//Ratios")
    if ratios_node is None:
        return None

    # 汇率
    try:
        fx_rate = float(ratios_node.get("ExchangeRate", "1.0"))
    except (ValueError, TypeError):
        fx_rate = 1.0
    rep_ccy = ratios_node.get("ReportingCurrency", "USD")

    # 所有 Ratio 字段 → dict
    def gv(fn):
        elem = ratios_node.find(f".//*[@FieldName='{fn}']")
        if elem is None or not (elem.text or "").strip():
            return None
        try:
            v = float(elem.text)
            return None if v in (-1.0, -99999.99) else v
        except (ValueError, TypeError):
            return None

    nprice = gv("NPRICE")
    mktcap = gv("MKTCAP")
    ev     = gv("EV")        # 企业价值 = MKTCAP + 总债务 - 现金
    pe_ttm = gv("PEEXCLXOR")

    # 净现金 = MKTCAP - EV = 现金 - 总债务
    # 验证: PDD  149616 - 88437   = +61179M ✅（净现金）
    #       AAPL 4013103 - 4036705 = -23602M ✅（净债务）
    net_cash_m = (mktcap - ev) if (mktcap is not None and ev is not None) else None
    return dict(
        fx_rate    = fx_rate,
        rep_ccy    = rep_ccy,
        price      = nprice,
        mktcap_m   = mktcap,
        net_cash_m = net_cash_m,
        pe_ttm     = pe_ttm,
    )

# ─── ReportSnapshot 解析：提取 ExchangeRate ──────────────────────────────────────
def parse_exchange_rate(xml_str: str) -> tuple[float, str]:
    """
    从 ReportSnapshot XML 提取汇率。
    <Ratios ExchangeRate='0.14618' ReportingCurrency='CNY' PriceCurrency='USD'>
    返回 (exchange_rate, reporting_currency)
    """
    if not xml_str or len(xml_str) < 50:
        return 1.0, "USD"
    try:
        root = ET.fromstring(xml_str)
        ratios = root.find(".//Ratios")
        if ratios is not None:
            fx  = float(ratios.get("ExchangeRate", "1.0"))
            ccy = ratios.get("ReportingCurrency", "USD")
            return fx, ccy
    except (ET.ParseError, ValueError, TypeError):
        pass
    return 1.0, "USD"





# ─── 单股处理 ────────────────────────────────────────────────────────────────────
def process_ticker(ticker: str, ib: IB) -> StockResult:
    res      = StockResult(ticker=ticker)
    contract = Stock(ticker, EXCHANGE, CURRENCY)

    for attempt in range(1, MAX_RETRY + 2):
        try:
            _limiter.acquire()

            # ── 请求1: ReportsFinSummary（优先读缓存）────────────────────────
            xml_fs = _cache.get(ticker, "ReportsFinSummary")
            if xml_fs:
                log.debug(f"[{ticker}] ReportsFinSummary 命中缓存")
            else:
                xml_fs = ib.reqFundamentalData(contract, reportType="ReportsFinSummary")
                if xml_fs and len(xml_fs) > 100:
                    _cache.set(ticker, "ReportsFinSummary", xml_fs)
                time.sleep(REQUEST_DELAY)
            fs = parse_finsummary(xml_fs)

            # ── 请求2: ReportSnapshot（优先读缓存）──────────────────────────
            xml_snap = _cache.get(ticker, "ReportSnapshot")
            if xml_snap:
                log.debug(f"[{ticker}] ReportSnapshot 命中缓存")
            else:
                xml_snap = ib.reqFundamentalData(contract, reportType="ReportSnapshot")
                if xml_snap and len(xml_snap) > 100:
                    _cache.set(ticker, "ReportSnapshot", xml_snap)
                time.sleep(REQUEST_DELAY)
            snap = parse_snapshot(xml_snap)

            # ── 整合数据（无需 reqMktData）───────────────────────────────────
            price      = snap["price"]      if snap else None
            net_cash_m = snap["net_cash_m"] if snap else None
            mktcap     = snap["mktcap_m"]   if snap else None
            pe_ttm     = snap["pe_ttm"]     if snap else None
            fx_rate    = snap["fx_rate"]    if snap else 1.0
            rep_ccy    = snap["rep_ccy"]    if snap else "USD" 

            # ── EPS 选取优先级 ────────────────────────────────────────────────
            # 1. Projection 12M（reportType='P'）← 真正的 Forward EPS
            # 2. TTM 12M（reportType='TTM'）     ← 历史滚动，备用
            # 3. PEEXCLXOR                        ← 最终兜底（直接是 PE，无需乘汇率）
            eps_local = None
            eps_type  = ""
            eps_date  = ""

            if fs:
                if fs["proj_eps"] is not None and fs["proj_eps"] > 0:
                    eps_local = fs["proj_eps"]
                    eps_type  = "P(Projection)"
                    eps_date  = fs["proj_date"] or ""
                elif fs["ttm_eps"] is not None and fs["ttm_eps"] > 0:
                    eps_local = fs["ttm_eps"]
                    eps_type  = "TTM"
                    eps_date  = fs["ttm_date"] or ""

            # ── Forward PE 计算 ───────────────────────────────────────────────
            forward_pe = None
            eps_usd    = None

            if price and eps_local and eps_local > 0:
                eps_usd    = eps_local * fx_rate
                forward_pe = round(price / eps_usd, 2)
            elif price and pe_ttm and pe_ttm > 0:
                # 兜底：PEEXCLXOR 已是 USD 对齐的 TTM PE
                forward_pe = round(pe_ttm, 2)
                eps_type   = "PEEXCLXOR(兜底)"

            # ── 填充结果 ──────────────────────────────────────────────────────
            res.forward_pe    = forward_pe
            res.eps_local     = eps_local
            res.eps_usd       = eps_usd
            res.eps_type      = eps_type
            res.eps_date      = eps_date
            res.eps_currency  = rep_ccy
            res.exchange_rate = fx_rate
            res.price         = price
            res.net_cash_m    = net_cash_m
            res.net_debt_m    = (-net_cash_m) if net_cash_m is not None else None
            res.mktcap_m      = mktcap
            res.pe_ttm        = pe_ttm

            # ── 筛选 ──────────────────────────────────────────────────────────
            net_cash = net_cash_m

            if forward_pe is None:
                res.status = "skipped"
                res.reason = "无法计算PE"
            elif net_cash is None:
                res.status = "skipped"
                res.reason = "无NetDebt_I"
            elif forward_pe < FORWARD_PE_MAX and net_cash > 0:
                res.status = "passed"
                res.reason = eps_type
            else:
                parts = []
                if forward_pe >= FORWARD_PE_MAX:
                    parts.append(f"PE={forward_pe:.1f}>={FORWARD_PE_MAX}")
                if net_cash <= 0:
                    parts.append(f"净现金={net_cash:.0f}M<=0")
                res.status = "failed"
                res.reason = "  ".join(parts) + f"  [{eps_type}]"

            return res

        except Exception as e:
            if attempt <= MAX_RETRY:
                log.warning(f"[{ticker}] 第{attempt}次: {e}，{RETRY_DELAY}s后重试")
                time.sleep(RETRY_DELAY)
            else:
                res.status = "error"
                res.reason = str(e)
                return res
        finally:
            time.sleep(REQUEST_DELAY)

    res.status = "error"
    return res


# ─── Worker 线程 ─────────────────────────────────────────────────────────────────
RECONNECT_WAIT    = 10   # 每次重连前等待（秒）
RECONNECT_RETRIES = 12   # 最多重连次数（12×10s ≈ 2 分钟）


def _connect_with_retry(ib: IB, client_id: int, tname: str) -> bool:
    """尝试连接，失败则重试，返回是否成功。"""
    for attempt in range(1, RECONNECT_RETRIES + 1):
        try:
            if ib.isConnected():
                return True
            ib.connect(IB_HOST, IB_PORT, clientId=client_id)
            log.info(f"[{tname}] 已连接 (clientId={client_id})")
            return True
        except Exception as e:
            log.warning(f"[{tname}] 连接失败({attempt}/{RECONNECT_RETRIES}): {e}，"
                        f"{RECONNECT_WAIT}s 后重试...")
            time.sleep(RECONNECT_WAIT)
    log.error(f"[{tname}] 重连耗尽，放弃")
    return False


def worker_main(worker_id: int, task_queue: queue.Queue,
                result_list: list, result_lock: threading.Lock, progress: tqdm):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    client_id = BASE_CLIENT_ID + worker_id
    tname     = threading.current_thread().name

    ib = IB()
    if not _connect_with_retry(ib, client_id, tname):
        loop.close()
        return

    try:
        while True:
            # ── 取任务 ────────────────────────────────────────────────────────
            try:
                ticker = task_queue.get_nowait()
            except queue.Empty:
                break

            # ── 处理每只股票前检查连接 ────────────────────────────────────────
            if not ib.isConnected():
                log.warning(f"[{tname}] 检测到断线，尝试重连...")
                try:
                    ib.disconnect()
                except Exception:
                    pass
                ib = IB()
                if not _connect_with_retry(ib, client_id, tname):
                    task_queue.put(ticker)
                    task_queue.task_done()
                    break

            res = process_ticker(ticker, ib)

            # ── 若因断线失败，重连后重新入队 ─────────────────────────────────
            if res.status == "error" and not ib.isConnected():
                log.warning(f"[{tname}] {ticker} 因断线失败，重连后重新入队")
                try:
                    ib.disconnect()
                except Exception:
                    pass
                ib = IB()
                if _connect_with_retry(ib, client_id, tname):
                    task_queue.put(ticker)
                task_queue.task_done()
                progress.update(1)
                continue

            icon = {"passed": "✅", "failed": "❌",
                    "skipped": "⚠️", "error": "🔴"}.get(res.status, "?")
            log.info(
                f"{icon} {ticker:8s}  "
                f"FwdPE={str(res.forward_pe or 'N/A'):>6}  "
                f"净现金={str(res.net_cash_m if res.net_cash_m is not None else 'N/A'):>10}M  "
                f"CCY={res.eps_currency:4s}  FX={res.exchange_rate or 'N/A'}  "
                f"[{res.eps_type}]"
            )

            with result_lock:
                result_list.append(res)
            progress.update(1)
            task_queue.task_done()
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass
        loop.close()
        log.info(f"[{tname}] 已退出")


# ─── 主函数 ──────────────────────────────────────────────────────────────────────
def load_tickers(filepath: str) -> list:
    with open(filepath, "r", encoding="utf-8") as f:
        return [l.strip().upper() for l in f
                if l.strip() and not l.strip().startswith("#")]


def main():
    tickers = load_tickers(TICKER_FILE)
    log.info(f"读取到 {len(tickers)} 只股票，启动 {NUM_WORKERS} 个 Worker")
    cs = _cache.stats()
    log.info(f"缓存状态: 共 {cs['total']} 条，有效 {cs['fresh']} 条，过期 {cs['expired']} 条（TTL={CACHE_TTL_DAYS}天）")
    log.info(f"筛选: Forward PE < {FORWARD_PE_MAX}  且  净现金 > 0")

    asyncio.set_event_loop(asyncio.new_event_loop())

    task_queue  = queue.Queue()
    result_list = []
    result_lock = threading.Lock()
    for t in tickers:
        task_queue.put(t)

    start  = time.time()
    actual = min(NUM_WORKERS, len(tickers))

    with tqdm(total=len(tickers), desc="筛选进度", unit="只") as pbar:
        threads = []
        for wid in range(actual):
            t = threading.Thread(
                target=worker_main,
                args=(wid, task_queue, result_list, result_lock, pbar),
                name=f"Worker-{wid:02d}", daemon=True,
            )
            threads.append(t)
            t.start()
            time.sleep(0.4)
        for t in threads:
            t.join()

    elapsed = time.time() - start

    passed  = [r for r in result_list if r.status == "passed"]
    failed  = [r for r in result_list if r.status == "failed"]
    skipped = [r for r in result_list if r.status in ("skipped", "error")]

    print(f"\n{'='*76}")
    print(f"  耗时 {elapsed:.0f}s ({elapsed/60:.1f}min)  |  "
          f"✅ {len(passed)} 通过  ❌ {len(failed)} 不符合  ⚠️ {len(skipped)} 跳过")
    print(f"{'='*76}")

    if not passed:
        print("没有股票符合条件。")
        if skipped:
            print("\n前10个跳过原因：")
            for r in skipped[:10]:
                print(f"  {r.ticker:8s} → {r.reason}")
        return

    rows = [{
        "Ticker"       : r.ticker,
        "Forward_PE"   : r.forward_pe,
        "EPS_Type"     : r.eps_type,
        "EPS_Date"     : r.eps_date,
        "EPS_Local"    : r.eps_local,
        "EPS_Currency" : r.eps_currency,
        "ExchangeRate" : r.exchange_rate,
        "EPS_USD"      : round(r.eps_usd, 4) if r.eps_usd else None,
        "Price"        : r.price,
        "NetCash_M"    : r.net_cash_m,
        "NetDebt_M"    : r.net_debt_m,
        "MktCap_M"     : r.mktcap_m,
        "PE_TTM"       : r.pe_ttm,
    } for r in passed]

    df = pd.DataFrame(rows).sort_values("Forward_PE").reset_index(drop=True)
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.width", 160)

    print(f"\n✅ 符合条件（Forward PE < {FORWARD_PE_MAX} 且 净现金 > 0）：")
    print(df.to_string(index=False))
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n📄 已保存: {OUTPUT_CSV}")
    print(f"   Forward PE 均值 : {df['Forward_PE'].mean():.2f}")
    print(f"   净现金中位数    : {df['NetCash_M'].median():.0f} M")


if __name__ == "__main__":
    main()
