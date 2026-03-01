"""
美股筛选器 v8（ReportsFinSummary + ExchangeRate 精确 Forward PE）

数据来源：
  reqFundamentalData("ReportsFinSummary")
    <EPSs currency='CNY'>          ← EPS 货币
      <EPS reportType='P' period='12M'>  ← Projection 全年 EPS（分析师预测）✅ Forward
      <EPS reportType='TTM' period='12M'> ← 滚动12个月实际 EPS
      <EPS reportType='A' period='3M'>    ← 季度实际 EPS

  reqFundamentalData("ReportSnapshot")
    <Ratios ExchangeRate='0.14618' ReportingCurrency='CNY'>  ← 报表货币→USD 汇率

  reqMktData(genericTickList="47")
    NPRICE    ← 当前股价（USD）
    NetDebt_I ← 净债务（百万，负=净现金）
    PEEXCLXOR ← IB算的TTM PE（兜底用）

Forward PE 计算（经验证）：
  PDD:  P-12M EPS 无数据 → TTM=73.36 CNY × 0.14618 = 10.72 USD → PE=105.39/10.72=9.83
  AAPL: P-12M EPS=7.49 USD × 1.0 → Forward PE=272.95/7.49=36.4

EPS 优先级：
  1. reportType='P' period='12M'  最新日期   ← 分析师全年预测（真正 Forward）
  2. reportType='TTM' period='12M' 最新日期  ← 滚动实际（历史）
  3. PEEXCLXOR（fundamentalRatios）          ← 最终兜底

筛选条件：
  1. Forward PE < FORWARD_PE_MAX
  2. NetDebt_I < 0（净现金 > 总债务）

依赖：pip install ib_insync pandas tqdm
"""

import asyncio
import time
import queue
import logging
import threading
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from tqdm import tqdm
from ib_insync import IB, Stock

# ─── 配置 ──────────────────────────────────────────────────────────────────────
IB_HOST        = "127.0.0.1"
IB_PORT        = 4001
BASE_CLIENT_ID = 10

TICKER_FILE    = "tickers.txt"
EXCHANGE       = "SMART"
CURRENCY       = "USD"

FORWARD_PE_MAX = 20.0
NUM_WORKERS    = 20
MKT_DATA_WAIT  = 4.0
REQUEST_DELAY  = 0.5
MAX_RETRY      = 2
RETRY_DELAY    = 3.0

OUTPUT_CSV     = "screened_stocks.csv"
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


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
    net_debt_m   : Optional[float] = None
    net_cash_m   : Optional[float] = None
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


# ─── fundamentalRatios 解析：提取 NetDebt、Price、PE 兜底 ────────────────────────
def parse_fr(fr) -> Optional[dict]:
    if fr is None:
        return None

    def g(attr):
        v = getattr(fr, attr, None)
        if v is None:
            return None
        try:
            f = float(v)
            return None if f in (-1.0, 0.0) else f
        except (TypeError, ValueError):
            return None

    def g0(attr):
        v = getattr(fr, attr, None)
        if v is None:
            return None
        try:
            f = float(v)
            return None if f == -1.0 else f
        except (TypeError, ValueError):
            return None

    return dict(
        price      = g("NPRICE"),
        net_debt_m = g0("NetDebt_I"),
        mktcap_m   = g("MKTCAP"),
        pe_ttm     = g0("PEEXCLXOR"),  # IB 算好的 TTM PE，兜底用
    )


# ─── 单股处理 ────────────────────────────────────────────────────────────────────
def process_ticker(ticker: str, ib: IB) -> StockResult:
    res      = StockResult(ticker=ticker)
    contract = Stock(ticker, EXCHANGE, CURRENCY)

    for attempt in range(1, MAX_RETRY + 2):
        mkt = None
        try:
            _limiter.acquire()

            # ── 请求1: ReportsFinSummary（EPS 序列 + 货币）───────────────────
            xml_fs = ib.reqFundamentalData(contract, reportType="ReportsFinSummary")
            fs     = parse_finsummary(xml_fs)
            time.sleep(REQUEST_DELAY)

            # ── 请求2: ReportSnapshot（ExchangeRate）────────────────────────
            xml_snap = ib.reqFundamentalData(contract, reportType="ReportSnapshot")
            fx_rate, rep_ccy = parse_exchange_rate(xml_snap)
            time.sleep(REQUEST_DELAY)

            # ── 请求3: reqMktData（NetDebt_I、NPRICE、PE 兜底）──────────────
            mkt     = ib.reqMktData(contract, genericTickList="47", snapshot=False)
            ib.sleep(MKT_DATA_WAIT)
            fr_data = parse_fr(mkt.fundamentalRatios)

            # ── 整合价格和净债务 ──────────────────────────────────────────────
            price    = fr_data["price"]    if fr_data else None
            net_debt = fr_data["net_debt_m"] if fr_data else None
            mktcap   = fr_data["mktcap_m"]   if fr_data else None
            pe_ttm   = fr_data["pe_ttm"]      if fr_data else None

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
            res.net_debt_m    = net_debt
            res.net_cash_m    = (-net_debt) if net_debt is not None else None
            res.mktcap_m      = mktcap
            res.pe_ttm        = pe_ttm

            # ── 筛选 ──────────────────────────────────────────────────────────
            net_cash = res.net_cash_m

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
            if mkt is not None:
                try:
                    ib.cancelMktData(contract)
                except Exception:
                    pass
            time.sleep(REQUEST_DELAY)

    res.status = "error"
    return res


# ─── Worker 线程 ─────────────────────────────────────────────────────────────────
def worker_main(worker_id: int, task_queue: queue.Queue,
                result_list: list, result_lock: threading.Lock, progress: tqdm):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    client_id = BASE_CLIENT_ID + worker_id
    tname     = threading.current_thread().name

    ib = IB()
    try:
        ib.connect(IB_HOST, IB_PORT, clientId=client_id)
        log.info(f"[{tname}] 已连接 (clientId={client_id})")
    except Exception as e:
        log.error(f"[{tname}] 连接失败: {e}")
        loop.close()
        return

    try:
        while True:
            try:
                ticker = task_queue.get_nowait()
            except queue.Empty:
                break

            res  = process_ticker(ticker, ib)
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
        ib.disconnect()
        loop.close()
        log.info(f"[{tname}] 已断开")


# ─── 主函数 ──────────────────────────────────────────────────────────────────────
def load_tickers(filepath: str) -> list:
    with open(filepath, "r", encoding="utf-8") as f:
        return [l.strip().upper() for l in f
                if l.strip() and not l.strip().startswith("#")]


def main():
    tickers = load_tickers(TICKER_FILE)
    log.info(f"读取到 {len(tickers)} 只股票，启动 {NUM_WORKERS} 个 Worker")
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
