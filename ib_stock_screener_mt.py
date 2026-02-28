"""
美股筛选器 v6（多线程 + fundamentalRatios 精确解析）

字段货币说明（经 PDD/BABA/AAPL 实测验证）：
  TTMEPSXCLX — TTM EPS，IB 已统一换算为 USD  ✅ 直接用
  AEPSNORM   — 标准化年度 EPS，IB 已换算 USD  ✅ 直接用
  AFEEPSNTM  — 分析师预期 EPS，原始本地货币  ❌ 不可直接用（中概股为人民币）
  PEEXCLXOR  — IB 直接算好的 TTM PE（货币已对齐） ✅ 最可靠，用作兜底
  NPRICE     — 最新股价（USD）
  NetDebt_I  — 净债务百万（负值=净现金为正）
  CURRENCY   — 仅为标注字段，所有计算字段已统一为 USD

验证：
  PDD:  NPRICE=105.39 / TTMEPSXCLX=10.075 = 10.46 = PEEXCLXOR ✅
  AAPL: NPRICE=272.95 / TTMEPSXCLX=7.871  = 34.67 = PEEXCLXOR ✅

Forward PE 计算优先级：
  1. NPRICE / TTMEPSXCLX   （TTM实际，USD已对齐）
  2. NPRICE / AEPSNORM     （标准化，USD已对齐）
  3. PEEXCLXOR             （IB直接，兜底）

筛选条件：
  1. Forward PE < 20
  2. NetDebt_I < 0（净现金 > 总债务）

依赖：pip install ib_insync pandas tqdm
"""

import asyncio
import time
import queue
import logging
import threading
from dataclasses import dataclass, field
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
NUM_WORKERS    = 10
MKT_DATA_WAIT  = 4.0    # reqMktData 推送等待（秒）
FX_WAIT        = 3.0    # Forex 汇率请求等待（秒）
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




# ─── 全局令牌桶 ─────────────────────────────────────────────────────────────────
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
    ticker     : str
    forward_pe : Optional[float] = None   # NPRICE / TTMEPSXCLX（USD已对齐）
    pe_src     : str             = ""     # TTM / NORM / PEEXCLXOR(兜底)
    ttm_eps    : Optional[float] = None   # TTMEPSXCLX（USD）
    price      : Optional[float] = None   # NPRICE（USD）
    net_debt_m : Optional[float] = None   # NetDebt_I（百万，负=净现金）
    net_cash_m : Optional[float] = None   # -NetDebt_I
    mktcap_m   : Optional[float] = None   # MKTCAP（百万）
    pe_ttm_ib  : Optional[float] = None   # PEEXCLXOR（IB直接算，供校验）
    status     : str             = "pending"
    reason     : str             = ""


# ─── fundamentalRatios 解析 + PE 计算 ──────────────────────────────────────────
def parse_fr(fr) -> Optional[dict]:
    """
    解析 FundamentalRatios。
    所有 IB 计算字段（TTMEPSXCLX、AEPSNORM、PEEXCLXOR）已统一为 USD，直接使用。
    AFEEPSNTM 为原始本地货币（中概股是人民币），不参与计算。
    """
    if fr is None:
        return None

    def g(attr):
        """取正浮点，-1 和 0.0 视为无效（IB 填充值）"""
        v = getattr(fr, attr, None)
        if v is None:
            return None
        try:
            f = float(v)
            return None if f in (-1.0, 0.0) else f
        except (TypeError, ValueError):
            return None

    def g0(attr):
        """取浮点，允许 0.0（NetDebt=0 合法）"""
        v = getattr(fr, attr, None)
        if v is None:
            return None
        try:
            f = float(v)
            return None if f == -1.0 else f
        except (TypeError, ValueError):
            return None

    price    = g("NPRICE")          # USD
    ttm_eps  = g("TTMEPSXCLX")     # USD（IB已换算）✅
    norm_eps = g("AEPSNORM")       # USD（IB已换算）✅
    pe_ib    = g0("PEEXCLXOR")     # IB直接算的TTM PE，货币已对齐 ✅

    # Forward PE 优先级：
    #   1. NPRICE / TTMEPSXCLX  — TTM实际盈利，USD，最准
    #   2. NPRICE / AEPSNORM    — 标准化盈利，USD
    #   3. PEEXCLXOR            — IB直接，兜底
    forward_pe, pe_src = None, ""
    if price and ttm_eps and ttm_eps > 0:
        forward_pe, pe_src = round(price / ttm_eps, 2), "TTM"
    elif price and norm_eps and norm_eps > 0:
        forward_pe, pe_src = round(price / norm_eps, 2), "NORM"
    elif pe_ib and pe_ib > 0:
        forward_pe, pe_src = round(pe_ib, 2), "PEEXCLXOR(兜底)"

    return dict(
        forward_pe = forward_pe,
        pe_src     = pe_src,
        ttm_eps    = ttm_eps,
        price      = price,
        pe_ttm_ib  = pe_ib,
        net_debt_m = g0("NetDebt_I"),
        mktcap_m   = g("MKTCAP"),
    )


# ─── 单股处理 ────────────────────────────────────────────────────────────────────
def process_ticker(ticker: str, ib: IB) -> StockResult:
    res = StockResult(ticker=ticker)
    contract = Stock(ticker, EXCHANGE, CURRENCY)

    for attempt in range(1, MAX_RETRY + 2):
        mkt = None
        try:
            _limiter.acquire()
            mkt = ib.reqMktData(contract, genericTickList="47", snapshot=False)
            ib.sleep(MKT_DATA_WAIT)

            d = parse_fr(mkt.fundamentalRatios)
            if d is None:
                res.status = "skipped"
                res.reason = "fundamentalRatios=None"
                return res

            # 填充结果
            res.forward_pe = d["forward_pe"]
            res.pe_src     = d["pe_src"]
            res.ttm_eps    = d["ttm_eps"]
            res.price      = d["price"]
            res.pe_ttm_ib  = d["pe_ttm_ib"]
            res.mktcap_m   = d["mktcap_m"]

            net_debt       = d["net_debt_m"]
            res.net_debt_m = net_debt
            res.net_cash_m = (-net_debt) if net_debt is not None else None

            fpe      = res.forward_pe
            net_cash = res.net_cash_m

            # ── 筛选 ──────────────────────────────────────────────────────
            if fpe is None:
                res.status = "skipped"
                res.reason = f"无法计算PE(ttm_eps={d['ttm_eps']},price={d['price']})"
            elif net_cash is None:
                res.status = "skipped"
                res.reason = "无NetDebt_I"
            elif fpe < FORWARD_PE_MAX and net_cash > 0:
                res.status = "passed"
                res.reason = d["pe_src"]
            else:
                res.status = "failed"
                parts = []
                if fpe >= FORWARD_PE_MAX:
                    parts.append(f"PE={fpe:.1f}>={FORWARD_PE_MAX}")
                if net_cash <= 0:
                    parts.append(f"净现金={net_cash:.0f}M<=0")
                res.reason = "  ".join(parts) + f"  [{d['pe_src']}]"

            return res

        except Exception as e:
            if attempt <= MAX_RETRY:
                log.warning(f"[{ticker}] 第{attempt}次异常: {e}，{RETRY_DELAY}s后重试")
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
            icon = {"passed":"✅","failed":"❌","skipped":"⚠️","error":"🔴"}.get(res.status,"?")
            log.info(
                f"{icon} {ticker:8s}  "
                f"FwdPE={str(res.forward_pe or 'N/A'):>6}  "
                f"净现金={res.net_cash_m if res.net_cash_m is not None else 'N/A'!s:>10}M  "
                f"src={res.pe_src:12s}  "
                f"{res.reason[:50]}"
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
    log.info(f"读取到 {len(tickers)} 只股票")

    # 多线程筛选
    asyncio.set_event_loop(asyncio.new_event_loop())

    task_queue  = queue.Queue()
    result_list = []
    result_lock = threading.Lock()
    for t in tickers:
        task_queue.put(t)

    log.info(f"启动 {NUM_WORKERS} 个 Worker，筛选条件: Forward PE < {FORWARD_PE_MAX} 且 净现金 > 0")
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
        "Ticker"    : r.ticker,
        "Forward_PE": r.forward_pe,
        "PE_Src"    : r.pe_src,
        "TTM_EPS"   : r.ttm_eps,
        "Price"     : r.price,
        "NetCash_M" : r.net_cash_m,
        "NetDebt_M" : r.net_debt_m,
        "MktCap_M"  : r.mktcap_m,
        "PE_TTM_IB" : r.pe_ttm_ib,
    } for r in passed]

    df = pd.DataFrame(rows).sort_values("Forward_PE").reset_index(drop=True)
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.width", 140)

    print(f"\n✅ 符合条件（Forward PE < {FORWARD_PE_MAX} 且 净现金 > 0）：")
    print(df.to_string(index=False))
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n📄 已保存: {OUTPUT_CSV}")
    print(f"   Forward PE 均值: {df['Forward_PE'].mean():.2f}")
    print(f"   净现金中位数:    {df['NetCash_M'].median():.0f} M")


if __name__ == "__main__":
    main()
