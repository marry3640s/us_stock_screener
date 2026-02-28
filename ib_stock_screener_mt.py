"""
美股筛选器 v4（多线程 + fundamentalRatios 精确解析）
基于实测确认的 IB fundamentalRatios 字段：

  Forward PE  = NPRICE / AFEEPSNTM   (NTM预期EPS)
  净现金判断  = NetDebt_I < 0        (NetDebt_I 单位：百万，负值=净现金为正)

筛选条件：
  1. Forward PE (NPRICE / AFEEPSNTM) < 20
  2. NetDebt_I < 0  (即净现金 > 总债务)
  3. 股票列表从 tickers.txt 读取

依赖：pip install ib_insync pandas tqdm
前提：TWS/Gateway 已运行，已有基础行情订阅（无需 Reuters Fundamentals）
"""

import asyncio
import time
import queue
import logging
import threading
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from tqdm import tqdm
from ib_insync import IB, Stock

# ─── 配置 ──────────────────────────────────────────────────────────────────────
IB_HOST        = "127.0.0.1"
IB_PORT        = 4001          # 模拟 7497 | 实盘 7496 | Gateway 4002
BASE_CLIENT_ID = 10

TICKER_FILE    = "all_tickers.txt"
EXCHANGE       = "SMART"
CURRENCY       = "USD"

FORWARD_PE_MAX = 20.0
NUM_WORKERS    = 8            # 并发线程数（每个线程一个 IB 连接）
MKT_DATA_WAIT  = 4.0           # reqMktData 等待时间（秒），太短会拿不到数据
REQUEST_DELAY  = 0.5           # 每次请求后冷却
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
    ticker       : str
    company      : str             = ""
    forward_pe   : Optional[float] = None   # NPRICE / AFEEPSNTM
    ntm_eps      : Optional[float] = None   # AFEEPSNTM
    price        : Optional[float] = None   # NPRICE
    net_debt_m   : Optional[float] = None   # NetDebt_I（负=净现金）
    net_cash_m   : Optional[float] = None   # -NetDebt_I
    mktcap_m     : Optional[float] = None   # MKTCAP
    pe_ttm       : Optional[float] = None   # PEEXCLXOR（仅供参考）
    status       : str             = "pending"
    reason       : str             = ""


# ─── fundamentalRatios 解析 ─────────────────────────────────────────────────────
def parse_fr(fr) -> Optional[dict]:
    """
    从 FundamentalRatios 对象提取所需字段。
    实测可用字段（来自 AAPL）：
      AFEEPSNTM  — NTM 预期 EPS（Forward EPS，分析师共识）
      NPRICE     — 最新价格
      NetDebt_I  — 净债务（百万，负值表示净现金为正）
      PEEXCLXOR  — TTM PE（排除特殊项）
      MKTCAP     — 市值（百万）
    """
    if fr is None:
        return None

    def g(attr):
        """安全取属性，-1 视为无效"""
        v = getattr(fr, attr, None)
        if v is None or v == -1:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    ntm_eps  = g("AFEEPSNTM")
    price    = g("NPRICE")
    net_debt = g("NetDebt_I")   # 百万，负=净现金 > 0

    # Forward PE 计算
    forward_pe = None
    if price and ntm_eps and ntm_eps > 0:
        forward_pe = round(price / ntm_eps, 2)

    return dict(
        forward_pe = forward_pe,
        ntm_eps    = ntm_eps,
        price      = price,
        net_debt_m = net_debt,
        net_cash_m = (-net_debt) if net_debt is not None else None,
        mktcap_m   = g("MKTCAP"),
        pe_ttm     = g("PEEXCLXOR"),
    )


# ─── 单股处理 ────────────────────────────────────────────────────────────────────
def process_ticker(ticker: str, ib: IB) -> StockResult:
    res = StockResult(ticker=ticker)
    contract = Stock(ticker, EXCHANGE, CURRENCY)

    for attempt in range(1, MAX_RETRY + 2):
        mkt = None
        try:
            _limiter.acquire()

            # snapshot=False + sleep 才能拿到 fundamentalRatios
            mkt = ib.reqMktData(contract, genericTickList="47", snapshot=False)
            ib.sleep(MKT_DATA_WAIT)

            d = parse_fr(mkt.fundamentalRatios)

            if d is None:
                res.status = "skipped"
                res.reason = "fundamentalRatios=None"
                return res

            res.forward_pe = d["forward_pe"]
            res.ntm_eps    = d["ntm_eps"]
            res.price      = d["price"]
            res.net_debt_m = d["net_debt_m"]
            res.net_cash_m = d["net_cash_m"]
            res.mktcap_m   = d["mktcap_m"]
            res.pe_ttm     = d["pe_ttm"]

            # ── 筛选逻辑 ──────────────────────────────────────────────────
            fpe       = res.forward_pe
            net_cash  = res.net_cash_m   # 负 net_debt = 正 net_cash

            if fpe is None:
                res.status = "skipped"
                res.reason = f"无AFEEPSNTM或NPRICE (ntm_eps={d['ntm_eps']}, price={d['price']})"
            elif net_cash is None:
                res.status = "skipped"
                res.reason = "无NetDebt_I"
            elif fpe < FORWARD_PE_MAX and net_cash > 0:
                res.status = "passed"
            else:
                res.status = "failed"
                res.reason = (
                    f"PE={fpe:.1f}>={FORWARD_PE_MAX}" if fpe >= FORWARD_PE_MAX else ""
                ) + (
                    f" 净现金={net_cash:.0f}M<=0" if net_cash <= 0 else ""
                )

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
    # 每个子线程必须创建独立的 asyncio 事件循环
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
                f"净现金={res.net_cash_m if res.net_cash_m is not None else 'N/A'!s:>9}M  "
                f"{'[' + res.reason + ']' if res.reason else ''}"
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
    log.info(f"筛选条件: Forward PE < {FORWARD_PE_MAX}  且  净现金(NetDebt_I<0) > 0")

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
            time.sleep(0.4)   # 错开连接
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
        # 打印前10个 skipped 的原因，辅助调试
        print("\n前10个跳过原因：")
        for r in skipped[:10]:
            print(f"  {r.ticker:8s} → {r.reason}")
        return

    # 构建输出 DataFrame
    rows = [{
        "Ticker"     : r.ticker,
        "Forward_PE" : r.forward_pe,
        "NTM_EPS"    : r.ntm_eps,
        "Price"      : r.price,
        "NetCash_M"  : r.net_cash_m,
        "NetDebt_M"  : r.net_debt_m,
        "MktCap_M"   : r.mktcap_m,
        "TTM_PE"     : r.pe_ttm,
    } for r in passed]

    df = pd.DataFrame(rows).sort_values("Forward_PE").reset_index(drop=True)

    pd.set_option("display.max_rows", 500)
    pd.set_option("display.width", 120)
    print(f"\n✅ 符合条件的股票（Forward PE < {FORWARD_PE_MAX} 且 净现金 > 0）：")
    print(df.to_string(index=False))

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n📄 结果已保存: {OUTPUT_CSV}")
    print(f"   Forward PE 均值: {df['Forward_PE'].mean():.2f}")
    print(f"   净现金中位数:    {df['NetCash_M'].median():.0f} M")


if __name__ == "__main__":
    main()