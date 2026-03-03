"""
SEC 财报下载器

核心思路：
  美股（10-K / 10-Q）：从 submissions.json 取最新的，直接下载。
  外国公司（6-K / 20-F）：
    1. 扫描 master.idx，找该公司所有 6-K
       master.idx 格式：CIK|Company|Form|Date|edgar/data/{CIK}/{accnodash}.txt
       那个 .txt 文件本身就是财报内容（SGML 打包格式）
    2. 按日期从新到旧，对每个 .txt 发 HEAD 请求检查文件大小
    3. 大于 MIN_6K_BYTES（200KB）的视为财务 6-K
    4. 与最新 20-F 比日期，谁新下谁

文件保存到 ./sec-data/<TICKER>/ 目录
依赖：pip install requests tqdm
"""

import re
import time
import logging
from typing import Optional
from datetime import datetime
import requests
from pathlib import Path
from tqdm import tqdm

# ─── 配置 ──────────────────────────────────────────────────────────────────────
TICKER_FILE   = "tickers.txt"
OUTPUT_DIR    = "sec-data"
REQUEST_DELAY = 0.5
MAX_RETRY     = 3
RETRY_DELAY   = 5.0

USER_AGENT = "MyResearchBot contact@example.com"   # ← 改成你的邮箱

# 6-K 财务报表大小阈值：master.idx 的 .txt 文件超过此值视为财务 6-K
MIN_6K_BYTES = 200 * 1024   # 200 KB

# 扫描最近几个季度的 master.idx
IDX_QUARTERS = 4

# 最多检查多少条 6-K（从新到旧，找到第一个命中即停止）
MAX_6K_SCAN = 20

# 调试模式：只跑这几只；改为 None 则使用 TICKER_FILE
DEBUG_TICKERS = None
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": USER_AGENT}
US_FORMS = ["10-K", "10-Q"]


# ─── 季度工具 ─────────────────────────────────────────────────────────────────
def recent_quarters(n: int) -> list:
    """返回最近 n 个季度的 (year, qtr) 列表，从最新到最旧。"""
    today = datetime.utcnow()
    quarters = []
    year, month = today.year, today.month
    qtr = (month - 1) // 3 + 1
    for _ in range(n):
        quarters.append((year, qtr))
        qtr -= 1
        if qtr == 0:
            qtr = 4
            year -= 1
    return quarters


# ─── master.idx 下载与解析 ────────────────────────────────────────────────────
_idx_cache: dict = {}

def fetch_master_idx(year: int, qtr: int) -> Optional[str]:
    """下载并缓存 master.idx。"""
    cache_key = (year, qtr)
    if cache_key in _idx_cache:
        return _idx_cache[cache_key]

    url = (f"https://www.sec.gov/Archives/edgar/full-index/"
           f"{year}/QTR{qtr}/master.idx")
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            _idx_cache[cache_key] = resp.text
            log.debug(f"已加载 {year}/QTR{qtr}/master.idx ({len(resp.text)//1024}KB)")
            return resp.text
        except Exception as e:
            if attempt < MAX_RETRY:
                time.sleep(RETRY_DELAY)
            else:
                log.warning(f"下载 master.idx 失败 {year}/QTR{qtr}: {e}")
                _idx_cache[cache_key] = None
                return None
        finally:
            time.sleep(REQUEST_DELAY)
    return None


def parse_6k_from_master(cik: str) -> list:
    """
    扫描最近 IDX_QUARTERS 个季度的 master.idx，
    返回该 CIK 的所有 6-K，每条包含：
      - filingDate
      - txt_url：完整的 https://www.sec.gov/Archives/... .txt URL
      - accessionNumber
    按日期从新到旧排序。
    """
    cik_int = int(cik)
    cik_str = str(cik_int)
    results = []

    for year, qtr in recent_quarters(IDX_QUARTERS):
        text = fetch_master_idx(year, qtr)
        if not text:
            continue
        for line in text.splitlines():
            if not line or "|" not in line:
                continue
            parts = line.split("|")
            if len(parts) < 5:
                continue
            if parts[0].strip() != cik_str:
                continue
            if parts[2].strip().upper() != "6-K":
                continue
            date     = parts[3].strip()
            filepath = parts[4].strip()   # edgar/data/{CIK}/{accnodash}.txt
            txt_url  = f"https://www.sec.gov/Archives/{filepath}"

            # 从路径提取 accession
            segs = filepath.replace("\\", "/").split("/")
            acc_nodash = Path(segs[-1]).stem   # 去掉 .txt 后缀
            if len(acc_nodash) == 18 and acc_nodash.isdigit():
                acc = f"{acc_nodash[:10]}-{acc_nodash[10:12]}-{acc_nodash[12:]}"
            else:
                acc = acc_nodash

            results.append({
                "filingDate"     : date,
                "txt_url"        : txt_url,
                "accessionNumber": acc,
                "filepath"       : filepath,
            })

    results.sort(key=lambda x: x["filingDate"], reverse=True)
    return results


# ─── HEAD 检查文件大小 ────────────────────────────────────────────────────────
def get_file_size(url: str) -> Optional[int]:
    """
    获取 URL 对应文件的实际大小（字节）。
    SEC 的 .txt 文件 HEAD 不返回 Content-Length，
    改用 streaming GET，只读取数据不写盘，统计实际字节数。
    为避免大文件浪费流量，读到超过阈值就提前返回。
    """
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30, stream=True)
            resp.raise_for_status()
            total = 0
            for chunk in resp.iter_content(chunk_size=65536):
                total += len(chunk)
                if total >= MIN_6K_BYTES:
                    # 已经超过阈值，提前返回，不继续下载
                    resp.close()
                    return total
            return total
        except Exception as e:
            if attempt < MAX_RETRY:
                time.sleep(RETRY_DELAY)
            else:
                log.debug(f"GET size 失败 {url}: {e}")
                return None
        finally:
            time.sleep(REQUEST_DELAY)
    return None


# ─── CIK / submissions ────────────────────────────────────────────────────────
_ticker_map: Optional[dict] = None

def get_cik_fast(ticker: str) -> Optional[str]:
    global _ticker_map
    if _ticker_map is None:
        url = "https://www.sec.gov/files/company_tickers.json"
        for attempt in range(1, MAX_RETRY + 1):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=30)
                resp.raise_for_status()
                raw = resp.json()
                _ticker_map = {
                    v["ticker"].upper(): str(v["cik_str"]).zfill(10)
                    for v in raw.values()
                }
                log.info(f"已加载 SEC ticker 映射表，共 {len(_ticker_map)} 条")
                break
            except Exception as e:
                if attempt < MAX_RETRY:
                    time.sleep(RETRY_DELAY)
                else:
                    log.error(f"无法下载 company_tickers.json: {e}")
                    _ticker_map = {}
        time.sleep(REQUEST_DELAY)
    return _ticker_map.get(ticker)


def get_submissions(cik: str) -> Optional[dict]:
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < MAX_RETRY:
                time.sleep(RETRY_DELAY)
            else:
                log.warning(f"[CIK {cik}] 获取 submissions 失败: {e}")
                return None
        finally:
            time.sleep(REQUEST_DELAY)


def is_foreign(submissions: dict) -> bool:
    recent = submissions.get("filings", {}).get("recent", {})
    for f in recent.get("form", []):
        if f.upper() in ("20-F", "6-K", "40-F"):
            return True
    return False


def find_latest_filing_from_submissions(submissions: dict,
                                        form_types: list) -> Optional[dict]:
    """从 submissions JSON 找最新的指定类型 filing。"""
    recent     = submissions.get("filings", {}).get("recent", {})
    forms      = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    docs       = recent.get("primaryDocument", [])
    dates      = recent.get("filingDate", [])

    best = {}
    target_set = {f.upper() for f in form_types}
    for i, form in enumerate(forms):
        fu = form.upper()
        if fu not in target_set:
            continue
        if fu not in best or dates[i] > best[fu]["filingDate"]:
            best[fu] = {
                "form"           : form,
                "accessionNumber": accessions[i],
                "primaryDocument": docs[i],
                "filingDate"     : dates[i],
            }
    if not best:
        return None
    return max(best.values(), key=lambda x: x["filingDate"])


# ─── 核心：从 master.idx 找财务 6-K ──────────────────────────────────────────
def find_financial_6k(cik: str) -> Optional[dict]:
    """
    从 master.idx 找该公司最新的财务 6-K：
      - 取最近 IDX_QUARTERS 个季度所有 6-K
      - 按日期从新到旧，HEAD 检查 .txt 文件大小
      - 第一个 >= MIN_6K_BYTES 的即为财务 6-K
    """
    candidates = parse_6k_from_master(cik)[:MAX_6K_SCAN]
    log.debug(f"[CIK {cik}] master.idx 共找到 {len(candidates)} 条 6-K")

    for f in candidates:
        size = get_file_size(f["txt_url"])
        size_kb = f"{size//1024}KB" if size else "?"
        log.debug(f"  {f['filingDate']}  {size_kb:>8s}  {f['txt_url']}")
        if size and size >= MIN_6K_BYTES:
            log.info(f"[CIK {cik}] 财务 6-K: {f['filingDate']} ({size//1024}KB)")
            # 补充 primaryDocument 字段供下载使用（用 .txt 本身）
            f["primaryDocument"] = Path(f["filepath"]).name
            f["form"] = "6-K"
            return f

    log.debug(f"[CIK {cik}] 未找到 >={MIN_6K_BYTES//1024}KB 的 6-K")
    return None


# ─── 下载工具 ─────────────────────────────────────────────────────────────────
def build_filing_url(cik: str, accession: str, filename: str) -> str:
    acc_nodash = accession.replace("-", "")
    return (f"https://www.sec.gov/Archives/edgar/data/"
            f"{int(cik)}/{acc_nodash}/{filename}")


def download_file(url: str, save_path: Path) -> bool:
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=60, stream=True)
            resp.raise_for_status()
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            return True
        except Exception as e:
            if attempt < MAX_RETRY:
                time.sleep(RETRY_DELAY)
            else:
                log.warning(f"下载失败 {url}: {e}")
                return False
        finally:
            time.sleep(REQUEST_DELAY)
    return False


def _download_filing(ticker: str, cik: str, filing: dict,
                     result: dict, form_label: str = None) -> dict:
    form = form_label or filing["form"]

    # 6-K 用 txt_url 直接下载；其他用标准路径构造
    if "txt_url" in filing:
        url = filing["txt_url"]
        ext = ".txt"
    else:
        url = build_filing_url(cik, filing["accessionNumber"], filing["primaryDocument"])
        ext = Path(filing["primaryDocument"]).suffix or ".htm"

    filename  = f"{ticker}_{form}_{filing['filingDate']}{ext}"
    save_path = Path(OUTPUT_DIR) / ticker / filename

    if save_path.exists():
        result.update(status="exists", form=form,
                      file=str(save_path), date=filing["filingDate"])
        return result

    ok = download_file(url, save_path)
    if ok:
        result.update(status="ok", form=form,
                      file=str(save_path), date=filing["filingDate"])
    else:
        result["status"] = "error"
        result["reason"] = f"下载失败: {url}"
    return result


# ─── 主处理逻辑 ───────────────────────────────────────────────────────────────
def process_ticker(ticker: str) -> dict:
    result = {"ticker": ticker, "status": "pending", "form": None,
              "file": None, "date": None, "reason": ""}

    cik = get_cik_fast(ticker)
    if not cik:
        result["status"] = "skip"
        result["reason"] = "未找到 CIK"
        return result

    subs = get_submissions(cik)
    if not subs:
        result["status"] = "error"
        result["reason"] = "无法获取 submissions"
        return result

    if not is_foreign(subs):
        # ── 美股：10-K / 10-Q 取最新 ─────────────────────────────────────
        filing = find_latest_filing_from_submissions(subs, US_FORMS)
        if not filing:
            result["status"] = "skip"
            result["reason"] = "未找到 10-K/10-Q"
            return result
        return _download_filing(ticker, cik, filing, result)

    else:
        # ── 外国公司：财务 6-K vs 20-F，取更新的 ────────────────────────
        filing_6k  = find_financial_6k(cik)
        filing_20f = find_latest_filing_from_submissions(subs, ["20-F"])

        if not filing_6k and not filing_20f:
            result["status"] = "skip"
            result["reason"] = "未找到 20-F 或财务 6-K"
            return result

        if filing_6k and filing_20f:
            if filing_6k["filingDate"] >= filing_20f["filingDate"]:
                log.info(f"[{ticker}] 6-K({filing_6k['filingDate']}) >= "
                         f"20-F({filing_20f['filingDate']}), 下载 6-K")
                return _download_filing(ticker, cik, filing_6k, result, "6-K")
            else:
                log.info(f"[{ticker}] 20-F({filing_20f['filingDate']}) > "
                         f"6-K({filing_6k['filingDate']}), 下载 20-F")
                return _download_filing(ticker, cik, filing_20f, result)

        if filing_6k:
            return _download_filing(ticker, cik, filing_6k, result, "6-K")
        return _download_filing(ticker, cik, filing_20f, result)


def main():
    # 预加载 master.idx
    log.info(f"预加载最近 {IDX_QUARTERS} 个季度的 master.idx ...")
    for year, qtr in recent_quarters(IDX_QUARTERS):
        fetch_master_idx(year, qtr)
    log.info("预加载完成")

    if DEBUG_TICKERS:
        tickers = DEBUG_TICKERS
        log.info(f"[调试模式] 只处理: {tickers}")
    else:
        tickers = load_tickers(TICKER_FILE)
        log.info(f"读取到 {len(tickers)} 只股票，开始下载财报...")

    log.info(f"保存目录: {Path(OUTPUT_DIR).resolve()}")
    Path(OUTPUT_DIR).mkdir(exist_ok=True)

    stats = {"ok": 0, "exists": 0, "skip": 0, "error": 0}

    with tqdm(total=len(tickers), desc="下载进度", unit="只") as pbar:
        for ticker in tickers:
            res = process_ticker(ticker)
            stats[res["status"]] = stats.get(res["status"], 0) + 1

            icon = {"ok": "✅", "exists": "📁", "skip": "⚠️",
                    "error": "🔴"}.get(res["status"], "?")
            msg  = res["file"] or res["reason"]
            log.info(f"{icon} {ticker:8s}  {res.get('form',''):10s}  "
                     f"{res.get('date',''):12s}  {msg}")
            pbar.update(1)

    print(f"\n{'='*60}")
    print(f"  ✅ 下载成功: {stats.get('ok',  0)}")
    print(f"  📁 已存在:   {stats.get('exists', 0)}")
    print(f"  ⚠️  跳过:     {stats.get('skip', 0)}")
    print(f"  🔴 失败:     {stats.get('error', 0)}")
    print(f"{'='*60}")
    print(f"  文件保存在: {Path(OUTPUT_DIR).resolve()}")


def load_tickers(filepath: str) -> list:
    with open(filepath, "r", encoding="utf-8") as f:
        return [l.strip().upper() for l in f
                if l.strip() and not l.strip().startswith("#")]


if __name__ == "__main__":
    main()
