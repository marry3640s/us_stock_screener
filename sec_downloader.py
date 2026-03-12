"""
SEC 财报下载器（多线程 + 双层断点续存版）

核心思路：
  美股（10-K / 10-Q）：从 submissions.json 取最新的，直接下载。
  外国公司（6-K / 20-F）：
    1. 扫描 master.idx，找该公司所有 6-K
       master.idx 格式：CIK|Company|Form|Date|edgar/data/{CIK}/{accnodash}.txt
       那个 .txt 文件本身就是财报内容（SGML 打包格式）
    2. 按日期从新到旧，逐一下载 .txt，读取前 300KB 用关键词判断是否含财务报表
    3. 命中则保留，否则删除继续找下一条
    4. 与最新 20-F 比日期，谁新下谁

多线程改造说明：
  - 使用 ThreadPoolExecutor 并发处理多个 ticker（默认 5 个）
  - 全局 RateLimiter 控制对 SEC 的请求速率（默认 ≤10 req/s）
  - _ticker_map / _idx_cache 用 threading.Lock 保护
  - tqdm 进度条通过 lock 线程安全更新

双层断点续存：
  ① ticker 级：进度记录到 sec-data/progress.json；
               重启后自动跳过 status=ok/exists/skip 的 ticker，
               只重跑 error / 未处理的
  ② 文件级：下载中断后留 .part 临时文件；
            重启后用 HTTP Range: bytes=<offset>- 从断点续传；
            下载完成后原子重命名为目标文件

文件保存到 ./sec-data/<TICKER>/ 目录
依赖：pip install requests tqdm
"""

import re
import json
import time
import logging
import threading
from typing import Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from pathlib import Path
from tqdm import tqdm

# ─── 配置 ──────────────────────────────────────────────────────────────────────
TICKER_FILE   = "tickers.txt"
OUTPUT_DIR    = "sec-data"

# 并发 ticker 数量（建议 3~8，过高会被 SEC 限流）
MAX_WORKERS   = 5

# SEC 全局请求速率上限（每秒最多 N 次，官方建议 ≤10）
MAX_RPS       = 8

MAX_RETRY     = 3
RETRY_DELAY   = 5.0

USER_AGENT = "MyResearchBot contact@example.com"   # ← 改成你的邮箱

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

HEADERS  = {"User-Agent": USER_AGENT}
US_FORMS = ["10-K", "10-Q"]

# ticker 级进度文件路径
PROGRESS_FILE = Path(OUTPUT_DIR) / "progress.json"


# ─── ticker 级断点续存 ────────────────────────────────────────────────────────
_progress: dict      = {}
_progress_lock       = threading.Lock()

def load_progress() -> dict:
    """从 progress.json 加载上次的进度记录。"""
    global _progress
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                _progress = json.load(f)
            log.info(f"已加载进度文件，共 {len(_progress)} 条记录")
        except Exception as e:
            log.warning(f"读取进度文件失败，从头开始: {e}")
            _progress = {}
    return _progress


def save_progress(ticker: str, result: dict):
    """线程安全地将单条 ticker 结果写入进度文件。"""
    with _progress_lock:
        _progress[ticker] = {
            "status": result["status"],
            "form"  : result.get("form"),
            "file"  : result.get("file"),
            "date"  : result.get("date"),
            "reason": result.get("reason", ""),
        }
        try:
            PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
            tmp = PROGRESS_FILE.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(_progress, f, ensure_ascii=False, indent=2)
            tmp.replace(PROGRESS_FILE)   # 原子写入
        except Exception as e:
            log.warning(f"保存进度文件失败: {e}")


def should_skip_ticker(ticker: str) -> Optional[dict]:
    """
    若该 ticker 上次已成功（ok/exists/skip），直接返回缓存结果；
    否则返回 None，表示需要重新处理。
    """
    with _progress_lock:
        rec = _progress.get(ticker)
    if rec and rec.get("status") in ("ok", "exists", "skip"):
        return rec
    return None


# ─── 全局速率限制器 ────────────────────────────────────────────────────────────
class RateLimiter:
    """令牌桶：限制全局每秒请求数，多线程安全。"""
    def __init__(self, rps: float):
        self._interval = 1.0 / rps
        self._lock     = threading.Lock()
        self._last     = 0.0

    def __call__(self):
        with self._lock:
            now  = time.monotonic()
            wait = self._interval - (now - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()


_rate_limit = RateLimiter(MAX_RPS)


def _get(url: str, **kwargs) -> requests.Response:
    """带速率限制的 GET 请求。"""
    _rate_limit()
    return requests.get(url, headers=HEADERS, **kwargs)


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
_idx_cache: dict  = {}
_idx_lock         = threading.Lock()

def fetch_master_idx(year: int, qtr: int) -> Optional[str]:
    """下载并缓存 master.idx（线程安全）。"""
    cache_key = (year, qtr)

    with _idx_lock:
        if cache_key in _idx_cache:
            return _idx_cache[cache_key]

    url = (f"https://www.sec.gov/Archives/edgar/full-index/"
           f"{year}/QTR{qtr}/master.idx")
    result = None
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = _get(url, timeout=30)
            resp.raise_for_status()
            result = resp.text
            log.debug(f"已加载 {year}/QTR{qtr}/master.idx ({len(resp.text)//1024}KB)")
            break
        except Exception as e:
            if attempt < MAX_RETRY:
                time.sleep(RETRY_DELAY)
            else:
                log.warning(f"下载 master.idx 失败 {year}/QTR{qtr}: {e}")

    with _idx_lock:
        _idx_cache[cache_key] = result
    return result


def parse_6k_from_master(cik: str) -> list:
    """
    扫描最近 IDX_QUARTERS 个季度的 master.idx，
    返回该 CIK 的所有 6-K，按日期从新到旧排序。
    """
    cik_str = str(int(cik))
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
            filepath = parts[4].strip()
            txt_url  = f"https://www.sec.gov/Archives/{filepath}"

            segs = filepath.replace("\\", "/").split("/")
            acc_nodash = Path(segs[-1]).stem
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


# ─── 财务 6-K 关键词判断 ──────────────────────────────────────────────────────
_SCAN_BYTES = 1024 * 1024  # 读取前 1MB 做关键词匹配

_INCOME_RE = re.compile(
    r"(total revenue|net revenue|net sales|gross profit|operating income|"
    r"net income|net loss|earnings per share|loss per share|"
    r"consolidated statements? of (operations|income|earnings|comprehensive))",
    re.IGNORECASE,
)
_BALANCE_RE = re.compile(
    r"(total assets|total liabilities|shareholders.{0,10}equity|"
    r"stockholders.{0,10}equity|consolidated balance sheet|"
    r"current assets|current liabilities)",
    re.IGNORECASE,
)
# 从 SGML .txt 中提取 EX-99.1 附件文件名
_EX99_RE = re.compile(
    r"<TYPE>EX-99\.1.*?<FILENAME>([^\s<]+)",
    re.IGNORECASE | re.DOTALL,
)

def _has_financial_keywords(text: str) -> bool:
    """收入表或资产负债表关键词命中任意一类即返回 True。"""
    return bool(_INCOME_RE.search(text)) or bool(_BALANCE_RE.search(text))


def is_financial_6k(save_path: Path, txt_url: str) -> bool:
    """
    判断一个 6-K 是否为财务季报：
      1. 读取 .txt 主文件前 300KB，直接做关键词匹配
      2. 若未命中，从 .txt 中提取 EX-99.1 附件文件名，
         下载附件再做关键词匹配（如 BABA 的 ex99-1.htm）
      3. 任一命中即返回 True，同时返回实际财报文件路径
    返回 (is_financial: bool, report_path: Path)
    report_path 为实际保存的财报文件（可能是 .txt 或附件 .htm）
    """
    try:
        with open(save_path, "rb") as f:
            raw = f.read(_SCAN_BYTES)
        text = raw.decode("latin-1", errors="ignore")

        # 先检查主文件
        if _has_financial_keywords(text):
            log.debug(f"  主文件关键词命中")
            return True, save_path

        # 主文件未命中，找 EX-99.1 附件
        m = _EX99_RE.search(text)
        if not m:
            log.debug(f"  主文件未命中，也无 EX-99.1 附件")
            return False, save_path

        ex_filename = m.group(1).strip()
        # 构造附件 URL：同目录下
        base_url  = txt_url.rsplit("/", 1)[0]
        ex_url    = f"{base_url}/{ex_filename}"
        ex_path   = save_path.parent / ex_filename
        log.debug(f"  主文件未命中，尝试 EX-99.1: {ex_filename}")

        for attempt in range(1, MAX_RETRY + 1):
            try:
                resp = _get(ex_url, timeout=60, stream=True)
                resp.raise_for_status()
                ex_path.parent.mkdir(parents=True, exist_ok=True)
                with open(ex_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
                break
            except Exception as e:
                if attempt < MAX_RETRY:
                    time.sleep(RETRY_DELAY)
                else:
                    log.debug(f"  EX-99.1 下载失败: {e}")
                    return False, save_path

        with open(ex_path, "rb") as f:
            ex_raw = f.read(_SCAN_BYTES)
        ex_text = ex_raw.decode("latin-1", errors="ignore")

        if _has_financial_keywords(ex_text):
            log.debug(f"  EX-99.1 关键词命中: {ex_filename}")
            return True, ex_path
        else:
            log.debug(f"  EX-99.1 也未命中关键词")
            ex_path.unlink(missing_ok=True)
            return False, save_path

    except Exception as e:
        log.debug(f"  关键词判断失败: {e}")
        return False, save_path


# ─── CIK / submissions ────────────────────────────────────────────────────────
_ticker_map: Optional[dict] = None
_ticker_map_lock             = threading.Lock()

def get_cik_fast(ticker: str) -> Optional[str]:
    global _ticker_map
    with _ticker_map_lock:
        if _ticker_map is None:
            url = "https://www.sec.gov/files/company_tickers.json"
            for attempt in range(1, MAX_RETRY + 1):
                try:
                    resp = _get(url, timeout=30)
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
        return _ticker_map.get(ticker)


def get_submissions(cik: str) -> Optional[dict]:
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = _get(url, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < MAX_RETRY:
                time.sleep(RETRY_DELAY)
            else:
                log.warning(f"[CIK {cik}] 获取 submissions 失败: {e}")
                return None
    return None


def is_foreign(submissions: dict) -> bool:
    recent = submissions.get("filings", {}).get("recent", {})
    for f in recent.get("form", []):
        if f.upper() in ("20-F", "6-K", "40-F"):
            return True
    return False


def find_latest_filing_from_submissions(submissions: dict,
                                        form_types: list) -> Optional[dict]:
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
def find_financial_6k(ticker: str, cik: str) -> Optional[dict]:
    """
    从 master.idx 找财务 6-K：
      按日期从新到旧逐一下载 .txt，
      读取前 300KB 用关键词判断是否含财务报表，
      命中则保留，否则删除继续找下一条。
    返回 filing 信息，含 save_path（已下载完成的文件路径）。
    """
    candidates = parse_6k_from_master(cik)[:MAX_6K_SCAN]
    log.debug(f"[{ticker}] master.idx 共找到 {len(candidates)} 条 6-K")

    for f in candidates:
        filename  = f"{ticker}_6-K_{f['filingDate']}.txt"
        save_path = Path(OUTPUT_DIR) / ticker / filename

        # 已存在则直接视为命中（之前已验证过）
        if save_path.exists():
            log.debug(f"[{ticker}] 6-K 已存在: {save_path}")
            f["save_path"] = save_path
            f["form"] = "6-K"
            return f

        log.debug(f"[{ticker}] 下载 6-K {f['filingDate']}: {f['txt_url']}")
        ok = download_file(f["txt_url"], save_path)
        if not ok:
            continue

        is_fin, report_path = is_financial_6k(save_path, f["txt_url"])
        if is_fin:
            log.info(f"[{ticker}] 财务 6-K: {f['filingDate']} ({report_path.stat().st_size//1024}KB) → {report_path.name}")
            # 若实际财报是附件（ex99-1.htm），删掉已无用的 .txt 封面
            if report_path != save_path:
                save_path.unlink(missing_ok=True)
            f["save_path"] = report_path
            f["form"] = "6-K"
            return f
        else:
            log.debug(f"[{ticker}] 非财务 6-K，删除，继续")
            save_path.unlink(missing_ok=True)

    log.debug(f"[{ticker}] 未找到财务 6-K")
    return None


# ─── 下载工具 ─────────────────────────────────────────────────────────────────
def build_filing_url(cik: str, accession: str, filename: str) -> str:
    acc_nodash = accession.replace("-", "")
    return (f"https://www.sec.gov/Archives/edgar/data/"
            f"{int(cik)}/{acc_nodash}/{filename}")


def download_file(url: str, save_path: Path) -> bool:
    """
    文件级断点续传下载：
      - 先检查 <save_path>.part 临时文件已有字节数
      - 若服务器支持 Range，发送 Range: bytes=<offset>- 续传
      - 下载完成后原子重命名 .part → save_path
      - 若服务器不支持 Range（206 以外），从头下载
    """
    save_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = save_path.with_suffix(save_path.suffix + ".part")

    for attempt in range(1, MAX_RETRY + 1):
        try:
            offset = part_path.stat().st_size if part_path.exists() else 0

            req_headers = dict(HEADERS)
            if offset:
                req_headers["Range"] = f"bytes={offset}-"
                log.debug(f"续传 {save_path.name} 从 {offset//1024}KB")

            _rate_limit()
            resp = requests.get(url, headers=req_headers, timeout=60, stream=True)

            # 服务器不支持 Range（返回 200 而非 206），从头下载
            if offset and resp.status_code == 200:
                log.debug(f"服务器不支持 Range，从头下载: {url}")
                offset = 0
                part_path.unlink(missing_ok=True)

            resp.raise_for_status()

            mode = "ab" if offset else "wb"
            with open(part_path, mode) as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)

            # 原子重命名
            part_path.replace(save_path)
            return True

        except Exception as e:
            if attempt < MAX_RETRY:
                log.debug(f"下载出错（第{attempt}次），{RETRY_DELAY}s 后重试: {e}")
                time.sleep(RETRY_DELAY)
            else:
                log.warning(f"下载失败 {url}: {e}")
                return False
    return False


def _download_filing(ticker: str, cik: str, filing: dict,
                     result: dict, form_label: str = None) -> dict:
    form = form_label or filing["form"]

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

    # 有 .part 文件说明上次下载中断，续传
    part_path = save_path.with_suffix(save_path.suffix + ".part")
    if part_path.exists():
        log.info(f"[{ticker}] 发现未完成文件 {part_path.name}，尝试续传…")

    ok = download_file(url, save_path)
    if ok:
        result.update(status="ok", form=form,
                      file=str(save_path), date=filing["filingDate"])
    else:
        result["status"] = "error"
        result["reason"] = f"下载失败: {url}"
    return result


# ─── 主处理逻辑（单 ticker） ──────────────────────────────────────────────────
def process_ticker(ticker: str) -> dict:
    result = {"ticker": ticker, "status": "pending", "form": None,
              "file": None, "date": None, "reason": ""}

    # ── ticker 级断点续存：上次已成功则直接返回缓存 ──────────────────────────
    cached = should_skip_ticker(ticker)
    if cached:
        log.debug(f"[{ticker}] 跳过（上次已完成: {cached['status']}）")
        return {**result, **cached, "ticker": ticker}

    cik = get_cik_fast(ticker)
    if not cik:
        result["status"] = "skip"
        result["reason"] = "未找到 CIK"
        save_progress(ticker, result)
        return result

    subs = get_submissions(cik)
    if not subs:
        result["status"] = "error"
        result["reason"] = "无法获取 submissions"
        save_progress(ticker, result)
        return result

    if not is_foreign(subs):
        filing = find_latest_filing_from_submissions(subs, US_FORMS)
        if not filing:
            result["status"] = "skip"
            result["reason"] = "未找到 10-K/10-Q"
            save_progress(ticker, result)
            return result
        result = _download_filing(ticker, cik, filing, result)
        save_progress(ticker, result)
        return result
    else:
        filing_6k  = find_financial_6k(ticker, cik)
        filing_20f = find_latest_filing_from_submissions(subs, ["20-F"])

        if not filing_6k and not filing_20f:
            result["status"] = "skip"
            result["reason"] = "未找到 20-F 或财务 6-K"
            save_progress(ticker, result)
            return result

        if filing_6k and filing_20f:
            if filing_6k["filingDate"] >= filing_20f["filingDate"]:
                log.info(f"[{ticker}] 6-K({filing_6k['filingDate']}) >= "
                         f"20-F({filing_20f['filingDate']}), 使用 6-K")
                result.update(status="ok", form="6-K",
                              file=str(filing_6k["save_path"]),
                              date=filing_6k["filingDate"])
            else:
                log.info(f"[{ticker}] 20-F({filing_20f['filingDate']}) > "
                         f"6-K({filing_6k['filingDate']}), 下载 20-F")
                # 6-K 已下载但不用，删掉
                filing_6k["save_path"].unlink(missing_ok=True)
                result = _download_filing(ticker, cik, filing_20f, result)
            save_progress(ticker, result)
            return result

        if filing_6k:
            result.update(status="ok", form="6-K",
                          file=str(filing_6k["save_path"]),
                          date=filing_6k["filingDate"])
        else:
            result = _download_filing(ticker, cik, filing_20f, result)
        save_progress(ticker, result)
        return result


# ─── 多线程主流程 ─────────────────────────────────────────────────────────────
def main():
    Path(OUTPUT_DIR).mkdir(exist_ok=True)

    # 加载上次进度（ticker 级断点续存）
    load_progress()

    # 预加载 master.idx（串行，避免重复下载）
    log.info(f"预加载最近 {IDX_QUARTERS} 个季度的 master.idx ...")
    for year, qtr in recent_quarters(IDX_QUARTERS):
        fetch_master_idx(year, qtr)
    log.info("预加载完成")

    # 预加载 CIK 映射（所有线程共用，提前加载避免并发重复请求）
    log.info("预加载 SEC ticker → CIK 映射表 ...")
    get_cik_fast("AAPL")   # 任意一次调用即可触发加载
    log.info("预加载完成")

    if DEBUG_TICKERS:
        tickers = DEBUG_TICKERS
        log.info(f"[调试模式] 只处理: {tickers}")
    else:
        tickers = load_tickers(TICKER_FILE)
        log.info(f"读取到 {len(tickers)} 只股票")

    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    log.info(f"保存目录: {Path(OUTPUT_DIR).resolve()}")
    log.info(f"并发 ticker 数: {MAX_WORKERS}，全局速率上限: {MAX_RPS} req/s")

    already_done = sum(
        1 for t in tickers
        if _progress.get(t, {}).get("status") in ("ok", "exists", "skip")
    )
    if already_done:
        log.info(f"断点续存：{already_done}/{len(tickers)} 只已完成，跳过")

    stats     = {"ok": 0, "exists": 0, "skip": 0, "error": 0}
    stats_lock = threading.Lock()

    icon_map = {"ok": "✅", "exists": "📁", "skip": "⚠️", "error": "🔴"}

    with tqdm(total=len(tickers), desc="下载进度", unit="只") as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            future_to_ticker = {
                pool.submit(process_ticker, t): t for t in tickers
            }
            for future in as_completed(future_to_ticker):
                res  = future.result()
                icon = icon_map.get(res["status"], "?")
                msg  = res["file"] or res["reason"]
                log.info(
                    f"{icon} {res['ticker']:8s}  "
                    f"{res.get('form',''):10s}  "
                    f"{res.get('date',''):12s}  {msg}"
                )
                with stats_lock:
                    stats[res["status"]] = stats.get(res["status"], 0) + 1
                pbar.update(1)

    print(f"\n{'='*60}")
    print(f"  ✅ 下载成功: {stats.get('ok',    0)}")
    print(f"  📁 已存在:   {stats.get('exists', 0)}")
    print(f"  ⚠️  跳过:     {stats.get('skip',  0)}")
    print(f"  🔴 失败:     {stats.get('error',  0)}")
    print(f"{'='*60}")
    print(f"  文件保存在: {Path(OUTPUT_DIR).resolve()}")


def load_tickers(filepath: str) -> list:
    with open(filepath, "r", encoding="utf-8") as f:
        return [l.strip().upper() for l in f
                if l.strip() and not l.strip().startswith("#")]


if __name__ == "__main__":
    main()
