#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


TEXT_EXTENSIONS = {".txt", ".htm", ".html"}
FORM_TYPES = {"10-K", "10-Q", "6-K", "20-F"}


def _parse_any_date(s: str) -> datetime | None:
    s = re.sub(r"\s+", " ", s).strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def _strip_html(text: str) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\\s*>", "\n", text)
    text = re.sub(r"(?i)</tr\\s*>", "\n", text)
    text = re.sub(r"(?i)</h[1-6]\\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&#160;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
    )
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _try_sec_parser_text(raw: str) -> str | None:
    try:
        from sec_parser import Edgar10QParser  # type: ignore
    except Exception:
        return None
    try:
        parser = Edgar10QParser()
        elements = parser.parse(raw)
        lines: list[str] = []
        for e in elements:
            t = getattr(e, "text", None)
            if isinstance(t, str) and t.strip():
                lines.append(t.strip())
        if lines:
            return "\n".join(lines)
    except Exception:
        return None
    return None


def _sec_parser_available() -> bool:
    try:
        from sec_parser import Edgar10QParser  # type: ignore

        _ = Edgar10QParser
        return True
    except Exception:
        return False


def _detect_form_type(path: Path, raw: str) -> str | None:
    name = path.name.upper()
    for ft in FORM_TYPES:
        if f"_{ft}_" in name:
            return ft
    m = re.search(r"(?i)CONFORMED SUBMISSION TYPE:\s*([0-9A-Z\-]+)", raw)
    if m:
        ft = m.group(1).upper()
        if ft in FORM_TYPES:
            return ft
    m = re.search(r"(?i)<TYPE>\s*([0-9A-Z\-]+)", raw)
    if m:
        ft = m.group(1).upper()
        if ft in FORM_TYPES:
            return ft
    return None


def _extract_company_name(raw: str, plain: str) -> str | None:
    for pat in (
        r"(?i)COMPANY CONFORMED NAME:\s*(.+)",
        r"(?i)ENTITY REGISTRANT NAME\s*[:\-]\s*(.+)",
    ):
        m = re.search(pat, raw)
        if m:
            return m.group(1).strip(" \t\r\n.;")
    m = re.search(r"(?i)^(.+?)\s+announced", plain)
    if m:
        c = m.group(1).strip()
        if 3 <= len(c) <= 120:
            return c
    return None


def _extract_date(raw: str, plain: str, kind: str) -> str | None:
    if kind == "filing":
        for pat in (
            r"(?i)FILED AS OF DATE:\s*([0-9]{8})",
            r"(?i)FILING DATE\s*[:\-]\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        ):
            m = re.search(pat, raw)
            if m:
                s = m.group(1).strip()
                if re.fullmatch(r"[0-9]{8}", s):
                    return datetime.strptime(s, "%Y%m%d").strftime("%B %d, %Y")
                return s
    else:
        # Prefer explicit period-ended disclosures over filing meta dates.
        ended_candidates: list[str] = []
        for pat in (
            r"(?i)for the (?:quarter|period|year) ended\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
            r"(?i)nine months ended\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
        ):
            ended_candidates.extend([x.strip() for x in re.findall(pat, plain)])
        if ended_candidates:
            def _parse_dt(s: str) -> datetime:
                dt = _parse_any_date(s)
                if dt is None:
                    raise ValueError("bad date")
                return dt
            try:
                return max(ended_candidates, key=_parse_dt)
            except Exception:
                return ended_candidates[-1]
        candidates = re.findall(r"(?i)as of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", plain)
        if candidates:
            quarter_like = [c for c in candidates if re.search(r"(?i)(March 31|June 30|September 30|December 31|Mar 31|Jun 30|Sep 30|Dec 31)", c)]
            pool = quarter_like if quarter_like else candidates
            try:
                return max(pool, key=lambda x: _parse_any_date(x) or datetime.min).strip()
            except Exception:
                return pool[-1].strip()
        m = re.search(r"(?i)CONFORMED PERIOD OF REPORT:\s*([0-9]{8})", raw)
        if m:
            return datetime.strptime(m.group(1), "%Y%m%d").strftime("%B %d, %Y")
    return None


def _extract_currency_unit(plain: str) -> str | None:
    for pat in (
        r"(?i)\(All amounts in ([^)]+)\)",
        r"(?i)\((USD|US\$|RMB|CNY|HKD|EUR|GBP|CAD|JPY)[^)]*?(thousand|thousands|million|millions|billion|billions)?[^)]*\)",
        r"(?i)\bin\s+(thousand|thousands|million|millions|billion|billions)\s+of\s+(USD|US\$|RMB|CNY|HKD|EUR|GBP|CAD|JPY)\b",
    ):
        m = re.search(pat, plain)
        if m:
            return m.group(0).strip("() ")
    return None


def _to_number(token: str | None) -> float | None:
    if not token:
        return None
    t = token.strip().replace(",", "")
    if t in {"", "-", "--", "---", "—"}:
        return None
    neg = False
    if t.startswith("(") and t.endswith(")"):
        neg = True
        t = t[1:-1]
    t = t.replace("$", "").replace("US$", "").replace("RMB", "").replace("HK$", "").strip()
    mult = 1.0
    if t.endswith(("K", "k")):
        mult = 1_000.0
        t = t[:-1]
    elif t.endswith(("M", "m")):
        mult = 1_000_000.0
        t = t[:-1]
    elif t.endswith(("B", "b")):
        mult = 1_000_000_000.0
        t = t[:-1]
    try:
        v = float(t) * mult
        return -v if neg else v
    except ValueError:
        return None


def _extract_labeled_number(plain: str, labels: Iterable[str]) -> float | None:
    for label in labels:
        pat = rf"(?is)\b{label}\b[^0-9\-\(]{{0,80}}([\(]?-?\$?[0-9][0-9,]*(?:\.[0-9]+)?[KMBkmb]?[)]?)"
        m = re.search(pat, plain)
        if m:
            v = _to_number(m.group(1))
            if v is not None:
                return v
    return None


def _extract_ads_ratio(plain: str) -> float | None:
    patterns = (
        r"(?i)each ADS represents\s+([0-9]+(?:\.[0-9]+)?)\s+ordinary share",
        r"(?i)one ADS represents\s+([0-9]+(?:\.[0-9]+)?)\s+ordinary share",
        r"(?i)ADSs?\s+represent(?:s)?\s+([0-9]+(?:\.[0-9]+)?)\s+ordinary share",
    )
    for pat in patterns:
        m = re.search(pat, plain)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                pass
    return None


def _extract_shares_outstanding(plain: str) -> tuple[float | None, str | None]:
    for pat in (
        r"(?i)([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,})\s+(?:ordinary\s+)?shares\s+outstanding",
        r"(?i)(?:ordinary\s+)?shares\s+outstanding[^0-9]{0,40}([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,})",
        r"(?i)([0-9]+(?:\.[0-9]+)?)\s+million\s+(?:ordinary\s+)?shares\s+outstanding",
    ):
        m = re.search(pat, plain)
        if m:
            raw = m.group(1)
            if "million" in pat.lower():
                try:
                    return float(raw) * 1_000_000.0, "manual"
                except ValueError:
                    continue
            return _to_number(raw), "manual"
    return None, None


@dataclass
class FilingSummary:
    file_path: str
    ticker: str
    company_name: str | None
    form_type: str
    filing_date: str | None
    period_end_date: str | None
    currency_unit: str | None
    revenue: float | None
    cogs: float | None
    gross_profit: float | None
    research_and_development: float | None
    selling_and_marketing: float | None
    general_and_administrative: float | None
    sga: float | None
    pretax_income: float | None
    tax_expense: float | None
    net_income: float | None
    operating_income: float | None
    ebitda: float | None
    eps_basic: float | None
    eps_diluted: float | None
    total_assets: float | None
    assets_current: float | None
    total_liabilities: float | None
    liabilities_current: float | None
    equity: float | None
    cash: float | None
    short_term_investments: float | None
    accounts_receivable: float | None
    accounts_payable: float | None
    deferred_revenue: float | None
    retained_earnings: float | None
    operating_cash_flow: float | None
    investing_cash_flow: float | None
    financing_cash_flow: float | None
    free_cash_flow: float | None
    depreciation_and_amortization: float | None
    share_based_compensation: float | None
    capex: float | None
    shares_outstanding: float | None
    ordinary_shares_per_ads: float | None
    shares_outstanding_ads_equivalent: float | None
    weighted_avg_shares_basic: float | None
    weighted_avg_shares_diluted: float | None
    shares_source: str | None


def _extract_summary(path: Path) -> FilingSummary | None:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    form_type = _detect_form_type(path, raw)
    if not form_type:
        return None

    sec_text = _try_sec_parser_text(raw)
    plain = _strip_html(sec_text or raw)
    ticker = path.parent.name.upper()
    filing_date = _extract_date(raw, plain, "filing")
    period_end = _extract_date(raw, plain, "period")
    ads_ratio = _extract_ads_ratio(plain)
    shares, shares_source = _extract_shares_outstanding(plain)

    wavg_basic = _extract_labeled_number(
        plain,
        (
            "Weighted average number of ordinary shares used in calculating net income \\(loss\\) per ordinary share\\s*Basic",
            "Weighted average number of outstanding shares\\s*Basic",
            "Weighted average shares outstanding\\s*Basic",
        ),
    )
    wavg_diluted = _extract_labeled_number(
        plain,
        (
            "Weighted average number of ordinary shares used in calculating net income \\(loss\\) per ordinary share\\s*Diluted",
            "Weighted average number of outstanding shares\\s*Diluted",
            "Weighted average shares outstanding\\s*Diluted",
        ),
    )

    shares_ads_eq = shares / ads_ratio if shares and ads_ratio else None
    sga = None
    s_and_m = _extract_labeled_number(plain, ("selling and marketing", "sales and marketing"))
    g_and_a = _extract_labeled_number(plain, ("general and administrative",))
    if s_and_m is not None and g_and_a is not None:
        sga = s_and_m + g_and_a

    return FilingSummary(
        file_path=str(path),
        ticker=ticker,
        company_name=_extract_company_name(raw, plain),
        form_type=form_type,
        filing_date=filing_date,
        period_end_date=period_end,
        currency_unit=_extract_currency_unit(plain),
        revenue=_extract_labeled_number(plain, ("revenue", "net revenues", "total revenues")),
        cogs=_extract_labeled_number(plain, ("cost of revenue", "cost of revenues", "cost of goods sold")),
        gross_profit=_extract_labeled_number(plain, ("gross profit",)),
        research_and_development=_extract_labeled_number(plain, ("research and development", "R&D")),
        selling_and_marketing=s_and_m,
        general_and_administrative=g_and_a,
        sga=sga,
        pretax_income=_extract_labeled_number(plain, ("income before income taxes", "income before tax")),
        tax_expense=_extract_labeled_number(plain, ("income tax expense", "tax expense")),
        net_income=_extract_labeled_number(plain, ("net income", "net \\(loss\\) income", "net loss")),
        operating_income=_extract_labeled_number(plain, ("income from operations", "operating income")),
        ebitda=_extract_labeled_number(plain, ("EBITDA",)),
        eps_basic=_extract_labeled_number(plain, ("net income per ordinary share\\s*basic", "basic earnings per share")),
        eps_diluted=_extract_labeled_number(
            plain, ("net income per ordinary share\\s*diluted", "diluted earnings per share")
        ),
        total_assets=_extract_labeled_number(plain, ("total assets",)),
        assets_current=_extract_labeled_number(plain, ("total current assets", "current assets")),
        total_liabilities=_extract_labeled_number(plain, ("total liabilities",)),
        liabilities_current=_extract_labeled_number(plain, ("total current liabilities", "current liabilities")),
        equity=_extract_labeled_number(
            plain, ("total shareholders' equity", "total stockholders' equity", "total equity")
        ),
        cash=_extract_labeled_number(plain, ("cash and cash equivalents",)),
        short_term_investments=_extract_labeled_number(plain, ("short-term investments", "short term investments")),
        accounts_receivable=_extract_labeled_number(plain, ("accounts receivable", "trade receivables")),
        accounts_payable=_extract_labeled_number(plain, ("accounts payable", "trade payables")),
        deferred_revenue=_extract_labeled_number(plain, ("deferred revenue", "contract liabilities")),
        retained_earnings=_extract_labeled_number(plain, ("retained earnings",)),
        operating_cash_flow=_extract_labeled_number(plain, ("net cash provided by operating activities",)),
        investing_cash_flow=_extract_labeled_number(plain, ("net cash used in investing activities",)),
        financing_cash_flow=_extract_labeled_number(plain, ("net cash provided by financing activities",)),
        free_cash_flow=None,
        depreciation_and_amortization=_extract_labeled_number(plain, ("depreciation and amortization",)),
        share_based_compensation=_extract_labeled_number(plain, ("share-based compensation", "stock-based compensation")),
        capex=_extract_labeled_number(plain, ("capital expenditures", "purchases of property and equipment")),
        shares_outstanding=shares,
        ordinary_shares_per_ads=ads_ratio,
        shares_outstanding_ads_equivalent=shares_ads_eq,
        weighted_avg_shares_basic=wavg_basic,
        weighted_avg_shares_diluted=wavg_diluted,
        shares_source=shares_source,
    )


def _iter_filing_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in TEXT_EXTENSIONS:
            out.append(p)
    out.sort()
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Parse SEC filings with sec_parser-first strategy.")
    ap.add_argument("--dir", required=True, help="Root folder containing SEC files")
    ap.add_argument("--output", default="sec_filing_summary_sec_parser.json", help="Output JSON path")
    ap.add_argument("--workers", type=int, default=8, help="Thread workers")
    ap.add_argument("--progress-every", type=int, default=50, help="Print progress every N files")
    args = ap.parse_args()

    root = Path(args.dir)
    files = _iter_filing_files(root)
    total = len(files)
    if total == 0:
        print(f"No files found under {root}")
        return 1

    parsed: list[FilingSummary] = []
    lock = threading.Lock()
    scanned = 0
    start = time.time()

    def _task(p: Path) -> FilingSummary | None:
        try:
            return _extract_summary(p)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        fut_to_path = {ex.submit(_task, p): p for p in files}
        for fut in as_completed(fut_to_path):
            item = fut.result()
            with lock:
                scanned += 1
                if item is not None:
                    parsed.append(item)
                if scanned % max(1, args.progress_every) == 0 or scanned == total:
                    elapsed = max(time.time() - start, 1e-6)
                    print(
                        f"[Progress] {scanned}/{total} scanned, parsed={len(parsed)}, "
                        f"speed={scanned/elapsed:.1f} files/s, symbol={fut_to_path[fut].parent.name.upper()}",
                        flush=True,
                    )

    # keep one latest filing per ticker+form family
    latest: dict[tuple[str, str], FilingSummary] = {}
    for item in parsed:
        k = (item.ticker, item.form_type)
        prev = latest.get(k)
        if prev is None:
            latest[k] = item
            continue
        prev_dt = prev.filing_date or ""
        cur_dt = item.filing_date or ""
        if cur_dt >= prev_dt:
            latest[k] = item

    out_items = [asdict(v) for v in sorted(latest.values(), key=lambda x: (x.ticker, x.form_type))]
    Path(args.output).write_text(json.dumps(out_items, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(out_items)} records to {args.output}")
    if not _sec_parser_available():
        print("Note: sec_parser import unavailable; current run used text fallback.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
