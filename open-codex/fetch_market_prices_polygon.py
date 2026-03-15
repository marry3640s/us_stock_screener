#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path


GROUPED_ENDPOINT = (
    "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Fetch Polygon grouped daily close prices and write prices.csv "
            "(symbol,close)."
        )
    )
    p.add_argument(
        "--api-key",
        default=os.getenv("POLYGON_API_KEY", ""),
        help="Polygon API key (or set POLYGON_API_KEY env).",
    )
    p.add_argument(
        "--date",
        default="",
        help="Trading date in YYYY-MM-DD. Default: today (or previous weekday if weekend).",
    )
    p.add_argument(
        "--summary",
        default="",
        help="Optional sec_filing_summary.json path; if set, only keep these tickers.",
    )
    p.add_argument(
        "--output",
        default="prices.csv",
        help="Output CSV path (default: prices.csv).",
    )
    return p.parse_args()


def default_market_date() -> str:
    d = dt.date.today()
    if d.weekday() == 5:
        d = d - dt.timedelta(days=1)
    elif d.weekday() == 6:
        d = d - dt.timedelta(days=2)
    return d.isoformat()


def load_filter_tickers(summary_path: str) -> set[str]:
    data = json.loads(Path(summary_path).read_text(encoding="utf-8"))
    out: set[str] = set()
    for rec in data:
        t = str(rec.get("ticker") or "").strip().upper()
        if t:
            out.add(t)
    return out


def http_get_json(url: str) -> dict:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (PriceFetcher/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8", errors="ignore"))


def main() -> int:
    args = parse_args()
    if not args.api_key:
        print("Missing Polygon API key: use --api-key or POLYGON_API_KEY.", file=sys.stderr)
        return 2

    market_date = args.date.strip() or default_market_date()
    filter_tickers: set[str] = set()
    if args.summary:
        filter_tickers = load_filter_tickers(args.summary)
        print(f"[Init] loaded {len(filter_tickers)} tickers from summary", flush=True)

    qs = urllib.parse.urlencode({"adjusted": "true", "apiKey": args.api_key})
    url = GROUPED_ENDPOINT.format(date=market_date) + "?" + qs
    print(f"[Fetch] Polygon grouped daily: date={market_date}", flush=True)

    try:
        payload = http_get_json(url)
    except Exception as exc:
        print(f"[Error] request failed: {exc}", file=sys.stderr)
        return 1

    status = str(payload.get("status") or "").upper()
    if status != "OK":
        print(
            f"[Error] Polygon returned status={payload.get('status')} error={payload.get('error')}",
            file=sys.stderr,
        )
        return 1

    results = payload.get("results") or []
    if not isinstance(results, list):
        print("[Error] Polygon payload has no results list.", file=sys.stderr)
        return 1

    out_rows: list[tuple[str, float]] = []
    for row in results:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("T") or "").strip().upper()
        close = row.get("c")
        if not sym or not isinstance(close, (int, float)) or close <= 0:
            continue
        if filter_tickers and sym not in filter_tickers:
            continue
        out_rows.append((sym, float(close)))

    out_rows.sort(key=lambda x: x[0])
    out_path = Path(args.output)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        f.write("symbol,close\n")
        for sym, px in out_rows:
            f.write(f"{sym},{px:.6f}\n")

    print(
        f"[Done] wrote {len(out_rows)} prices to {out_path} "
        f"(polygon results={len(results)})",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
