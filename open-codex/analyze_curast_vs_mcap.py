from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import csv
import json
import os
import random
import re
import time
import sys
import threading
import urllib.parse
import urllib.request
import urllib.error
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

YAHOO_QUOTE_ENDPOINT = "https://query1.finance.yahoo.com/v7/finance/quote"
STOOQ_QUOTE_ENDPOINT = "https://stooq.com/q/l/"
POLYGON_PREV_ENDPOINT = "https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
_FX_CACHE: dict[str, float] = {"USD": 1.0}


class RateLimitController:
    def __init__(self, min_interval_seconds: float = 0.12, base_cooldown_seconds: float = 3.0):
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self.base_cooldown_seconds = max(0.0, float(base_cooldown_seconds))
        self._lock = threading.Lock()
        self._next_allowed = 0.0
        self._pause_until = 0.0
        self._penalty = self.base_cooldown_seconds

    def wait_turn(self) -> None:
        while True:
            with self._lock:
                now = time.perf_counter()
                ready_at = max(self._next_allowed, self._pause_until)
                wait_s = max(0.0, ready_at - now)
                if wait_s <= 0:
                    self._next_allowed = now + self.min_interval_seconds
                    return
            time.sleep(min(wait_s, 0.5))

    def on_rate_limited(self, retry_after_seconds: float | None = None) -> float:
        with self._lock:
            now = time.perf_counter()
            if retry_after_seconds is not None and retry_after_seconds > 0:
                pause = retry_after_seconds
            else:
                pause = max(self.base_cooldown_seconds, self._penalty)
                self._penalty = min(max(self.base_cooldown_seconds, self._penalty * 1.8), 60.0)
            self._pause_until = max(self._pause_until, now + pause)
            return pause

    def on_success(self) -> None:
        with self._lock:
            self._penalty = max(self.base_cooldown_seconds, self._penalty * 0.85)


def _parse_retry_after(header_value: str | None) -> float | None:
    if not header_value:
        return None
    try:
        value = float(header_value.strip())
        if value > 0:
            return value
    except (TypeError, ValueError):
        return None
    return None


def _http_get_with_retry(
    url: str,
    headers: dict[str, str],
    timeout: float,
    limiter: RateLimitController | None,
    max_retries: int,
) -> tuple[str | None, str | None]:
    """
    Returns (body, fail_reason). fail_reason is None on success.
    """
    for attempt in range(1, max_retries + 1):
        if limiter:
            limiter.wait_turn()
        req = urllib.request.Request(url, headers=headers)
        try:
            body = urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", errors="ignore").strip()
            if limiter:
                limiter.on_success()
            return body, None
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 503):
                retry_after = _parse_retry_after(exc.headers.get("Retry-After") if exc.headers else None)
                pause = limiter.on_rate_limited(retry_after) if limiter else (retry_after or 2.0)
                print(
                    f"[RateLimit] HTTP {exc.code}, attempt={attempt}/{max_retries}, sleeping {pause:.1f}s",
                    flush=True,
                )
                time.sleep(pause + random.uniform(0, 0.3))
                continue
            return None, f"http_{exc.code}"
        except urllib.error.URLError:
            if attempt < max_retries:
                time.sleep(min(0.5 * attempt, 2.0) + random.uniform(0, 0.2))
                continue
            return None, "urlerror"
        except (TimeoutError, ValueError):
            if attempt < max_retries:
                time.sleep(min(0.5 * attempt, 2.0) + random.uniform(0, 0.2))
                continue
            return None, "timeout_or_valueerror"
    return None, "retries_exhausted"


@dataclass
class Candidate:
    ticker: str
    company_name: str
    currency_unit: str | None
    assets_current: float
    total_liabilities: float
    shares_outstanding: float
    shares_used_for_mcap: float
    fx_to_usd: float
    price: float
    market_cap: float
    surplus: float
    surplus_pct_of_mcap: float
    ebitda: float | None = None
    net_income: float | None = None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _nested_get(record: dict[str, Any], *path: str) -> Any:
    current: Any = record
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _unit_label_multiplier(unit_label: Any) -> float:
    text = " ".join(str(unit_label or "").split()).lower()
    if not text:
        return 1.0
    if "billion" in text:
        return 1_000_000_000.0
    if "million" in text:
        return 1_000_000.0
    if "thousand" in text:
        return 1_000.0
    return 1.0


def _compose_currency_unit(currency: Any, unit_label: Any) -> str | None:
    ccy = " ".join(str(currency or "").split()).strip().upper()
    label = " ".join(str(unit_label or "").split()).strip()
    if ccy and label:
        return f"{ccy}, {label}"
    if ccy:
        return ccy
    if label:
        return label
    return None


def _normalize_input_record(record: dict[str, Any]) -> dict[str, Any]:
    # Old flat schema stays unchanged.
    if not any(key in record for key in ("income_statement", "balance_sheet", "cash_flow", "shares", "ratios")):
        return record

    # Nested JSON amounts are normalized to millions by contract, even if
    # upstream metadata still carries a legacy unit label like "thousands".
    scale = 1_000_000.0
    effective_unit_label = "millions(dom)"
    shares_common = _as_float(_nested_get(record, "shares", "common_M"))
    shares_weighted = _as_float(_nested_get(record, "shares", "weighted_avg_M"))
    shares_diluted = _as_float(_nested_get(record, "shares", "diluted_M"))
    ads_per_share_raw = _as_float(_nested_get(record, "shares", "ads_per_share"))
    ordinary_shares_per_ads = _as_float(_nested_get(record, "shares", "ordinary_shares_per_ads"))
    ads_per_ordinary_share = _as_float(_nested_get(record, "shares", "ads_per_ordinary_share"))

    if ordinary_shares_per_ads not in (None, 0):
        ads_per_ordinary_share = 1.0 / ordinary_shares_per_ads
    elif ads_per_ordinary_share not in (None, 0):
        ordinary_shares_per_ads = 1.0 / ads_per_ordinary_share
    elif ads_per_share_raw not in (None, 0):
        # Accept both conventions for backward compatibility:
        # values > 1 usually mean "ordinary shares per ADS" (e.g. LX = 2.0),
        # while fractional values usually mean "ADS per ordinary share" (e.g. VIPS = 0.2).
        if ads_per_share_raw >= 1.0:
            ordinary_shares_per_ads = ads_per_share_raw
            ads_per_ordinary_share = 1.0 / ads_per_share_raw
        else:
            ads_per_ordinary_share = ads_per_share_raw
            ordinary_shares_per_ads = 1.0 / ads_per_share_raw

    def _scaled_metric(*path: str) -> float | None:
        value = _as_float(_nested_get(record, *path))
        if value is None:
            return None
        return value * scale

    normalized = {
        "file_path": record.get("file_path"),
        "ticker": record.get("ticker"),
        "company_name": record.get("company_name"),
        "form_type": record.get("filing_type") or record.get("form_type"),
        "filing_date": record.get("filing_date"),
        "period_end_date": record.get("fiscal_period") or record.get("period_end_date"),
        "currency_unit": _compose_currency_unit(record.get("currency"), effective_unit_label),
        "revenue": _scaled_metric("income_statement", "revenue"),
        "ebitda": _scaled_metric("income_statement", "ebitda"),
        "net_income": _scaled_metric("income_statement", "net_income"),
        "total_assets": _scaled_metric("balance_sheet", "total_assets"),
        "assets_current": _scaled_metric("balance_sheet", "current_assets"),
        "total_liabilities": _scaled_metric("balance_sheet", "total_liabilities"),
        "liabilities_current": _scaled_metric("balance_sheet", "current_liabilities"),
        "shares_outstanding": shares_common * 1_000_000.0 if shares_common is not None else None,
        "weighted_avg_shares_basic": shares_weighted * 1_000_000.0 if shares_weighted is not None else None,
        "weighted_avg_shares_diluted": shares_diluted * 1_000_000.0 if shares_diluted is not None else None,
        "shares_outstanding_ads_equivalent": (
            shares_common * ads_per_ordinary_share * 1_000_000.0
            if shares_common is not None and ads_per_ordinary_share is not None
            else None
        ),
        "ordinary_shares_per_ads": ordinary_shares_per_ads,
    }
    return normalized


def _normalize_input_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_normalize_input_record(record) for record in records]


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _parse_currency_code(currency_unit: str | None) -> str:
    if not currency_unit:
        return "USD"
    upper = currency_unit.upper()
    token_match = re.search(r"\b[A-Z]{3}\b", upper)
    token = token_match.group(0) if token_match else ""
    alias = {
        "RMB": "CNY",
        "CNH": "CNY",
    }
    if token in alias:
        return alias[token]
    if token:
        return token
    name_map = [
        ("CNY", r"RENMINBI|YUAN|人民币"),
        ("HKD", r"HONG\s*KONG"),
        ("TWD", r"NEW\s*TAIWAN"),
        ("JPY", r"YEN"),
        ("KRW", r"WON"),
        ("EUR", r"EURO"),
        ("GBP", r"POUND|STERLING"),
        ("BRL", r"BRAZILIAN"),
        ("INR", r"RUPEE"),
        ("USD", r"U\.?S\.?\s*DOLLAR|US\$"),
    ]
    for code, patt in name_map:
        if re.search(patt, upper):
            return code
    return "USD"


def fetch_prices(
    tickers: list[str],
    progress_every: int = 200,
    heartbeat_seconds: float = 3.0,
    fallback_max_seconds: float = 0.0,
    fallback_wave_size: int = 400,
    fallback_wave_pause_seconds: float = 1.0,
    max_retries: int = 3,
    min_request_interval_seconds: float = 0.12,
    base_cooldown_seconds: float = 3.0,
    fallback_max_workers: int = 6,
) -> dict[str, float]:
    prices: dict[str, float] = {}
    if not tickers:
        return prices
    limiter = RateLimitController(
        min_interval_seconds=min_request_interval_seconds,
        base_cooldown_seconds=base_cooldown_seconds,
    )

    unique_tickers = sorted(set(tickers))
    batches = _chunked(unique_tickers, 50)
    total_batches = len(batches)
    for i, batch in enumerate(batches, start=1):
        query = urllib.parse.urlencode({"symbols": ",".join(batch)})
        url = f"{YAHOO_QUOTE_ENDPOINT}?{query}"
        body, reason = _http_get_with_retry(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
            timeout=12,
            limiter=limiter,
            max_retries=max_retries,
        )
        if not body:
            if reason:
                print(f"[Progress][Price] Yahoo batch failed: reason={reason}", flush=True)
            continue
        try:
            payload = json.loads(body)
        except ValueError:
            continue
        results = payload.get("quoteResponse", {}).get("result", [])
        for item in results:
            ticker = item.get("symbol")
            px = _as_float(item.get("regularMarketPrice"))
            if ticker and px and px > 0:
                ticker_u = ticker.upper()
                prices[ticker_u] = px
                print(f"[Price] {ticker_u} = {px:.4f} (Yahoo)", flush=True)
        if i == 1 or i == total_batches or (progress_every > 0 and i % max(1, progress_every // 50) == 0):
            print(
                f"[Progress][Price] Yahoo batches {i}/{total_batches}, resolved={len(prices)}/{len(unique_tickers)}",
                flush=True,
            )

    print(f"[Progress][Price] Yahoo done: resolved={len(prices)}/{len(unique_tickers)}", flush=True)
    unresolved = [t for t in unique_tickers if t.upper() not in prices]
    total_unresolved = len(unresolved)
    if total_unresolved == 0:
        return prices

    def _fetch_one_stooq(ticker: str) -> tuple[str, float | None, str]:
        # Try .us first, then raw symbol as fallback.
        attempts = [f"{ticker.lower()}.us", ticker.lower()]
        last_reason = "unknown"
        for sym in attempts:
            for _ in range(2):
                q = urllib.parse.urlencode({"s": sym, "i": "d"})
                url = f"{STOOQ_QUOTE_ENDPOINT}?{q}"
                raw, reason = _http_get_with_retry(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "text/plain,*/*",
                    },
                    timeout=8,
                    limiter=limiter,
                    max_retries=max_retries,
                )
                if not raw:
                    last_reason = reason or "request_failed"
                    continue
                parts = [p.strip() for p in raw.split(",")]
                if len(parts) < 7:
                    last_reason = "bad_csv"
                    continue
                close_px = _as_float(parts[6])
                if close_px and close_px > 0:
                    return ticker.upper(), close_px, "ok"
                if parts[6].upper() in {"N/D", "NA", ""}:
                    last_reason = "no_data"
                else:
                    last_reason = "invalid_price"
        return ticker.upper(), None, last_reason

    max_workers = min(max(1, int(fallback_max_workers)), max(1, total_unresolved))
    wave_size = max(1, int(fallback_wave_size))
    wave_pause = max(0.0, float(fallback_wave_pause_seconds))
    waves = _chunked(unresolved, wave_size)
    total_waves = len(waves)
    done_count = 0
    done_lock = threading.Lock()
    reason_stats: dict[str, int] = {}
    for wave_idx, wave in enumerate(waves, start=1):
        print(
            f"[Progress][Price] Stooq wave {wave_idx}/{total_waves} start: size={len(wave)}",
            flush=True,
        )
        with ThreadPoolExecutor(max_workers=min(max_workers, len(wave))) as pool:
            pending = {pool.submit(_fetch_one_stooq, t) for t in wave}
            stage_start = time.perf_counter()
            last_heartbeat = stage_start
            while pending:
                done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                now = time.perf_counter()
                if done:
                    for fut in done:
                        try:
                            ticker_u, px, reason = fut.result()
                        except Exception:
                            ticker_u, px, reason = "", None, "future_exception"
                        if px is not None and ticker_u:
                            prices[ticker_u] = px
                            print(f"[Price] {ticker_u} = {px:.4f} (Stooq)", flush=True)
                        else:
                            reason_stats[reason] = reason_stats.get(reason, 0) + 1
                        with done_lock:
                            done_count += 1
                            if (
                                done_count == 1
                                or done_count == total_unresolved
                                or (progress_every > 0 and done_count % progress_every == 0)
                            ):
                                print(
                                    f"[Progress][Price] Stooq fallback {done_count}/{total_unresolved}, "
                                    f"resolved={len(prices)}/{len(unique_tickers)}",
                                    flush=True,
                                )
                if heartbeat_seconds > 0 and (now - last_heartbeat) >= heartbeat_seconds:
                    elapsed = now - stage_start
                    print(
                        f"[Progress][Price] Stooq waiting (wave {wave_idx}/{total_waves}): "
                        f"done={done_count}/{total_unresolved}, inflight={len(pending)}, elapsed={elapsed:.1f}s",
                        flush=True,
                    )
                    last_heartbeat = now
                if fallback_max_seconds > 0 and (now - stage_start) >= fallback_max_seconds:
                    for fut in pending:
                        fut.cancel()
                    reason_stats["stage_timeout_cancelled"] = reason_stats.get("stage_timeout_cancelled", 0) + len(pending)
                    print(
                        f"[Progress][Price] Stooq wave {wave_idx}/{total_waves} timeout ({fallback_max_seconds:.1f}s), "
                        f"cancelled={len(pending)}",
                        flush=True,
                    )
                    pending.clear()
                    break
        if wave_pause > 0 and wave_idx < total_waves:
            print(
                f"[Progress][Price] Stooq wave {wave_idx}/{total_waves} pause {wave_pause:.1f}s",
                flush=True,
            )
            time.sleep(wave_pause)
    if reason_stats:
        reason_text = ", ".join(f"{k}:{v}" for k, v in sorted(reason_stats.items()))
        print(f"[Progress][Price] Stooq fail reasons: {reason_text}", flush=True)
    return prices


def fetch_prices_polygon(
    tickers: list[str],
    api_key: str,
    progress_every: int = 200,
    heartbeat_seconds: float = 3.0,
    wave_size: int = 300,
    wave_pause_seconds: float = 1.0,
    max_retries: int = 3,
    min_request_interval_seconds: float = 0.2,
    base_cooldown_seconds: float = 3.0,
    max_workers: int = 5,
    stage_timeout_seconds: float = 0.0,
) -> dict[str, float]:
    prices: dict[str, float] = {}
    if not tickers:
        return prices
    if not api_key:
        raise ValueError("Polygon API key is required (use --polygon-api-key or POLYGON_API_KEY).")

    unique_tickers = sorted(set(tickers))
    limiter = RateLimitController(
        min_interval_seconds=min_request_interval_seconds,
        base_cooldown_seconds=base_cooldown_seconds,
    )

    def _fetch_one_polygon(ticker: str) -> tuple[str, float | None, str]:
        url = (
            POLYGON_PREV_ENDPOINT.format(ticker=urllib.parse.quote(ticker))
            + "?"
            + urllib.parse.urlencode({"adjusted": "true", "apiKey": api_key})
        )
        body, reason = _http_get_with_retry(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=10,
            limiter=limiter,
            max_retries=max_retries,
        )
        if not body:
            return ticker.upper(), None, reason or "request_failed"
        try:
            payload = json.loads(body)
        except ValueError:
            return ticker.upper(), None, "bad_json"
        results = payload.get("results") or []
        if not results:
            return ticker.upper(), None, f"no_data_{payload.get('status', 'unknown')}"
        close_px = _as_float(results[0].get("c"))
        if close_px and close_px > 0:
            return ticker.upper(), close_px, "ok"
        return ticker.upper(), None, "invalid_close"

    all_waves = _chunked(unique_tickers, max(1, int(wave_size)))
    total_waves = len(all_waves)
    done_count = 0
    reason_stats: dict[str, int] = {}
    for wave_idx, wave in enumerate(all_waves, start=1):
        print(
            f"[Progress][Price] Polygon wave {wave_idx}/{total_waves} start: size={len(wave)}",
            flush=True,
        )
        wave_start = time.perf_counter()
        last_hb = wave_start
        wave_done = 0
        for ticker in wave:
            now = time.perf_counter()
            if stage_timeout_seconds > 0 and (now - wave_start) >= stage_timeout_seconds:
                remaining = len(wave) - wave_done
                reason_stats["stage_timeout_cancelled"] = reason_stats.get("stage_timeout_cancelled", 0) + remaining
                print(
                    f"[Progress][Price] Polygon wave {wave_idx}/{total_waves} timeout ({stage_timeout_seconds:.1f}s), "
                    f"skipped={remaining}",
                    flush=True,
                )
                break

            ticker_u, px, reason = _fetch_one_polygon(ticker)
            wave_done += 1
            done_count += 1
            if px is not None:
                prices[ticker_u] = px
                print(f"[Price] {ticker_u} = {px:.4f} (Polygon)", flush=True)
            else:
                reason_stats[reason] = reason_stats.get(reason, 0) + 1

            if (
                done_count == 1
                or done_count == len(unique_tickers)
                or (progress_every > 0 and done_count % progress_every == 0)
            ):
                print(
                    f"[Progress][Price] Polygon {done_count}/{len(unique_tickers)}, "
                    f"resolved_price={len(prices)}",
                    flush=True,
                )
            now = time.perf_counter()
            if heartbeat_seconds > 0 and (now - last_hb) >= heartbeat_seconds:
                print(
                    f"[Progress][Price] Polygon waiting (wave {wave_idx}/{total_waves}): "
                    f"done={wave_done}/{len(wave)}, elapsed={(now - wave_start):.1f}s",
                    flush=True,
                )
                last_hb = now

        if wave_pause_seconds > 0 and wave_idx < total_waves:
            print(
                f"[Progress][Price] Polygon wave {wave_idx}/{total_waves} pause {wave_pause_seconds:.1f}s",
                flush=True,
            )
            time.sleep(wave_pause_seconds)

    if reason_stats:
        reason_text = ", ".join(f"{k}:{v}" for k, v in sorted(reason_stats.items()))
        print(f"[Progress][Price] Polygon fail reasons: {reason_text}", flush=True)
    return prices


def fetch_prices_from_csv(
    csv_path: str,
    symbol_col: str = "symbol",
    price_col: str = "close",
) -> dict[str, float]:
    prices: dict[str, float] = {}
    with open(csv_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        if not reader.fieldnames:
            raise ValueError("Price CSV has no header.")
        lower_map = {name.lower(): name for name in reader.fieldnames}
        symbol_key = lower_map.get(symbol_col.lower())
        price_key = lower_map.get(price_col.lower())
        if not symbol_key or not price_key:
            raise ValueError(
                f"Price CSV missing columns. Need '{symbol_col}' and '{price_col}'. "
                f"Available: {', '.join(reader.fieldnames)}"
            )
        for row in reader:
            sym_raw = (row.get(symbol_key) or "").strip().upper()
            if not sym_raw:
                continue
            sym = sym_raw.replace(".US", "")
            px = _as_float(row.get(price_key))
            if px and px > 0:
                prices[sym] = px
    return prices


def fetch_fx_to_usd(currency_codes: list[str], progress_every: int = 200) -> dict[str, float]:
    """
    Return map like {"USD":1.0, "CNY": 1/USDCNY, "BRL": 1/USDBRL}.
    Values mean: 1 unit of local currency = ? USD.
    """
    need = sorted({c for c in currency_codes if c and c not in _FX_CACHE})
    total_need = len(need)
    for idx, code in enumerate(need, start=1):
        pair = f"usd{code.lower()}"
        url = f"{STOOQ_QUOTE_ENDPOINT}?{urllib.parse.urlencode({'s': pair, 'i': 'd'})}"
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "text/plain,*/*",
            },
        )
        try:
            raw = urllib.request.urlopen(req, timeout=15).read().decode("utf-8", errors="ignore").strip()
            parts = [p.strip() for p in raw.split(",")]
            # CLOSE in col 7: USDXXX close (local currency per 1 USD)
            usd_to_local = _as_float(parts[6]) if len(parts) >= 7 else None
            if usd_to_local and usd_to_local > 0:
                _FX_CACHE[code] = 1.0 / usd_to_local
        except (urllib.error.URLError, TimeoutError, ValueError, IndexError):
            continue
        if idx == 1 or idx == total_need or (progress_every > 0 and idx % progress_every == 0):
            print(
                f"[Progress][FX] fetched {idx}/{total_need}, resolved={len(_FX_CACHE)} currency rates",
                flush=True,
            )

    # Batch fallback for currencies not resolved by Stooq.
    missing = sorted({c for c in need if c not in _FX_CACHE and c != "USD"})
    if missing:
        joined = ",".join(missing)
        # 1 USD = rate_in_local  => local->USD = 1 / rate.
        fallback_urls = [
            f"https://api.frankfurter.app/latest?from=USD&to={urllib.parse.quote(joined)}",
            "https://open.er-api.com/v6/latest/USD",
        ]
        for uidx, url in enumerate(fallback_urls, start=1):
            if not missing:
                break
            try:
                req = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "application/json,text/plain,*/*",
                    },
                )
                raw = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", errors="ignore")
                obj = json.loads(raw)
                rates = obj.get("rates") if isinstance(obj, dict) else None
                if not isinstance(rates, dict):
                    continue
                resolved_now = 0
                for code in list(missing):
                    usd_to_local = _as_float(rates.get(code))
                    if usd_to_local and usd_to_local > 0:
                        _FX_CACHE[code] = 1.0 / usd_to_local
                        missing.remove(code)
                        resolved_now += 1
                print(
                    f"[Progress][FX] fallback {uidx}/{len(fallback_urls)} resolved={resolved_now}, remaining={len(missing)}",
                    flush=True,
                )
            except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
                continue

    if missing:
        sample = ",".join(missing[:20])
        print(f"[FX] unresolved currency codes: {sample}", flush=True)
    return {code: _FX_CACHE[code] for code in set(currency_codes) if code in _FX_CACHE} | {"USD": 1.0}


def build_candidates(
    records: list[dict[str, Any]], prices: dict[str, float], fx_to_usd: dict[str, float], progress_every: int = 200
) -> tuple[list[Candidate], dict[str, Any]]:
    out: list[Candidate] = []
    stats = {
        "condition_matched": 0,
        "missing_shares": 0,
        "missing_price": 0,
        "missing_fx": 0,
        "below_market_cap_threshold": 0,
        "missing_price_tickers": set(),
    }
    total_records = len(records)
    for idx, r in enumerate(records, start=1):
        ticker = (r.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        cur_ast = _as_float(r.get("assets_current"))
        liab = _as_float(r.get("total_liabilities"))
        shares = _as_float(r.get("shares_outstanding"))
        if cur_ast is None or liab is None:
            continue
        if cur_ast <= liab:
            continue
        stats["condition_matched"] += 1
        shares_used = shares
        if shares_used is None:
            stats["missing_shares"] += 1
            continue

        ccy = _parse_currency_code(r.get("currency_unit"))
        local_to_usd = fx_to_usd.get(ccy)
        if local_to_usd is None:
            stats["missing_fx"] += 1
            continue

        cur_ast_usd = cur_ast * local_to_usd
        liab_usd = liab * local_to_usd
        surplus_usd = cur_ast_usd - liab_usd
        ebitda_local = _as_float(r.get("ebitda"))
        ebitda_usd = ebitda_local * local_to_usd if ebitda_local is not None else None
        net_income_local = _as_float(r.get("net_income"))
        net_income_usd = net_income_local * local_to_usd if net_income_local is not None else None

        price = prices.get(ticker)
        if price is None:
            stats["missing_price"] += 1
            stats["missing_price_tickers"].add(ticker)
            continue

        market_cap = shares_used * price
        if market_cap <= 0:
            continue
        if market_cap < 30_000_000:
            stats["below_market_cap_threshold"] += 1
            continue
        pct = surplus_usd / market_cap * 100.0

        out.append(
            Candidate(
                ticker=ticker,
                company_name=(r.get("company_name") or "").strip(),
                currency_unit=r.get("currency_unit"),
                assets_current=cur_ast_usd,
                total_liabilities=liab_usd,
                shares_outstanding=shares,
                shares_used_for_mcap=shares_used,
                fx_to_usd=local_to_usd,
                price=price,
                market_cap=market_cap,
                surplus=surplus_usd,
                surplus_pct_of_mcap=pct,
                ebitda=ebitda_usd,
                net_income=net_income_usd,
            )
        )
        if idx == 1 or idx == total_records or (progress_every > 0 and idx % progress_every == 0):
            print(
                f"[Progress][Analyze] processed {idx}/{total_records}, matched={len(out)}",
                flush=True,
            )
    out.sort(key=lambda x: x.surplus_pct_of_mcap, reverse=True)
    return out, stats


def dedupe_records(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    removed = 0

    score_keys = (
        "assets_current",
        "total_liabilities",
        "shares_outstanding",
        "shares_outstanding_ads_equivalent",
        "ordinary_shares_per_ads",
        "currency_unit",
    )

    def _score(rec: dict[str, Any]) -> tuple[int, int]:
        non_null = 0
        for k in score_keys:
            if rec.get(k) is not None:
                non_null += 1
        path = (rec.get("file_path") or "")
        # Prefer canonical equity ticker folder over warrant-like suffix folders.
        prefer = 1
        if path:
            folder = Path(path).parent.name.upper()
            t = (rec.get("ticker") or "").upper()
            if folder == t:
                prefer = 3
            elif folder.endswith("W") or folder.endswith("WS") or folder.endswith("WT"):
                prefer = 0
            else:
                prefer = 2
        return (non_null, prefer)

    for rec in records:
        ticker = (rec.get("ticker") or "").upper().strip()
        form_type = (rec.get("form_type") or "").upper().strip()
        filing_date = str(rec.get("filing_date") or "").strip()
        period_end = str(rec.get("period_end_date") or "").strip()
        key = (ticker, form_type, filing_date, period_end)
        prev = grouped.get(key)
        if prev is None:
            grouped[key] = rec
            continue
        if _score(rec) > _score(prev):
            grouped[key] = rec
        removed += 1

    return list(grouped.values()), removed


def _parse_loose_date(value: str | None) -> datetime | None:
    if not value:
        return None
    s = " ".join(str(value).split()).strip()
    if not s:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _record_effective_date(rec: dict[str, Any]) -> datetime | None:
    period_raw = " ".join(str(rec.get("period_end_date") or "").split()).strip()
    filing = _parse_loose_date(rec.get("filing_date"))
    period_end = _parse_loose_date(period_raw)
    if period_end is not None:
        return period_end
    if period_raw and filing is not None:
        for fmt in ("%B %d", "%b %d"):
            try:
                md = datetime.strptime(period_raw, fmt)
            except ValueError:
                continue
            candidate = md.replace(year=filing.year)
            # If candidate is implausibly after filing date, use previous year.
            if candidate > filing + timedelta(days=31):
                candidate = candidate.replace(year=filing.year - 1)
            return candidate
    return filing


def _record_quality_score(rec: dict[str, Any]) -> int:
    score_keys = (
        "assets_current",
        "total_liabilities",
        "shares_outstanding",
        "shares_outstanding_ads_equivalent",
        "ordinary_shares_per_ads",
        "currency_unit",
    )
    return sum(1 for k in score_keys if rec.get(k) is not None)


def latest_record_per_ticker(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    grouped: dict[str, dict[str, Any]] = {}
    removed = 0
    for rec in records:
        ticker = (rec.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        prev = grouped.get(ticker)
        if prev is None:
            grouped[ticker] = rec
            continue
        cur_date = _record_effective_date(rec)
        prev_date = _record_effective_date(prev)
        cur_key = (
            cur_date is not None,
            cur_date or datetime.min,
            _record_quality_score(rec),
        )
        prev_key = (
            prev_date is not None,
            prev_date or datetime.min,
            _record_quality_score(prev),
        )
        if cur_key > prev_key:
            grouped[ticker] = rec
        removed += 1
    return list(grouped.values()), removed


def filter_stale_records(records: list[dict[str, Any]], max_age_days: int) -> tuple[list[dict[str, Any]], int]:
    if max_age_days <= 0:
        return records, 0
    cutoff = datetime.now() - timedelta(days=max_age_days)
    kept: list[dict[str, Any]] = []
    removed = 0
    for rec in records:
        dt = _record_effective_date(rec)
        if dt is None or dt >= cutoff:
            kept.append(rec)
        else:
            removed += 1
    return kept, removed


def fmt_money(value: float) -> str:
    sign = "-" if value < 0 else ""
    v = abs(value)
    if v >= 1_000_000_000:
        return f"{sign}{v / 1_000_000_000:.2f}B"
    if v >= 1_000_000:
        return f"{sign}{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{sign}{v / 1_000:.2f}K"
    return f"{sign}{v:.2f}"


def fmt_number(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.2f}"


def fmt_millions_csv(value: float) -> str:
    return f"{value / 1_000_000.0:.2f}"


def write_report(
    candidates: list[Candidate],
    output_path: Path,
    source_path: Path,
    stats: dict[str, Any],
    resolved_prices: int,
) -> None:
    lines: list[str] = []

    if not candidates:
        lines.append("No records matched the condition with available shares and market price.")
        missing = sorted(stats.get("missing_price_tickers", []))
        if missing:
            lines.append(f"Tickers missing price ({len(missing)}): {', '.join(missing)}")
    else:
        lines.append(
            "Ticker  Price      SharesUsed  FX(local->USD)   MktCap     CurAst(USD)  Liab(USD)   CurAst-Liab  (CurAst-Liab)/MktCap"
        )
        for c in candidates:
            lines.append(
                f"{c.ticker:<6}  "
                f"{c.price:>9.2f}  "
                f"{fmt_number(c.shares_used_for_mcap):>10}  "
                f"{c.fx_to_usd:>13.6f}  "
                f"{fmt_money(c.market_cap):>9}  "
                f"{fmt_money(c.assets_current):>11}  "
                f"{fmt_money(c.total_liabilities):>10}  "
                f"{fmt_money(c.surplus):>11}  "
                f"{c.surplus_pct_of_mcap:>8.2f}%"
            )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_report_tsv(candidates: list[Candidate], output_path: Path) -> None:
    headers = [
        "Ticker",
        "Price",
        "SharesUsed",
        "FX(local->USD)",
        "MktCap",
        "CurAst(USD)",
        "Liab(USD)",
        "CurAst-Liab",
        "(CurAst-Liab)/MktCap",
    ]
    lines: list[str] = ["\t".join(headers)]
    for c in candidates:
        lines.append(
            "\t".join(
                [
                    c.ticker,
                    f"{c.price:.2f}",
                    fmt_number(c.shares_used_for_mcap),
                    f"{c.fx_to_usd:.6f}",
                    fmt_money(c.market_cap),
                    fmt_money(c.assets_current),
                    fmt_money(c.total_liabilities),
                    fmt_money(c.surplus),
                    f"{c.surplus_pct_of_mcap:.2f}%",
                ]
            )
        )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_report_csv(candidates: list[Candidate], output_path: Path) -> None:
    headers = [
        "Ticker",
        "Price",
        "SharesUsed(M)",
        "FX(local->USD)",
        "MktCap(USD_M)",
        "CurAst(USD_M)",
        "Liab(USD_M)",
        "EBITDA(USD_M)",
        "NetIncome(USD_M)",
        "CurAst-Liab(USD_M)",
        "(CurAst-Liab)/MktCap",
    ]
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        for c in candidates:
            w.writerow(
                [
                    c.ticker,
                    f"{c.price:.2f}",
                    fmt_millions_csv(c.shares_used_for_mcap),
                    f"{c.fx_to_usd:.6f}",
                    fmt_millions_csv(c.market_cap),
                    fmt_millions_csv(c.assets_current),
                    fmt_millions_csv(c.total_liabilities),
                    fmt_millions_csv(c.ebitda) if c.ebitda is not None else "",
                    fmt_millions_csv(c.net_income) if c.net_income is not None else "",
                    fmt_millions_csv(c.surplus),
                    f"{c.surplus_pct_of_mcap:.2f}%",
                ]
            )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Analyze sec_filing_summary.json with condition assets_current > total_liabilities "
            "and compute surplus as a percentage of market cap."
        )
    )
    p.add_argument(
        "--input",
        default="sec_filing_summary.json",
        help="Path to sec_filing_summary.json (default: sec_filing_summary.json)",
    )
    p.add_argument(
        "--output",
        default="curast_liab_vs_mcap_report.txt",
        help="Path to output txt report (default: curast_liab_vs_mcap_report.txt)",
    )
    p.add_argument(
        "--output-format",
        choices=("txt", "tsv", "csv"),
        default="txt",
        help="Output format: txt (default), tsv, or csv (Numbers-friendly).",
    )
    p.add_argument(
        "--price-provider",
        choices=("polygon", "auto", "local-csv"),
        default="polygon",
        help="Price source: polygon (default), auto (Yahoo+Stooq), or local-csv.",
    )
    p.add_argument(
        "--polygon-api-key",
        default=os.getenv("POLYGON_API_KEY", ""),
        help="Polygon API key (default from POLYGON_API_KEY env).",
    )
    p.add_argument(
        "--polygon-max-workers",
        type=int,
        default=5,
        help="Max worker threads for Polygon stage.",
    )
    p.add_argument(
        "--polygon-wave-size",
        type=int,
        default=300,
        help="Polygon wave size (default: 300 symbols).",
    )
    p.add_argument(
        "--polygon-wave-pause-seconds",
        type=float,
        default=1.0,
        help="Pause seconds between Polygon waves.",
    )
    p.add_argument(
        "--price-file",
        default="",
        help="Local CSV file for local-csv provider.",
    )
    p.add_argument(
        "--price-file-symbol-col",
        default="symbol",
        help="Symbol column name in local price CSV (default: symbol).",
    )
    p.add_argument(
        "--price-file-price-col",
        default="close",
        help="Price column name in local price CSV (default: close).",
    )
    p.add_argument(
        "--progress-every",
        type=int,
        default=200,
        help="Print progress every N items (default: 200). Use 0 to disable.",
    )
    p.add_argument(
        "--progress-heartbeat-seconds",
        type=float,
        default=3.0,
        help="Print heartbeat progress every N seconds during slow network stages.",
    )
    p.add_argument(
        "--price-stage-timeout-seconds",
        type=float,
        default=0.0,
        help="Timeout for Stooq fallback stage in seconds (0 means no timeout).",
    )
    p.add_argument(
        "--price-wave-size",
        type=int,
        default=400,
        help="Stooq fallback wave size (default: 400 symbols per wave).",
    )
    p.add_argument(
        "--price-wave-pause-seconds",
        type=float,
        default=1.0,
        help="Pause seconds between Stooq fallback waves.",
    )
    p.add_argument(
        "--price-max-retries",
        type=int,
        default=3,
        help="Max retries per HTTP request when fetching prices.",
    )
    p.add_argument(
        "--price-min-request-interval-ms",
        type=int,
        default=120,
        help="Global minimum interval between price HTTP requests in milliseconds.",
    )
    p.add_argument(
        "--price-base-cooldown-seconds",
        type=float,
        default=3.0,
        help="Base cooldown seconds after HTTP 429/503 (adaptive backoff).",
    )
    p.add_argument(
        "--price-fallback-max-workers",
        type=int,
        default=6,
        help="Max worker threads for Stooq fallback stage.",
    )
    p.add_argument(
        "--latest-per-ticker",
        dest="latest_per_ticker",
        action="store_true",
        default=True,
        help="Keep only the latest filing per ticker before pricing (default: enabled).",
    )
    p.add_argument(
        "--no-latest-per-ticker",
        dest="latest_per_ticker",
        action="store_false",
        help="Disable latest-per-ticker selection.",
    )
    p.add_argument(
        "--max-report-age-days",
        type=int,
        default=730,
        help="Drop filings older than this many days based on period/filing date (default: 730; 0 disables).",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1

    records = json.loads(input_path.read_text(encoding="utf-8"))
    records = _normalize_input_records(records)
    records, removed_dupes = dedupe_records(records)
    removed_latest = 0
    if args.latest_per_ticker:
        records, removed_latest = latest_record_per_ticker(records)
    records, removed_stale = filter_stale_records(records, max(0, int(args.max_report_age_days)))
    tickers = [(r.get("ticker") or "").upper().strip() for r in records if r.get("ticker")]
    currency_codes = [_parse_currency_code(r.get("currency_unit")) for r in records]
    progress_every = max(0, int(args.progress_every))
    heartbeat = max(0.0, float(args.progress_heartbeat_seconds))
    stage_timeout = max(0.0, float(args.price_stage_timeout_seconds))
    max_retries = max(1, int(args.price_max_retries))
    min_interval = max(0, int(args.price_min_request_interval_ms)) / 1000.0
    cooldown = max(0.1, float(args.price_base_cooldown_seconds))

    if args.price_provider == "polygon":
        print("Price provider: polygon", flush=True)
        try:
            prices = fetch_prices_polygon(
                tickers,
                api_key=args.polygon_api_key,
                progress_every=progress_every,
                heartbeat_seconds=heartbeat,
                wave_size=max(1, int(args.polygon_wave_size)),
                wave_pause_seconds=max(0.0, float(args.polygon_wave_pause_seconds)),
                max_retries=max_retries,
                min_request_interval_seconds=min_interval,
                base_cooldown_seconds=cooldown,
                max_workers=max(1, int(args.polygon_max_workers)),
                stage_timeout_seconds=stage_timeout,
            )
            price_source_used = "polygon"
        except ValueError as exc:
            print(f"[Price] Polygon unavailable: {exc}. Falling back to auto.", flush=True)
            prices = fetch_prices(
                tickers,
                progress_every=progress_every,
                heartbeat_seconds=heartbeat,
                fallback_max_seconds=stage_timeout,
                fallback_wave_size=max(1, int(args.price_wave_size)),
                fallback_wave_pause_seconds=max(0.0, float(args.price_wave_pause_seconds)),
                max_retries=max_retries,
                min_request_interval_seconds=min_interval,
                base_cooldown_seconds=cooldown,
                fallback_max_workers=max(1, int(args.price_fallback_max_workers)),
            )
            price_source_used = "auto-fallback"
    elif args.price_provider == "local-csv":
        print(f"Price provider: local-csv ({args.price_file})", flush=True)
        if not args.price_file:
            print("[Price] local-csv requires --price-file", file=sys.stderr)
            return 2
        prices = fetch_prices_from_csv(
            args.price_file,
            symbol_col=args.price_file_symbol_col,
            price_col=args.price_file_price_col,
        )
        print(f"[Progress][Price] local-csv loaded {len(prices)} symbols", flush=True)
        price_source_used = "local-csv"
    else:
        print("Price provider: auto (Yahoo+Stooq)", flush=True)
        prices = fetch_prices(
            tickers,
            progress_every=progress_every,
            heartbeat_seconds=heartbeat,
            fallback_max_seconds=stage_timeout,
            fallback_wave_size=max(1, int(args.price_wave_size)),
            fallback_wave_pause_seconds=max(0.0, float(args.price_wave_pause_seconds)),
            max_retries=max_retries,
            min_request_interval_seconds=min_interval,
            base_cooldown_seconds=cooldown,
            fallback_max_workers=max(1, int(args.price_fallback_max_workers)),
        )
        price_source_used = "auto"
    fx_to_usd = fetch_fx_to_usd(currency_codes, progress_every=progress_every)
    candidates, stats = build_candidates(records, prices, fx_to_usd, progress_every=progress_every)
    output_format = args.output_format
    if output_format == "txt" and output_path.suffix.lower() in {".tsv", ".csv"}:
        output_format = output_path.suffix.lower().lstrip(".")
    if output_format == "tsv":
        write_report_tsv(candidates, output_path)
    elif output_format == "csv":
        write_report_csv(candidates, output_path)
    else:
        write_report(candidates, output_path, input_path, stats, len(prices))

    print(f"Analyzed {len(records)} record(s).")
    if removed_dupes > 0:
        print(f"Deduped duplicate records: {removed_dupes}")
    if removed_latest > 0:
        print(f"Collapsed to latest-per-ticker records: removed {removed_latest}")
    if removed_stale > 0:
        print(f"Filtered stale filings by age: removed {removed_stale}")
    print(f"Filtered by market cap (<$30M): {stats['below_market_cap_threshold']}")
    print(f"Price source used: {price_source_used}")
    print(f"Resolved {len(prices)} market price(s).")
    print(f"Resolved FX rate(s): {len(fx_to_usd)}")
    print(f"Matched {len(candidates)} record(s).")
    print(f"Output: {output_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
