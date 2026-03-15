from __future__ import annotations

from pathlib import Path
import re


PERIODIC_FORMS = {"10-K", "10-Q", "20-F"}


def _load_base():
    try:
        from src import sec_report_parser as base
    except ImportError:  # pragma: no cover - supports `python -m sec_report_parser` from `src/`
        import sec_report_parser as base
    return base


def _select_metric_from_tags(
    base,
    *,
    metric: str,
    tags: list[str],
    facts_by_name,
    contexts,
    target_end,
    form_type: str,
):
    best_value = None
    best_score = None
    for tag_rank, tag in enumerate(tags):
        for fact in facts_by_name.get(tag, []):
            context = contexts.get(fact.context_ref or "")
            report_date = context.report_date if context else None
            if target_end and report_date and abs((report_date - target_end).days) > base.MAX_FACT_DATE_GAP_DAYS:
                continue
            score = base._score_fact(
                fact=fact,
                metric=metric,
                contexts=contexts,
                target_end=target_end,
                form_type=form_type,
                tag_rank=tag_rank,
            )
            if best_score is None or score < best_score:
                best_score = score
                best_value = fact.value
    return best_value


def _periodic_net_income(base, facts_by_name, contexts, target_end, form_type: str):
    tags = [
        "us-gaap:ProfitLoss",
        "ifrs-full:ProfitLoss",
        "us-gaap:NetIncomeLossAttributableToParentDiluted",
        "us-gaap:NetIncomeLoss",
    ]
    return _select_metric_from_tags(
        base,
        metric="net_income",
        tags=tags,
        facts_by_name=facts_by_name,
        contexts=contexts,
        target_end=target_end,
        form_type=form_type,
    )


def _extract_periodic_capex_from_narrative(base, lines: list[str]) -> float | None:
    joined = " ".join(lines[:2000])
    patterns = [
        r"(?i)total\s+consolidated\s+capital\s+expenditures\s+were\s+\$?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
        r"(?i)capital\s+expenditures\s+were\s+\$?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ]
    multipliers = {"thousand": 1_000.0, "million": 1_000_000.0, "billion": 1_000_000_000.0}
    for pattern in patterns:
        match = re.search(pattern, joined)
        if not match:
            continue
        value = float(match.group(1).replace(",", ""))
        scale = match.group(2).lower()
        return value * multipliers[scale]
    return None


def _is_financial_like_periodic(base, report, lines: list[str]) -> bool:
    company_norm = base._normalize_label(report.company_name or "")
    preview = base._normalize_label(" ".join(lines[:3000]))
    company_signals = (
        "bank",
        "banc",
        "brokers",
        "broker",
        "financial",
        "insurance",
        "securities",
        "capital markets",
    )
    text_signals = (
        "net revenues",
        "income before income taxes",
        "total liabilities and equity",
        "brokerage industry",
        "net interest income",
        "insurance contract liabilities",
    )
    if any(token in company_norm for token in company_signals):
        return True
    hits = sum(1 for token in text_signals if token in preview)
    return hits >= 3


def _extract_financial_like_summary_amount(base, preview_text: str, patterns: list[str]) -> float | None:
    for pattern in patterns:
        match = re.search(pattern, preview_text, flags=re.IGNORECASE)
        if not match:
            continue
        amount = base._parse_number(match.group("amount"))
        if amount is None:
            continue
        unit = (match.groupdict().get("unit") or "").lower()
        multiplier = {
            "thousand": 1_000.0,
            "million": 1_000_000.0,
            "billion": 1_000_000_000.0,
            "trillion": 1_000_000_000_000.0,
        }.get(unit, 1.0)
        return amount * multiplier
    return None


def _extract_financial_like_row_values(base, lines: list[str], labels: list[str]) -> list[float] | None:
    normalized_labels = [base._normalize_label(label) for label in labels]
    for idx, line in enumerate(lines):
        normalized_line = base._normalize_label(line)
        if not any(normalized_line == label or label in normalized_line for label in normalized_labels):
            continue
        values: list[float] = []
        for follow in lines[idx + 1 : min(len(lines), idx + 12)]:
            cleaned = base._clean_text(follow)
            parsed = base._parse_number(cleaned)
            if parsed is not None:
                values.append(parsed)
                continue
            if values and re.search(r"[A-Za-z]", cleaned):
                break
        if values:
            return values
    return None


def _extract_financial_like_balance_sheet_total_equity(base, lines: list[str], multiplier: float) -> float | None:
    for idx, line in enumerate(lines):
        if base._normalize_label(line) != "total equity":
            continue
        values: list[float] = []
        for follow in lines[idx + 1 : min(len(lines), idx + 8)]:
            cleaned = base._clean_text(follow)
            parsed = base._parse_number(cleaned)
            if parsed is not None:
                values.append(parsed)
                continue
            if values and re.search(r"[A-Za-z]", cleaned):
                break
        if values:
            return values[0] * multiplier
    return None


def _apply_financial_like_periodic_overrides(base, report, lines: list[str]) -> None:
    multiplier = base._unit_multiplier(report.currency_unit)
    preview = base._clean_text(" ".join(lines[:2500]))

    financial_revenue = _extract_financial_like_summary_amount(
        base,
        preview,
        [
            r"net revenues? were\s+\$?\s*(?P<amount>[0-9][0-9,]*(?:\.\d+)?)\s*(?P<unit>thousand|million|billion|trillion)\b",
            r"total net revenues?,?\s+for the current year, increased[^.]{0,120}?to\s+\$?\s*(?P<amount>[0-9][0-9,]*(?:\.\d+)?)\s*(?P<unit>thousand|million|billion|trillion)\b",
        ],
    )
    if financial_revenue is None and multiplier > 1.0:
        revenue_row = _extract_financial_like_row_values(base, lines, ["Total net revenues", "Net revenues"])
        if revenue_row:
            financial_revenue = revenue_row[0] * multiplier
    if financial_revenue is not None and (
        report.revenue is None
        or report.revenue <= 0
        or report.revenue < financial_revenue * 0.5
        or report.revenue > financial_revenue * 1.5
    ):
        report.revenue = financial_revenue

    if multiplier > 1.0:
        equity_value = _extract_financial_like_balance_sheet_total_equity(base, lines, multiplier)
        if equity_value is not None:
            report.equity = equity_value

    if report.total_assets is not None and report.total_liabilities is not None:
        derived_equity = report.total_assets - report.total_liabilities
        if report.equity is None:
            report.equity = derived_equity
        else:
            gap = abs(report.equity - derived_equity)
            tolerance = max(1.0, abs(derived_equity) * 0.1)
            if gap > tolerance:
                report.equity = derived_equity

    if (
        report.assets_current is not None
        and report.total_assets is not None
        and report.assets_current > report.total_assets * 1.05
    ):
        report.assets_current = None


def _apply_periodic_fallbacks(base, report, facts_by_name, contexts, target_end, lines: list[str]) -> None:
    if report.interest_income is not None and report.interest_income < 0:
        report.interest_income = None

    if report.cash is None:
        restricted_cash = _select_metric_from_tags(
            base,
            metric="cash",
            tags=["us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"],
            facts_by_name=facts_by_name,
            contexts=contexts,
            target_end=target_end,
            form_type=report.form_type,
        )
        if restricted_cash is not None:
            report.cash = restricted_cash

    if report.capex is None:
        narrative_capex = _extract_periodic_capex_from_narrative(base, lines)
        if narrative_capex is not None:
            report.capex = narrative_capex


def _finalize_periodic_report(base, report) -> None:
    for expense_metric in (
        "cogs",
        "research_and_development",
        "selling_and_marketing",
        "general_and_administrative",
        "sga",
    ):
        value = getattr(report, expense_metric)
        if value is not None:
            setattr(report, expense_metric, abs(value))

    if report.selling_and_marketing is not None or report.general_and_administrative is not None:
        derived_sga = (report.selling_and_marketing or 0.0) + (report.general_and_administrative or 0.0)
        if report.sga is None:
            report.sga = derived_sga
        else:
            mismatch = abs(report.sga - derived_sga)
            tolerance = max(1.0, abs(derived_sga) * 0.05)
            if mismatch > tolerance:
                report.sga = derived_sga

    if report.revenue is not None and report.cogs is not None:
        derived_gross = report.revenue - report.cogs
        if report.gross_profit is None:
            report.gross_profit = derived_gross
        else:
            mismatch = abs(report.gross_profit - derived_gross)
            tolerance = max(1.0, abs(derived_gross) * 0.05)
            if mismatch > tolerance:
                report.gross_profit = derived_gross

    if report.tax_expense is not None and report.pretax_income is not None and report.net_income is not None:
        as_is_gap = abs((report.pretax_income - report.tax_expense) - report.net_income)
        flipped_gap = abs((report.pretax_income + report.tax_expense) - report.net_income)
        if flipped_gap + 1 < as_is_gap:
            report.tax_expense *= -1

    if report.capex is not None:
        report.capex = -abs(report.capex)
    if report.acquisitions is not None:
        report.acquisitions = -abs(report.acquisitions)

    if report.operating_income is not None and report.depreciation_and_amortization is not None:
        derived_ebitda = report.operating_income + report.depreciation_and_amortization
        if report.ebitda is None:
            report.ebitda = derived_ebitda
        else:
            gap = abs(report.ebitda - derived_ebitda)
            base_value = max(1.0, abs(report.ebitda), abs(derived_ebitda))
            if gap / base_value > 0.2:
                report.ebitda = derived_ebitda

    if report.free_cash_flow is None and report.operating_cash_flow is not None and report.capex is not None:
        report.free_cash_flow = report.operating_cash_flow + report.capex

    if report.eps_basic is None and report.eps_diluted is not None:
        report.eps_basic = report.eps_diluted
    if report.weighted_avg_shares_basic is None and report.weighted_avg_shares_diluted is not None:
        report.weighted_avg_shares_basic = report.weighted_avg_shares_diluted
    if (
        report.weighted_avg_shares_basic is not None
        and report.weighted_avg_shares_diluted is not None
        and report.weighted_avg_shares_diluted < report.weighted_avg_shares_basic * 0.2
    ):
        report.weighted_avg_shares_diluted = report.weighted_avg_shares_basic
    if report.shares_outstanding is None and report.weighted_avg_shares_basic is not None:
        report.shares_outstanding = report.weighted_avg_shares_basic
    if (
        report.shares_outstanding is not None
        and report.weighted_avg_shares_basic is not None
        and report.shares_outstanding < report.weighted_avg_shares_basic * 0.2
    ):
        report.shares_outstanding = report.weighted_avg_shares_basic

    if report.total_assets is not None and report.equity is not None:
        derived_liabilities = report.total_assets - report.equity
        if derived_liabilities > 0:
            if report.total_liabilities is None:
                report.total_liabilities = derived_liabilities
            else:
                liabilities_too_small_for_current = (
                    report.liabilities_current is not None and report.total_liabilities < report.liabilities_current * 0.9
                )
                liabilities_implausibly_small = (
                    report.total_assets > 0 and report.total_liabilities < report.total_assets * 0.1
                )
                gap = abs(report.total_liabilities - derived_liabilities)
                tolerance = max(1.0, abs(derived_liabilities) * 0.2)
                if (liabilities_too_small_for_current or liabilities_implausibly_small) and gap > tolerance:
                    report.total_liabilities = derived_liabilities

    if report.equity is None and report.total_assets is not None and report.total_liabilities is not None:
        report.equity = report.total_assets - report.total_liabilities

    report.gross_margin = base._safe_ratio(report.gross_profit, report.revenue)
    report.operating_margin = base._safe_ratio(report.operating_income, report.revenue)
    report.net_margin = base._safe_ratio(report.net_income, report.revenue)
    report.ebitda_margin = base._safe_ratio(report.ebitda, report.revenue)
    report.return_on_equity = base._safe_ratio(report.net_income, report.equity)
    report.return_on_assets = base._safe_ratio(report.net_income, report.total_assets)
    report.current_ratio = base._safe_ratio(report.assets_current, report.liabilities_current)
    report.debt_to_equity = base._safe_ratio(report.total_liabilities, report.equity)
    report.asset_turnover = base._safe_ratio(report.revenue, report.total_assets)


def parse_periodic_filing_v2(path: Path):
    base = _load_base()
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None

    form_type = base._detect_form_type(path, raw)
    if form_type not in PERIODIC_FORMS:
        return None

    company_name = base._extract_non_numeric_fact(raw, "dei:EntityRegistrantName")
    if company_name is None:
        header_match = base.re.search(r"(?i)COMPANY CONFORMED NAME:\s*(.+)", raw)
        if header_match:
            company_name = header_match.group(1).strip(" \t\r\n.;")

    period_end_date = base._extract_period_end(raw, form_type)
    filing_date = base._extract_filing_date(path, raw)
    target_end = base._parse_date(period_end_date)
    analysis_raw = base._select_analysis_raw(raw, form_type)
    lines = base._strip_html_to_lines(analysis_raw)

    ticker = base._extract_ticker_from_path(path) or base._extract_non_numeric_fact(raw, "dei:TradingSymbol") or path.parent.name.upper()
    statement_currency_unit = base._infer_currency_unit_from_statement_headers(lines)
    currency_unit = statement_currency_unit or base._extract_currency_unit(analysis_raw)
    ordinary_shares_per_depositary_share = (
        base._resolve_depositary_share_ratio(path, form_type, lines) if form_type == "20-F" else None
    )

    all_tag_names = {tag for tags in base.METRIC_TAGS.values() for tag in tags}
    all_tag_names.update(
        {
            "us-gaap:ProfitLoss",
            "us-gaap:NetIncomeLossAttributableToParentDiluted",
        }
    )

    contexts = base._parse_contexts(raw)
    facts = base._extract_inline_facts(raw)
    seen_keys = {(fact.name, fact.context_ref, fact.value) for fact in facts}
    for fact in base._extract_tag_facts(raw, all_tag_names):
        key = (fact.name, fact.context_ref, fact.value)
        if key not in seen_keys:
            facts.append(fact)
            seen_keys.add(key)

    facts_by_name = {}
    for fact in facts:
        facts_by_name.setdefault(fact.name, []).append(fact)

    report = base.FilingReport(
        file_path=str(path),
        ticker=ticker,
        company_name=company_name,
        form_type=form_type,
        form_explanation=base.FORM_EXPLANATIONS[form_type],
        filing_date=filing_date,
        period_end_date=period_end_date,
        currency_unit=currency_unit,
    )

    for metric in base.METRIC_TAGS:
        if metric == "ebitda_direct":
            continue
        if metric == "net_income" and form_type in {"10-K", "10-Q"}:
            value = _periodic_net_income(base, facts_by_name, contexts, target_end, form_type)
        elif metric == "shares_outstanding":
            value = base._select_shares_outstanding(facts_by_name, contexts, target_end)
        else:
            value = base._select_metric(metric, facts_by_name, contexts, target_end, form_type)
        if metric in base.SHARE_COUNT_METRICS | base.EPS_METRICS:
            value = base._normalize_depositary_metric(
                metric,
                None,
                value,
                ordinary_shares_per_depositary_share,
            )
        setattr(report, metric, value)

    report.ebitda = base._select_metric("ebitda_direct", facts_by_name, contexts, target_end, form_type)

    if form_type == "20-F":
        base._apply_20f_statement_fallbacks(report, lines, currency_unit)
        base._apply_20f_operations_statement_fallbacks(report, lines, currency_unit)

    base._maybe_correct_duplicate_thousand_scale_annual_report(report, lines, statement_currency_unit)
    _apply_periodic_fallbacks(base, report, facts_by_name, contexts, target_end, lines)
    if _is_financial_like_periodic(base, report, lines):
        _apply_financial_like_periodic_overrides(base, report, lines)
    _finalize_periodic_report(base, report)
    return report
