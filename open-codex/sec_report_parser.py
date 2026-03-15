from __future__ import annotations

import argparse
import html
import json
import multiprocessing
import os
import re
import signal
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from fractions import Fraction
from pathlib import Path
from typing import Any, TextIO

try:
    from src.sec_6k_v2 import build_6k_selection
except ImportError:  # pragma: no cover - supports `python -m sec_report_parser` from `src/`
    from sec_6k_v2 import build_6k_selection

try:
    from src.sec_periodic_v2 import parse_periodic_filing_v2
except ImportError:  # pragma: no cover - supports `python -m sec_report_parser` from `src/`
    from sec_periodic_v2 import parse_periodic_filing_v2

TEXT_EXTENSIONS = {".htm", ".html", ".txt", ".xml", ".xhtml"}
SUPPORTED_FORMS = ("10-K", "10-Q", "6-K", "20-F")
MAX_FACT_DATE_GAP_DAYS = 200
DEFAULT_PROGRESS_EVERY = 50
DEFAULT_FILE_TIMEOUT_SECONDS = 45.0
DEFAULT_SEC_DATA_DIR = "/Users/guowenyong/sec-data"

FORM_EXPLANATIONS = {
    "10-K": "美股发行人的年度报告，覆盖完整财年。",
    "10-Q": "美股发行人的季度报告，覆盖单季度或累计季度。",
    "6-K": "外国发行人向 SEC 提交的临时/阶段性报告，常见于季度经营更新或重大事项。",
    "20-F": "外国发行人的年度报告，对应美股本土公司的 10-K。",
}

DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%B %d, %Y", "%b %d, %Y")

NUMBER_WORDS = {
    "one": 1.0,
    "two": 2.0,
    "three": 3.0,
    "four": 4.0,
    "five": 5.0,
    "six": 6.0,
    "seven": 7.0,
    "eight": 8.0,
    "nine": 9.0,
    "ten": 10.0,
    "eleven": 11.0,
    "twelve": 12.0,
    "thirteen": 13.0,
    "fourteen": 14.0,
    "fifteen": 15.0,
    "sixteen": 16.0,
    "seventeen": 17.0,
    "eighteen": 18.0,
    "nineteen": 19.0,
    "twenty": 20.0,
    "thirty": 30.0,
    "forty": 40.0,
    "fifty": 50.0,
    "sixty": 60.0,
    "seventy": 70.0,
    "eighty": 80.0,
    "ninety": 90.0,
}

NUMBER_WORD_PATTERN = "|".join(sorted(NUMBER_WORDS, key=len, reverse=True))

SHARE_COUNT_METRICS = {
    "shares_outstanding",
    "weighted_avg_shares_basic",
    "weighted_avg_shares_diluted",
}

EPS_METRICS = {"eps_basic", "eps_diluted"}

DURATION_METRICS = {
    "revenue",
    "cogs",
    "gross_profit",
    "operating_income",
    "research_and_development",
    "selling_and_marketing",
    "general_and_administrative",
    "sga",
    "pretax_income",
    "tax_expense",
    "net_income",
    "interest_income",
    "ebitda_direct",
    "eps_basic",
    "eps_diluted",
    "operating_cash_flow",
    "investing_cash_flow",
    "financing_cash_flow",
    "depreciation_and_amortization",
    "share_based_compensation",
    "capex",
    "acquisitions",
    "weighted_avg_shares_basic",
    "weighted_avg_shares_diluted",
}

INSTANT_METRICS = {
    "total_assets",
    "assets_current",
    "total_liabilities",
    "liabilities_current",
    "equity",
    "cash",
    "short_term_investments",
    "goodwill",
    "accounts_receivable",
    "accounts_payable",
    "deferred_revenue",
    "retained_earnings",
    "shares_outstanding",
}

METRIC_TAGS: dict[str, list[str]] = {
    "revenue": [
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        "us-gaap:Revenues",
        "us-gaap:SalesRevenueNet",
        "ifrs-full:Revenue",
        "ifrs-full:RevenueFromContractsWithCustomers",
    ],
    "cogs": [
        "us-gaap:CostOfGoodsAndServicesSold",
        "us-gaap:CostOfRevenue",
        "ifrs-full:CostOfSales",
        "ifrs-full:CostOfGoodsSold",
    ],
    "gross_profit": ["us-gaap:GrossProfit", "ifrs-full:GrossProfit"],
    "operating_income": [
        "us-gaap:OperatingIncomeLoss",
        "ifrs-full:ProfitLossFromOperatingActivities",
        "ifrs-full:OperatingProfitLoss",
    ],
    "research_and_development": [
        "us-gaap:ResearchAndDevelopmentExpense",
        "ifrs-full:ResearchAndDevelopmentExpense",
    ],
    "selling_and_marketing": [
        "us-gaap:SellingAndMarketingExpense",
        "ifrs-full:SellingAndDistributionExpense",
        "ifrs-full:SalesAndMarketingExpense",
    ],
    "general_and_administrative": [
        "us-gaap:GeneralAndAdministrativeExpense",
        "ifrs-full:AdministrativeExpense",
        "ifrs-full:GeneralAndAdministrativeExpense",
    ],
    "sga": ["us-gaap:SellingGeneralAndAdministrativeExpense"],
    "pretax_income": [
        "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        "us-gaap:IncomeBeforeTax",
        "ifrs-full:ProfitLossBeforeTax",
    ],
    "tax_expense": [
        "us-gaap:IncomeTaxExpenseBenefit",
        "ifrs-full:IncomeTaxExpenseContinuingOperations",
    ],
    "net_income": ["us-gaap:NetIncomeLoss", "us-gaap:ProfitLoss", "ifrs-full:ProfitLoss"],
    "interest_income": [
        "us-gaap:InterestIncomeOperating",
        "us-gaap:InvestmentIncomeInterest",
        "us-gaap:InterestIncomeExpenseNonoperatingNet",
        "us-gaap:InterestAndOtherIncome",
        "ifrs-full:FinanceIncome",
        "ifrs-full:InterestRevenueExpense",
    ],
    "ebitda_direct": [
        "us-gaap:EarningsBeforeInterestTaxesDepreciationAndAmortization",
        "us-gaap:EarningsBeforeInterestTaxesAndDepreciationAndAmortization",
        "ifrs-full:EarningsBeforeInterestTaxesDepreciationAndAmortisation",
    ],
    "eps_basic": [
        "us-gaap:EarningsPerShareBasic",
        "ifrs-full:BasicEarningsLossPerShare",
    ],
    "eps_diluted": [
        "us-gaap:EarningsPerShareDiluted",
        "ifrs-full:DilutedEarningsLossPerShare",
    ],
    "total_assets": ["us-gaap:Assets", "ifrs-full:Assets"],
    "assets_current": ["us-gaap:AssetsCurrent", "ifrs-full:CurrentAssets"],
    "total_liabilities": ["us-gaap:Liabilities", "ifrs-full:Liabilities"],
    "liabilities_current": ["us-gaap:LiabilitiesCurrent", "ifrs-full:CurrentLiabilities"],
    "equity": [
        "us-gaap:StockholdersEquity",
        "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        "ifrs-full:Equity",
        "ifrs-full:NetAssetsLiabilities",
    ],
    "cash": [
        "us-gaap:CashAndCashEquivalentsAtCarryingValue",
        "us-gaap:Cash",
        "ifrs-full:CashAndCashEquivalents",
    ],
    "short_term_investments": [
        "us-gaap:ShortTermInvestments",
        "us-gaap:MarketableSecuritiesCurrent",
        "us-gaap:DebtSecuritiesHeldToMaturityAmortizedCostAfterAllowanceForCreditLossCurrent",
        "ifrs-full:CurrentFinancialAssets",
        "ifrs-full:OtherCurrentFinancialAssets",
    ],
    "goodwill": ["us-gaap:Goodwill", "ifrs-full:Goodwill"],
    "accounts_receivable": [
        "us-gaap:AccountsReceivableNetCurrent",
        "ifrs-full:CurrentTradeReceivables",
        "ifrs-full:TradeAndOtherCurrentReceivables",
    ],
    "accounts_payable": [
        "us-gaap:AccountsPayableCurrent",
        "ifrs-full:TradeAndOtherCurrentPayables",
    ],
    "deferred_revenue": [
        "us-gaap:ContractWithCustomerLiabilityCurrent",
        "us-gaap:DeferredRevenueCurrent",
        "ifrs-full:CurrentContractLiabilities",
    ],
    "retained_earnings": [
        "us-gaap:RetainedEarningsAccumulatedDeficit",
        "ifrs-full:RetainedEarnings",
    ],
    "operating_cash_flow": [
        "us-gaap:NetCashProvidedByUsedInOperatingActivities",
        "ifrs-full:CashFlowsFromUsedInOperatingActivities",
    ],
    "investing_cash_flow": [
        "us-gaap:NetCashProvidedByUsedInInvestingActivities",
        "ifrs-full:CashFlowsFromUsedInInvestingActivities",
    ],
    "financing_cash_flow": [
        "us-gaap:NetCashProvidedByUsedInFinancingActivities",
        "ifrs-full:CashFlowsFromUsedInFinancingActivities",
    ],
    "depreciation_and_amortization": [
        "us-gaap:DepreciationDepletionAndAmortization",
        "us-gaap:DepreciationAmortizationAndAccretionNet",
        "ifrs-full:DepreciationAmortisationAndImpairmentExpense",
    ],
    "share_based_compensation": [
        "us-gaap:ShareBasedCompensation",
        "ifrs-full:SharebasedPaymentExpense",
    ],
    "capex": [
        "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
        "ifrs-full:PurchaseOfPropertyPlantAndEquipment",
    ],
    "acquisitions": [
        "us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired",
        "ifrs-full:CashFlowsUsedInObtainingControlOfSubsidiariesOrOtherBusinesses",
    ],
    "shares_outstanding": [
        "dei:EntityCommonStockSharesOutstanding",
        "us-gaap:CommonStockSharesOutstanding",
        "ifrs-full:NumberOfSharesOutstanding",
        "ifrs-full:NumberOfSharesIssued",
    ],
    "weighted_avg_shares_basic": [
        "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
        "ifrs-full:WeightedAverageNumberOfSharesOutstanding",
        "ifrs-full:WeightedAverageNumberOfOrdinarySharesOutstandingBasic",
        "ifrs-full:WeightedAverageShares",
    ],
    "weighted_avg_shares_diluted": [
        "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
        "ifrs-full:WeightedAverageNumberOfSharesOutstandingDiluted",
        "ifrs-full:AdjustedWeightedAverageShares",
    ],
}

TEXT_LABELS: dict[str, list[str]] = {
    "revenue": ["total revenue", "total revenues", "net revenue", "net revenues", "revenues"],
    "cogs": ["cost of revenue", "cost of revenues", "cost of sales", "costs of revenues"],
    "gross_profit": ["gross profit"],
    "operating_income": ["operating income", "income from operations", "operating profit"],
    "research_and_development": ["research and development"],
    "selling_and_marketing": ["selling and marketing", "sales and marketing", "selling expenses"],
    "general_and_administrative": ["general and administrative", "administrative expenses"],
    "pretax_income": ["income before income taxes", "income before tax", "profit before tax"],
    "tax_expense": ["income tax expense", "tax expense", "income tax benefit"],
    "net_income": ["net income", "net earnings", "profit for the period"],
    "interest_income": ["interest income", "investment income"],
    "total_assets": ["total assets"],
    "assets_current": ["total current assets"],
    "total_liabilities": ["total liabilities"],
    "liabilities_current": ["total current liabilities"],
    "equity": ["total equity", "total stockholders' equity", "total shareholders' equity"],
    "cash": ["cash and cash equivalents"],
    "short_term_investments": ["short-term investments", "short term investments"],
    "goodwill": ["goodwill"],
    "accounts_receivable": ["accounts receivable", "trade receivables"],
    "accounts_payable": ["accounts payable", "trade payables"],
    "deferred_revenue": ["deferred revenue", "contract liabilities"],
    "retained_earnings": ["retained earnings", "accumulated deficit"],
    "operating_cash_flow": ["net cash provided by operating activities", "net cash from operating activities"],
    "investing_cash_flow": ["net cash used in investing activities", "net cash from investing activities"],
    "financing_cash_flow": ["net cash provided by financing activities", "net cash from financing activities"],
    "depreciation_and_amortization": ["depreciation and amortization"],
    "share_based_compensation": ["share-based compensation", "stock-based compensation"],
    "capex": ["purchase of property and equipment", "purchases of property and equipment", "capital expenditures"],
    "acquisitions": ["payments to acquire businesses", "acquisitions"],
    "shares_outstanding": ["shares outstanding", "ordinary shares outstanding"],
    "weighted_avg_shares_basic": ["weighted average number of shares outstanding basic"],
    "weighted_avg_shares_diluted": ["weighted average number of diluted shares outstanding"],
}

INLINE_XBRL_TEXT_FALLBACK_BLOCKLIST = {
    "general_and_administrative",
    "pretax_income",
    "tax_expense",
    "net_income",
    "interest_income",
    "ebitda",
    "goodwill",
    "accounts_payable",
    "depreciation_and_amortization",
    "share_based_compensation",
    "capex",
    "acquisitions",
}

NARRATIVE_PATTERNS: dict[str, list[str]] = {
    "revenue": [
        r"(?i)(?:total\s+)?net\s+revenues?.{0,160}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
        r"(?i)total\s+revenues?.{0,120}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
        r"(?i)(?:total\s+)?net\s+revenues?.{0,160}?\bto\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "cogs": [
        r"(?i)total\s+costs?\s+of\s+revenues?.{0,120}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
        r"(?i)costs?\s+of\s+revenues?.{0,120}?\bto\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "operating_income": [
        r"(?i)income\s+from\s+operations?.{0,120}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
        r"(?i)(?<!other\s)operating\s+(?:income|profit).{0,120}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "net_income": [
        r"(?i)net\s+income(?:\s+attributable\s+to\s+ordinary\s+shareholders)?.{0,160}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "pretax_income": [
        r"(?i)income\s+before\s+income\s+tax(?:es)?\s+expense.{0,120}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "tax_expense": [
        r"(?i)income\s+tax(?:es)?\s+expense.{0,120}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "interest_income": [
        r"(?i)interest\s+income.{0,120}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "ebitda": [
        r"(?i)\bebitda\b.{0,120}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "total_assets": [
        r"(?i)total\s+assets.{0,120}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "operating_cash_flow": [
        r"(?i)operating\s+cash\s+(?:inflow|outflow).{0,80}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
        r"(?i)net\s+cash\s+(?:generated|provided)\s+from\s+operating\s+activities.{0,80}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "investing_cash_flow": [
        r"(?i)investing\s+cash\s+(?:inflow|outflow).{0,80}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
    "financing_cash_flow": [
        r"(?i)financing\s+cash\s+(?:inflow|outflow).{0,80}?(?:was|were)\s+(?:rmb|cny|usd|us\$|hkd|hk\$)?\s*([0-9][0-9,]*(?:\.\d+)?)\s*(thousand|million|billion)",
    ],
}

STATEMENT_STOP_PATTERNS = [
    r"(?i)condensed consolidated statements of income",
    r"(?i)condensed consolidated statements of comprehensive income",
    r"(?i)condensed consolidated statements of cash flows",
    r"(?i)unaudited condensed consolidated statements of income",
    r"(?i)unaudited condensed consolidated statements of comprehensive income",
    r"(?i)unaudited condensed consolidated balance sheets",
    r"(?i)unaudited interim condensed consolidated statements of financial position",
]

OUTPUT_FIELDS = [
    "file_path",
    "ticker",
    "company_name",
    "form_type",
    "form_explanation",
    "filing_date",
    "period_end_date",
    "currency_unit",
    "revenue",
    "cogs",
    "gross_profit",
    "operating_income",
    "research_and_development",
    "selling_and_marketing",
    "general_and_administrative",
    "sga",
    "pretax_income",
    "tax_expense",
    "net_income",
    "ebitda",
    "interest_income",
    "eps_basic",
    "eps_diluted",
    "total_assets",
    "assets_current",
    "total_liabilities",
    "liabilities_current",
    "equity",
    "cash",
    "short_term_investments",
    "goodwill",
    "accounts_receivable",
    "accounts_payable",
    "deferred_revenue",
    "retained_earnings",
    "operating_cash_flow",
    "investing_cash_flow",
    "financing_cash_flow",
    "free_cash_flow",
    "depreciation_and_amortization",
    "share_based_compensation",
    "capex",
    "acquisitions",
    "shares_outstanding",
    "weighted_avg_shares_basic",
    "weighted_avg_shares_diluted",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "ebitda_margin",
    "return_on_equity",
    "return_on_assets",
    "current_ratio",
    "debt_to_equity",
    "asset_turnover",
]


@dataclass
class ContextInfo:
    context_id: str
    start_date: datetime | None
    end_date: datetime | None
    instant_date: datetime | None
    has_segment: bool = False
    segment_key: str | None = None

    @property
    def report_date(self) -> datetime | None:
        return self.instant_date or self.end_date

    @property
    def duration_days(self) -> int | None:
        if self.start_date and self.end_date:
            return (self.end_date - self.start_date).days
        return None


@dataclass
class Fact:
    name: str
    value: float
    context_ref: str | None
    unit_ref: str | None


@dataclass
class MetricCandidate:
    metric: str
    value: float
    source: str
    confidence: float = 1.0
    note: str | None = None


@dataclass
class FilingReport:
    file_path: str
    ticker: str | None
    company_name: str | None
    form_type: str
    form_explanation: str
    filing_date: str | None
    period_end_date: str | None
    currency_unit: str | None
    revenue: float | None = None
    cogs: float | None = None
    gross_profit: float | None = None
    operating_income: float | None = None
    research_and_development: float | None = None
    selling_and_marketing: float | None = None
    general_and_administrative: float | None = None
    sga: float | None = None
    pretax_income: float | None = None
    tax_expense: float | None = None
    net_income: float | None = None
    ebitda: float | None = None
    interest_income: float | None = None
    eps_basic: float | None = None
    eps_diluted: float | None = None
    total_assets: float | None = None
    assets_current: float | None = None
    total_liabilities: float | None = None
    liabilities_current: float | None = None
    equity: float | None = None
    cash: float | None = None
    short_term_investments: float | None = None
    goodwill: float | None = None
    accounts_receivable: float | None = None
    accounts_payable: float | None = None
    deferred_revenue: float | None = None
    retained_earnings: float | None = None
    operating_cash_flow: float | None = None
    investing_cash_flow: float | None = None
    financing_cash_flow: float | None = None
    free_cash_flow: float | None = None
    depreciation_and_amortization: float | None = None
    share_based_compensation: float | None = None
    capex: float | None = None
    acquisitions: float | None = None
    shares_outstanding: float | None = None
    weighted_avg_shares_basic: float | None = None
    weighted_avg_shares_diluted: float | None = None
    gross_margin: float | None = None
    operating_margin: float | None = None
    net_margin: float | None = None
    ebitda_margin: float | None = None
    return_on_equity: float | None = None
    return_on_assets: float | None = None
    current_ratio: float | None = None
    debt_to_equity: float | None = None
    asset_turnover: float | None = None


SIX_K_INCOME_CANDIDATE_METRICS = (
    "revenue",
    "cogs",
    "gross_profit",
    "operating_income",
    "research_and_development",
    "selling_and_marketing",
    "general_and_administrative",
    "sga",
    "pretax_income",
    "tax_expense",
    "net_income",
    "interest_income",
    "eps_basic",
    "eps_diluted",
    "weighted_avg_shares_basic",
    "weighted_avg_shares_diluted",
)

SIX_K_SHARE_CANDIDATE_METRICS = (
    "shares_outstanding",
    "weighted_avg_shares_basic",
    "weighted_avg_shares_diluted",
    "eps_basic",
    "eps_diluted",
)


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    text = " ".join(str(value).split())
    text = re.sub(r"(?i)\b(Sept)\b", "Sep", text)
    text = re.sub(r"(?i)\b(Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.", r"\1", text)
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _format_date(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.strftime("%B %d, %Y")


def _clean_text(value: str) -> str:
    text = re.sub(r"(?is)<[^>]+>", "", value)
    return " ".join(html.unescape(text).split()).strip()


def _extract_sec_document_sections(raw: str) -> list[dict[str, str]]:
    sections: list[dict[str, str]] = []
    for match in re.finditer(r"(?is)<DOCUMENT>(.*?)</DOCUMENT>", raw):
        block = match.group(1)
        doc_type_match = re.search(r"(?im)^<TYPE>\s*([^\r\n<]+)", block)
        filename_match = re.search(r"(?im)^<FILENAME>\s*([^\r\n<]+)", block)
        description_match = re.search(r"(?im)^<DESCRIPTION>\s*([^\r\n<]+)", block)
        text_match = re.search(r"(?is)<TEXT>(.*)", block)
        sections.append(
            {
                "type": (doc_type_match.group(1).strip() if doc_type_match else ""),
                "filename": (filename_match.group(1).strip() if filename_match else ""),
                "description": (description_match.group(1).strip() if description_match else ""),
                "text": text_match.group(1) if text_match else block,
            }
        )
    return sections


def _select_analysis_raw(raw: str, form_type: str) -> str:
    if form_type != "6-K" or "<DOCUMENT>" not in raw:
        return raw

    sections = _extract_sec_document_sections(raw)
    if not sections:
        return raw

    def _norm(value: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", value.upper())

    preferred_press_release: list[str] = []
    fallback_exhibits: list[str] = []
    cover_docs: list[str] = []
    for section in sections:
        norm_type = _norm(section["type"])
        description_upper = section["description"].upper()
        filename_upper = section["filename"].upper()
        text = section["text"]
        if (
            norm_type in {"EX991", "991"}
            or "PRESS RELEASE" in description_upper
            or "99-1" in filename_upper
            or "99_1" in filename_upper
        ):
            preferred_press_release.append(text)
            continue
        if norm_type.startswith("EX99") and "PRESENTATION" not in description_upper:
            fallback_exhibits.append(text)
            continue
        if norm_type == "6K":
            cover_docs.append(text)

    if preferred_press_release:
        return "\n".join(preferred_press_release)
    if fallback_exhibits:
        return "\n".join(fallback_exhibits)
    if cover_docs:
        return "\n".join(cover_docs)
    return raw


def _strip_html_to_lines(raw: str) -> list[str]:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", raw)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = re.sub(r"(?i)</div\s*>", "\n", text)
    text = re.sub(r"(?i)</tr\s*>", "\n", text)
    text = re.sub(r"(?i)</li\s*>", "\n", text)
    text = re.sub(r"(?i)</h[1-6]\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = html.unescape(text)
    lines = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if line:
            lines.append(line)
    return lines


def _parse_number(value: str | None) -> float | None:
    if value is None:
        return None
    token = html.unescape(value).strip()
    if token in {"", "-", "--", "---", "—", "N/A", "n/a"}:
        return None
    negative = False
    if token.startswith("(") and token.endswith(")"):
        negative = True
        token = token[1:-1]
    token = token.replace(",", "")
    token = token.replace("US$", "")
    token = token.replace("USD", "")
    token = token.replace("$", "")
    token = token.strip()
    multiplier = 1.0
    if token.endswith(("K", "k")):
        multiplier = 1_000.0
        token = token[:-1]
    elif token.endswith(("M", "m")):
        multiplier = 1_000_000.0
        token = token[:-1]
    elif token.endswith(("B", "b")):
        multiplier = 1_000_000_000.0
        token = token[:-1]
    token = token.strip()
    if token == "":
        return None
    try:
        number = float(token) * multiplier
    except ValueError:
        return None
    return -number if negative else number


def _parse_attr_map(attr_text: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for name, _, value in re.findall(r"([a-zA-Z_:][a-zA-Z0-9_:\-]*)\s*=\s*(['\"])(.*?)\2", attr_text):
        attrs[name] = value
    return attrs


def _detect_form_type(path: Path, raw: str) -> str | None:
    upper_name = path.name.upper()
    for form in SUPPORTED_FORMS:
        if f"_{form}_" in upper_name or upper_name.endswith(f"{form}{path.suffix.upper()}"):
            return form
    for pattern in [
        r"(?is)<ix:nonNumeric[^>]*name=['\"]dei:DocumentType['\"][^>]*>(.*?)</ix:nonNumeric>",
        r"(?is)<dei:DocumentType[^>]*>(.*?)</dei:DocumentType>",
        r"(?i)CONFORMED SUBMISSION TYPE:\s*([0-9A-Z\-]+)",
        r"(?i)<TYPE>\s*([0-9A-Z\-]+)",
    ]:
        match = re.search(pattern, raw)
        if not match:
            continue
        value = _clean_text(match.group(1)).upper()
        if value in SUPPORTED_FORMS:
            return value
    return None


def _extract_ticker_from_path(path: Path) -> str | None:
    filename_match = re.match(
        r"(?i)^([A-Z0-9.\-]+)_(?:10-K|10-Q|6-K|20-F)_",
        path.name,
    )
    if filename_match:
        return filename_match.group(1).upper()
    parent_name = path.parent.name.upper()
    if re.fullmatch(r"[A-Z0-9.\-]{1,16}", parent_name):
        return parent_name
    return None


def _extract_non_numeric_fact(raw: str, tag_name: str) -> str | None:
    patterns = [
        rf"(?is)<ix:nonNumeric[^>]*name=['\"]{re.escape(tag_name)}['\"][^>]*>(.*?)</ix:nonNumeric>",
        rf"(?is)<{re.escape(tag_name)}[^>]*>(.*?)</{re.escape(tag_name)}>",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw)
        if match:
            value = _clean_text(match.group(1))
            if value:
                return value
    return None


def _extract_filing_date(path: Path, raw: str) -> str | None:
    header_match = re.search(r"(?i)FILED AS OF DATE:\s*(\d{8})", raw)
    if header_match:
        dt = _parse_date(header_match.group(1))
        return _format_date(dt)
    accept_match = re.search(r"(?i)ACCEPTANCE-DATETIME:\s*(\d{8})", raw)
    if accept_match:
        dt = _parse_date(accept_match.group(1))
        return _format_date(dt)
    filename_match = re.search(r"_(\d{4}-\d{2}-\d{2})", path.name)
    if filename_match:
        dt = _parse_date(filename_match.group(1))
        return _format_date(dt)
    return None


def _is_quarter_or_year_end_date(value: datetime | None) -> bool:
    if value is None:
        return False
    for year_offset in (-1, 0, 1):
        for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
            target = datetime(value.year + year_offset, month, day)
            if abs((value - target).days) <= 7:
                return True
    return False


def _is_month_end_like_date(value: datetime | None) -> bool:
    if value is None:
        return False
    next_day = value + timedelta(days=1)
    return next_day.month != value.month


def _extract_period_end(raw: str, form_type: str | None = None) -> str | None:
    preferred_date: datetime | None = None
    direct = _extract_non_numeric_fact(raw, "dei:DocumentPeriodEndDate")
    if direct:
        dt = _parse_date(direct)
        if dt:
            preferred_date = dt
        fiscal_year = _extract_non_numeric_fact(raw, "dei:DocumentFiscalYearFocus")
        if fiscal_year and re.fullmatch(r"\d{4}", fiscal_year):
            dt = _parse_date(f"{direct}, {fiscal_year}")
            if dt:
                preferred_date = dt
    header_match = re.search(r"(?i)CONFORMED PERIOD OF REPORT:\s*(\d{8})", raw)
    if header_match:
        dt = _parse_date(header_match.group(1))
        if dt:
            preferred_date = dt
    context_match = re.search(r"(?is)<xbrli:context\b[^>]*\bid=['\"]c-1['\"][^>]*>.*?<xbrli:endDate>\s*([^<]+)\s*</xbrli:endDate>", raw)
    if context_match:
        dt = _parse_date(_clean_text(context_match.group(1)))
        if dt:
            preferred_date = dt
    lines = _strip_html_to_lines(raw)
    candidates: list[datetime] = []
    for line in lines[:1500]:
        for match in re.finditer(
            r"(?i)(?:for the [a-z0-9,\-\s]{0,100}? ended|as of)\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
            line,
        ):
            dt = _parse_date(match.group(1))
            if dt is not None:
                candidates.append(dt)
    if candidates:
        if preferred_date is not None:
            if form_type == "6-K":
                quarter_like = [dt for dt in candidates if _is_quarter_or_year_end_date(dt)]
                if quarter_like and not (
                    _is_quarter_or_year_end_date(preferred_date) or _is_month_end_like_date(preferred_date)
                ):
                    return _format_date(max(quarter_like))
            if preferred_date in candidates:
                return _format_date(preferred_date)
            if form_type == "6-K":
                if quarter_like:
                    return _format_date(max(quarter_like))
            return _format_date(preferred_date)
        quarter_like = [dt for dt in candidates if _is_quarter_or_year_end_date(dt)]
        chosen = max(quarter_like or candidates)
        return _format_date(chosen)
    return _format_date(preferred_date)


def _is_periodic_financial_results_6k(lines: list[str], period_end_date: str | None) -> bool:
    period_dt = _parse_date(period_end_date)
    preview = "\n".join(lines[:4000])
    if not (_is_quarter_or_year_end_date(period_dt) or _is_month_end_like_date(period_dt)):
        preview_dates = [
            _parse_date(match.group(1))
            for match in re.finditer(
                r"(?i)(?:for the [a-z0-9,\-\s]{0,100}? ended|as of)\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
                preview,
            )
        ]
        period_dt = next(
            (dt for dt in preview_dates if _is_quarter_or_year_end_date(dt) or _is_month_end_like_date(dt)),
            None,
        )
    if not (_is_quarter_or_year_end_date(period_dt) or _is_month_end_like_date(period_dt)):
        return False

    statement_patterns = [
        r"(?i)statements?\s+of\s+operations",
        r"(?i)statements?\s+of\s+income",
        r"(?i)statements?\s+of\s+financial position",
        r"(?i)balance sheets?",
        r"(?i)statements?\s+of\s+cash flows",
    ]
    if any(re.search(pattern, preview) for pattern in statement_patterns):
        return True

    periodic_patterns = [
        r"(?i)financial results for the (?:three|six|nine|twelve)\s+months?\s+ended",
        r"(?i)results for the (?:first|second|third|fourth)\s+quarter ended",
        r"(?i)\b(?:quarterly|interim|annual)\s+results\b",
        r"(?i)\bearnings release\b",
        r"(?i)\bthree months ended\b",
        r"(?i)\bnine months ended\b",
        r"(?i)\byear ended\b",
    ]
    if not any(re.search(pattern, preview) for pattern in periodic_patterns):
        return False

    financial_keyword_hits = sum(
        1
        for pattern in (
            r"(?i)\btotal revenues?\b",
            r"(?i)\bnet income\b",
            r"(?i)\bincome before income tax",
            r"(?i)\boperating expenses?\b",
            r"(?i)\btotal assets\b",
            r"(?i)\btotal liabilities\b",
        )
        if re.search(pattern, preview)
    )
    return financial_keyword_hits >= 2


def _infer_scale_from_inline_facts(raw: str) -> str | None:
    counts: dict[int, int] = {}
    for attr_text in re.findall(r"(?is)<ix:nonFraction\b([^>]*)>", raw):
        attrs = _parse_attr_map(attr_text)
        unit_ref = (attrs.get("unitRef") or "").lower()
        if (
            "usd" not in unit_ref
            and "cny" not in unit_ref
            and "rmb" not in unit_ref
            and "hkd" not in unit_ref
            and "eur" not in unit_ref
            and "gbp" not in unit_ref
            and "jpy" not in unit_ref
            and "krw" not in unit_ref
            and "twd" not in unit_ref
        ):
            continue
        try:
            scale = int(attrs.get("scale", "0"))
        except ValueError:
            continue
        counts[scale] = counts.get(scale, 0) + 1
    if not counts:
        return None
    scale = max(counts.items(), key=lambda item: item[1])[0]
    return {3: "thousands", 6: "millions", 9: "billions"}.get(scale)


def _infer_currency_from_statement_columns(lines: list[str]) -> str | None:
    code_map = {
        "CAD": {"cad", "c$"},
        "BRL": {"brl", "r$", "reais"},
        "CNY": {"rmb", "cny", "renminbi"},
        "HKD": {"hkd", "hk$"},
        "EUR": {"eur"},
        "GBP": {"gbp"},
        "JPY": {"jpy"},
        "KRW": {"krw"},
        "TWD": {"twd", "nt$"},
        "USD": {"usd", "us$"},
    }
    statement_starts = [
        idx
        for idx, line in enumerate(lines[:1500])
        if re.search(
            r"(?i)(balance sheets?|statements?\s+of|statement\s+s?\s+of|cash flows?|income|financial position)",
            line,
        )
    ]
    counts: dict[str, int] = {}
    for start in statement_starts[:12]:
        for line in lines[start : min(start + 28, len(lines))]:
            norm = _normalize_label(line)
            for code, tokens in code_map.items():
                if norm in tokens:
                    counts[code] = counts.get(code, 0) + 1
    if not counts:
        return None
    return max(counts.items(), key=lambda item: (item[1], item[0] != "USD", item[0]))[0]


def _infer_scale_from_statement_context(lines: list[str]) -> str | None:
    statement_heading_pattern = re.compile(
        r"(?i)(balance sheets?|statements?\s+of|statement\s+s?\s+of|financial position|cash flows?)"
    )
    monetary_context_pattern = re.compile(
        r"(?i)(amounts?|u\.?s\.?\s*dollars?|usd|us\$|rmb|cny|renminbi|hkd|hk\$|brl|r\$|cad|c\$|eur|gbp|jpy|yen|krw|korean won|twd|nt\$|new taiwan dollars?)"
    )
    statement_starts = [
        idx
        for idx, line in enumerate(lines[:1500])
        if statement_heading_pattern.search(line)
    ]
    windows: list[str] = []
    for start in statement_starts[:40]:
        for idx in range(start, min(start + 12, len(lines))):
            windows.append(lines[idx])
            if idx + 1 < len(lines):
                windows.append(f"{lines[idx]} {lines[idx + 1]}")

    direct_statement_scale = re.compile(r"(?i)(?:^|[\(])\s*in\s+(thousands|millions|billions)\b")
    for window in windows:
        match = direct_statement_scale.search(window)
        if match:
            return match.group(1).lower()

    if not windows:
        lead = lines[:240]
        for idx in range(len(lead)):
            window = lead[idx]
            if idx + 1 < len(lead):
                window = f"{window} {lead[idx + 1]}"
            windows.append(window)

    for window in windows:
        match = direct_statement_scale.search(window)
        if match and (
            statement_heading_pattern.search(window) or monetary_context_pattern.search(window)
        ):
            return match.group(1).lower()

    currency_or_amount_pattern = re.compile(
        r"(?i)(amounts?|u\.?s\.?\s*dollars?|usd|us\$|rmb|cny|renminbi|hkd|hk\$|brl|r\$|brazilian reais?|cad|c\$|eur|gbp|jpy|yen|krw|korean won|twd|nt\$|new taiwan dollars?)"
    )
    abbreviated_scale_patterns = (
        ("billions", re.compile(r"(?i)\b(?:usd|us\$|rmb|cny|renminbi|hkd|hk\$|brl|r\$|cad|c\$|eur|gbp|jpy|yen|krw|twd|nt\$)\s*['’]?\s*0{9}\b")),
        ("millions", re.compile(r"(?i)\b(?:usd|us\$|rmb|cny|renminbi|hkd|hk\$|brl|r\$|cad|c\$|eur|gbp|jpy|yen|krw|twd|nt\$)\s*['’]?\s*0{6}\b")),
        ("thousands", re.compile(r"(?i)\b(?:usd|us\$|rmb|cny|renminbi|hkd|hk\$|brl|r\$|cad|c\$|eur|gbp|jpy|yen|krw|twd|nt\$)\s*['’]?\s*0{3}\b")),
    )
    for scale_word, pattern in abbreviated_scale_patterns:
        for window in windows:
            if pattern.search(window):
                return scale_word
    for scale_word in ("thousands", "millions", "billions"):
        scale_pattern = re.compile(rf"(?i)\bin {scale_word}\b")
        for window in windows:
            if not scale_pattern.search(window):
                continue
            if not currency_or_amount_pattern.search(window):
                continue
            return scale_word
    return None


def _has_quarter_full_year_gaap_highlight_table(lines: list[str]) -> bool:
    preview = " ".join(lines[:140])
    return bool(
        re.search(r"(?i)\b4q\s+20\d{2}\s+gaap\b", preview)
        and re.search(r"(?i)\bfull year\s+20\d{2}\s+gaap\b", preview)
    )


def _prefer_quarter_current_mixed_6k(lines: list[str]) -> bool:
    preview = " ".join(line.strip() for line in lines[:260] if line.strip())
    normalized = _normalize_label(preview)
    return bool(
        re.search(r"\bfinancial results? for (?:the )?quarter ended\b", normalized)
        or re.search(r"\bfinancial results? for (?:the )?three months ended\b", normalized)
    )


def _is_mixed_quarter_year_statement_header(header: list[str]) -> bool:
    normalized = _normalize_label(" ".join(header[:24]))
    return (
        ("three months ended" in normalized or "quarter ended" in normalized)
        and "year ended" in normalized
    )


def _infer_currency_unit_from_statement_headers(lines: list[str]) -> str | None:
    statement_heading_pattern = re.compile(
        r"(?i)(consolidated|condensed|parent company|combined|statement\s+s?)?.{0,60}"
        r"(balance sheets?|statements?\s+of\s+(?:operations|income|cash flows|financial position|profit or loss(?:\s+and\s+other\s+comprehensive\s+income)?|comprehensive income|comprehensive loss)|(?:operations|income|cash flows|profit or loss(?:\s+and\s+other\s+comprehensive\s+income)?|comprehensive income|comprehensive loss)\s+statements?)"
    )
    windows: list[str] = []
    for idx, line in enumerate(lines):
        if not statement_heading_pattern.search(_statement_search_text(line)):
            continue
        for end in range(idx, min(idx + 12, len(lines))):
            windows.append(" ".join(lines[idx : end + 1]))
    if not windows:
        return None

    patterns = [
        ("USD", "thousands", r"(?i)\bin\s+(?:u\.?s\.?\s*)?dollars?\s+thousands\b"),
        ("USD", "millions", r"(?i)\bin\s+(?:u\.?s\.?\s*)?dollars?\s+millions\b"),
        ("USD", "billions", r"(?i)\bin\s+(?:u\.?s\.?\s*)?dollars?\s+billions\b"),
        ("USD", "thousands", r"(?i)\b(?:u\.?s\.?\s*)?dollars?\s+in\s+thousands\b"),
        ("USD", "millions", r"(?i)\b(?:u\.?s\.?\s*)?dollars?\s+in\s+millions\b"),
        ("USD", "billions", r"(?i)\b(?:u\.?s\.?\s*)?dollars?\s+in\s+billions\b"),
        ("USD", "thousands", r"(?i)\bin\s+thousands\s+of\s+(?:u\.?s\.?\s*)?dollars?\b"),
        ("USD", "millions", r"(?i)\bin\s+millions\s+of\s+(?:u\.?s\.?\s*)?dollars?\b"),
        ("USD", "billions", r"(?i)\bin\s+billions\s+of\s+(?:u\.?s\.?\s*)?dollars?\b"),
        ("USD", "thousands", r"(?i)amounts?\s+expressed\s+in\s+thousands\s+of\s+(?:u\.?s\.?\s*)?dollars?\b"),
        ("USD", "millions", r"(?i)amounts?\s+expressed\s+in\s+millions\s+of\s+(?:u\.?s\.?\s*)?dollars?\b"),
        ("USD", "billions", r"(?i)amounts?\s+expressed\s+in\s+billions\s+of\s+(?:u\.?s\.?\s*)?dollars?\b"),
        ("USD", "thousands", r"(?i)amounts?\s+expressed\s+in\s+thousands\s+of\s+(?:usd|us\$)\b"),
        ("USD", "millions", r"(?i)amounts?\s+expressed\s+in\s+millions\s+of\s+(?:usd|us\$)\b"),
        ("USD", "billions", r"(?i)amounts?\s+expressed\s+in\s+billions\s+of\s+(?:usd|us\$)\b"),
        ("CNY", "thousands", r"(?i)\bin\s+(?:rmb|renminbi|cny)\s+thousands\b"),
        ("CNY", "millions", r"(?i)\bin\s+(?:rmb|renminbi|cny)\s+millions\b"),
        ("CNY", "billions", r"(?i)\bin\s+(?:rmb|renminbi|cny)\s+billions\b"),
        ("CNY", "thousands", r"(?i)\bin\s+thousands\s+of\s+(?:rmb|renminbi|cny)\b"),
        ("CNY", "millions", r"(?i)\bin\s+millions\s+of\s+(?:rmb|renminbi|cny)\b"),
        ("CNY", "billions", r"(?i)\bin\s+billions\s+of\s+(?:rmb|renminbi|cny)\b"),
        ("BRL", "thousands", r"(?i)\bin\s+(?:brl|brazilian real(?:s)?|reais|r\$)\s+thousands\b"),
        ("BRL", "millions", r"(?i)\bin\s+(?:brl|brazilian real(?:s)?|reais|r\$)\s+millions\b"),
        ("BRL", "thousands", r"(?i)\bin\s+thousands\s+of\s+(?:brl|brazilian real(?:s)?|reais|r\$)\b"),
        ("KRW", "thousands", r"(?i)\bin\s+(?:krw|korean won)\s+thousands\b"),
        ("KRW", "millions", r"(?i)\bin\s+(?:krw|korean won)\s+millions\b"),
        ("TWD", "thousands", r"(?i)\bin\s+(?:twd|nt\$|new taiwan dollars?)\s+thousands\b"),
        ("CAD", "thousands", r"(?i)\bin\s+(?:cad|canadian dollars?|c\$)\s+thousands\b"),
        ("HKD", "thousands", r"(?i)\bin\s+(?:hkd|hong kong dollars?|hk\$)\s+thousands\b"),
        ("EUR", "thousands", r"(?i)\bin\s+(?:eur|euros?|€)\s+thousands\b"),
        ("EUR", "millions", r"(?i)\bin\s+(?:eur|euros?|€)\s+millions\b"),
        ("EUR", "billions", r"(?i)\bin\s+(?:eur|euros?|€)\s+billions\b"),
        ("GBP", "thousands", r"(?i)\bin\s+(?:gbp|pounds? sterling)\s+thousands\b"),
        ("JPY", "thousands", r"(?i)\bin\s+(?:jpy|yen)\s+thousands\b"),
    ]
    for currency, scale, pattern in patterns:
        if any(re.search(pattern, window) for window in windows):
            return f"{currency}, {scale}"

    scale_only_patterns = {
        "thousands": r"(?i)\(\s*in\s+thousands\s*\)|\bin\s+thousands\b",
        "millions": r"(?i)\(\s*in\s+millions\s*\)|\bin\s+millions\b",
        "billions": r"(?i)\(\s*in\s+billions\s*\)|\bin\s+billions\b",
    }
    currency_markers = [
        ("CNY", r"(?i)\b(?:rmb|renminbi|cny)\b"),
        ("HKD", r"(?i)\b(?:hkd|hk\$|hong kong dollars?)\b"),
        ("USD", r"(?i)\b(?:usd|us\$|u\.?s\.?\s*dollars?)\b"),
        ("BRL", r"(?i)\b(?:brl|r\$|brazilian real(?:s)?|reais)\b"),
        ("KRW", r"(?i)\b(?:krw|korean won)\b"),
        ("TWD", r"(?i)\b(?:twd|nt\$|new taiwan dollars?)\b"),
        ("CAD", r"(?i)\b(?:cad|c\$|canadian dollars?)\b"),
        ("EUR", r"(?i)(?:\b(?:eur|euros?)\b|€)"),
        ("GBP", r"(?i)\b(?:gbp|pounds? sterling)\b"),
        ("JPY", r"(?i)\b(?:jpy|yen)\b"),
        ("INR", r"(?i)\b(?:inr|indian rupees?|₹)\b"),
    ]
    for window in windows:
        detected_scale = next(
            (scale for scale, pattern in scale_only_patterns.items() if re.search(pattern, window)),
            None,
        )
        if detected_scale is None:
            continue
        matched_currency: tuple[int, str] | None = None
        for currency, pattern in currency_markers:
            match = re.search(pattern, window)
            if match is None:
                continue
            candidate = (match.start(), currency)
            if matched_currency is None or candidate < matched_currency:
                matched_currency = candidate
        if matched_currency is not None:
            return f"{matched_currency[1]}, {detected_scale}"
    return None


def _extract_currency_unit(raw: str) -> str | None:
    visible_raw = re.sub(r"(?is)<ix:header\b.*?</ix:header>", " ", raw)
    lines = _strip_html_to_lines(visible_raw)
    statement_currency_unit = _infer_currency_unit_from_statement_headers(lines)
    if statement_currency_unit is not None:
        return statement_currency_unit
    head_text = "\n".join(lines[:1200])
    lead_text = "\n".join(lines[:240])
    head_windows: list[str] = []
    capped = lines[:1200]
    for idx in range(len(capped)):
        head_windows.append(capped[idx])
        if idx + 1 < len(capped):
            head_windows.append(f"{capped[idx]} {capped[idx + 1]}")
        if idx + 2 < len(capped):
            head_windows.append(f"{capped[idx]} {capped[idx + 1]} {capped[idx + 2]}")
    explicit_patterns = [
        ("CAD", "thousands", r"(?i)amounts?\s+in\s+thousands\s+of\s+[^()\n]{0,80}?(canadian dollars|cad|c\$)"),
        ("CAD", "millions", r"(?i)amounts?\s+in\s+millions\s+of\s+[^()\n]{0,80}?(canadian dollars|cad|c\$)"),
        ("CAD", "billions", r"(?i)amounts?\s+in\s+billions\s+of\s+[^()\n]{0,80}?(canadian dollars|cad|c\$)"),
        ("BRL", "thousands", r"(?i)amounts?\s+in\s+thousands\s+of\s+[^()\n]{0,80}?(brazilian reais|brazilian real|reais|brl|r\$)"),
        ("BRL", "millions", r"(?i)amounts?\s+in\s+millions\s+of\s+[^()\n]{0,80}?(brazilian reais|brazilian real|reais|brl|r\$)"),
        ("BRL", "billions", r"(?i)amounts?\s+in\s+billions\s+of\s+[^()\n]{0,80}?(brazilian reais|brazilian real|reais|brl|r\$)"),
        ("CNY", "thousands", r"(?i)amounts?\s+in\s+thousands\s+of\s+[^()\n]{0,80}?(renminbi|rmb|cny)"),
        ("CNY", "millions", r"(?i)amounts?\s+in\s+millions\s+of\s+[^()\n]{0,80}?(renminbi|rmb|cny)"),
        ("CNY", "billions", r"(?i)amounts?\s+in\s+billions\s+of\s+[^()\n]{0,80}?(renminbi|rmb|cny)"),
        ("HKD", "thousands", r"(?i)amounts?\s+in\s+thousands\s+of\s+[^()\n]{0,80}?(hong kong dollars|hkd|hk\$)"),
        ("HKD", "millions", r"(?i)amounts?\s+in\s+millions\s+of\s+[^()\n]{0,80}?(hong kong dollars|hkd|hk\$)"),
        ("EUR", "thousands", r"(?i)amounts?\s+in\s+thousands\s+of\s+[^()\n]{0,80}?(euros?|eur|€)"),
        ("EUR", "millions", r"(?i)amounts?\s+in\s+millions\s+of\s+[^()\n]{0,80}?(euros?|eur|€)"),
        ("GBP", "thousands", r"(?i)amounts?\s+in\s+thousands\s+of\s+[^()\n]{0,80}?(pounds? sterling|gbp)"),
        ("GBP", "millions", r"(?i)amounts?\s+in\s+millions\s+of\s+[^()\n]{0,80}?(pounds? sterling|gbp)"),
        ("JPY", "thousands", r"(?i)amounts?\s+in\s+thousands\s+of\s+[^()\n]{0,80}?(yen|jpy)"),
        ("JPY", "millions", r"(?i)amounts?\s+in\s+millions\s+of\s+[^()\n]{0,80}?(yen|jpy)"),
        ("KRW", "thousands", r"(?i)amounts?\s+in\s+thousands\s+of\s+[^()\n]{0,80}?(korean won|krw)"),
        ("KRW", "millions", r"(?i)amounts?\s+in\s+millions\s+of\s+[^()\n]{0,80}?(korean won|krw)"),
        ("KRW", "billions", r"(?i)amounts?\s+in\s+billions\s+of\s+[^()\n]{0,80}?(korean won|krw)"),
        ("TWD", "thousands", r"(?i)amounts?\s+in\s+thousands\s+of\s+[^()\n]{0,80}?(new taiwan dollars?|nt\$|twd)"),
        ("TWD", "millions", r"(?i)amounts?\s+in\s+millions\s+of\s+[^()\n]{0,80}?(new taiwan dollars?|nt\$|twd)"),
        ("TWD", "billions", r"(?i)amounts?\s+in\s+billions\s+of\s+[^()\n]{0,80}?(new taiwan dollars?|nt\$|twd)"),
        ("USD", "thousands", r"(?i)amounts?\s+in\s+thousands\s+of\s+[^()\n]{0,80}?(u\.?s\.?\s*dollars?|usd|us\$)"),
        ("USD", "millions", r"(?i)amounts?\s+in\s+millions\s+of\s+[^()\n]{0,80}?(u\.?s\.?\s*dollars?|usd|us\$)"),
        ("USD", "billions", r"(?i)amounts?\s+in\s+billions\s+of\s+[^()\n]{0,80}?(u\.?s\.?\s*dollars?|usd|us\$)"),
    ]
    for currency, scale, pattern in explicit_patterns:
        if any(re.search(pattern, window, flags=re.IGNORECASE) for window in head_windows):
            return f"{currency}, {scale}"

    reversed_order_patterns = {
        "CAD": r"(?:canadian dollars?|cad|c\$)",
        "BRL": r"(?:brazilian reais?|brazilian real|reais|brl|r\$)",
        "CNY": r"(?:renminbi|rmb|cny)",
        "HKD": r"(?:hong kong dollars|hkd|hk\$)",
        "EUR": r"(?:euros?|eur|€)",
        "GBP": r"(?:pounds? sterling|gbp)",
        "JPY": r"(?:yen|jpy)",
        "KRW": r"(?:korean won|krw)",
        "TWD": r"(?:new taiwan dollars?|nt\$|twd)",
        "USD": r"(?:u\.?s\.?\s*dollars?|usd|us\$)",
    }
    for currency, token_pattern in reversed_order_patterns.items():
        for scale in ("thousands", "millions", "billions"):
            pattern = rf"(?i)\bin\s+{token_pattern}\s+{scale}\b"
            if any(re.search(pattern, window, flags=re.IGNORECASE) for window in head_windows):
                return f"{currency}, {scale}"

    statement_currency = _infer_currency_from_statement_columns(lines)

    xbrl_measure_currencies = [
        code.upper()
        for code in re.findall(r"(?i)<xbrli:measure>\s*iso4217:([A-Z]{3})\s*</xbrli:measure>", raw)
    ]
    primary_xbrl_currency = None
    if xbrl_measure_currencies:
        counts: dict[str, int] = {}
        for code in xbrl_measure_currencies:
            counts[code] = counts.get(code, 0) + 1
        primary_xbrl_currency = max(counts.items(), key=lambda item: (item[1], item[0]))[0]

    currency = None
    for code, patterns in {
        "CAD": [r"\bCAD(?=\b|[0-9])", r"\bCanadian dollars?\b", r"\bC\$(?=\b|[0-9])"],
        "BRL": [r"\bBRL(?=\b|[0-9])", r"\bBrazilian reais?\b", r"\bBrazilian real\b", r"\bR\$(?=\b|[0-9])"],
        "CNY": [r"\bRMB(?=\b|[0-9])", r"\bCNY(?=\b|[0-9])", r"\bRenminbi\b"],
        "HKD": [r"\bHKD(?=\b|[0-9])", r"\bHong Kong dollars\b", r"\bHK\$(?=\b|[0-9])"],
        "EUR": [r"\bEUR(?=\b|[0-9])", r"\bEuros?\b", r"€"],
        "GBP": [r"\bGBP(?=\b|[0-9])", r"\bPounds? sterling\b"],
        "JPY": [r"\bJPY(?=\b|[0-9])", r"\bYen\b"],
        "KRW": [r"\bKRW(?=\b|[0-9])", r"\bKorean won\b"],
        "TWD": [r"\bTWD(?=\b|[0-9])", r"\bNT\$(?=\b|[0-9])", r"\bNew Taiwan dollars?\b"],
        "USD": [r"\bUSD(?=\b|[0-9])", r"\bU\.S\. dollars\b", r"\bUS\$(?=\b|[0-9])"],
    }.items():
        if any(re.search(pattern, lead_text, flags=re.IGNORECASE) for pattern in patterns):
            currency = code
            break
    if statement_currency is not None and (currency is None or statement_currency != "USD"):
        currency = statement_currency
    if primary_xbrl_currency is not None and statement_currency is None:
        if currency is None or currency != primary_xbrl_currency:
            currency = primary_xbrl_currency

    scale = _infer_scale_from_statement_context(lines)
    if currency == "USD" and scale == "thousands" and _has_quarter_full_year_gaap_highlight_table(lines):
        scale = "millions"
    if scale is None:
        scale = _infer_scale_from_inline_facts(raw)

    if currency is None and "$" in lead_text and "C$" not in lead_text and "HK$" not in lead_text and "R$" not in lead_text:
        currency = "USD"
    if currency is None and scale and "$" in head_text and "C$" not in head_text and "HK$" not in head_text:
        currency = "USD"

    if currency and scale:
        return f"{currency}, {scale}"
    return currency


def _parse_contexts(raw: str) -> dict[str, ContextInfo]:
    contexts: dict[str, ContextInfo] = {}
    for context_id, body in re.findall(
        r"(?is)<xbrli:context\b[^>]*\bid=['\"]([^'\"]+)['\"][^>]*>(.*?)</xbrli:context>",
        raw,
    ):
        start_match = re.search(r"(?is)<(?:xbrli:)?startDate>\s*([^<]+)\s*</(?:xbrli:)?startDate>", body)
        end_match = re.search(r"(?is)<(?:xbrli:)?endDate>\s*([^<]+)\s*</(?:xbrli:)?endDate>", body)
        instant_match = re.search(r"(?is)<(?:xbrli:)?instant>\s*([^<]+)\s*</(?:xbrli:)?instant>", body)
        members = [
            _clean_text(x)
            for x in re.findall(
                r"(?is)<xbrldi:explicitMember\b[^>]*>(.*?)</xbrldi:explicitMember>",
                body,
            )
        ]
        segment_key = "|".join(x for x in members if x) or None
        contexts[context_id] = ContextInfo(
            context_id=context_id,
            start_date=_parse_date(_clean_text(start_match.group(1))) if start_match else None,
            end_date=_parse_date(_clean_text(end_match.group(1))) if end_match else None,
            instant_date=_parse_date(_clean_text(instant_match.group(1))) if instant_match else None,
            has_segment=bool(segment_key),
            segment_key=segment_key,
        )
    return contexts


def _extract_inline_facts(raw: str) -> list[Fact]:
    facts: list[Fact] = []
    for attr_text, body in re.findall(
        r"(?is)<ix:(?:nonFraction|fraction)\b([^>]*)>(.*?)</ix:(?:nonFraction|fraction)>",
        raw,
    ):
        attrs = _parse_attr_map(attr_text)
        name = attrs.get("name")
        if not name:
            continue
        value = _parse_number(_clean_text(body))
        if value is None:
            continue
        try:
            scale = int(attrs.get("scale", "0"))
        except ValueError:
            scale = 0
        if scale:
            value *= 10 ** scale
        if attrs.get("sign") == "-":
            value *= -1
        facts.append(
            Fact(
                name=name,
                value=value,
                context_ref=attrs.get("contextRef"),
                unit_ref=attrs.get("unitRef"),
            )
        )
    return facts


def _extract_tag_facts(raw: str, tag_names: set[str]) -> list[Fact]:
    facts: list[Fact] = []
    for tag_name in tag_names:
        pattern = re.compile(
            rf"(?is)<{re.escape(tag_name)}\b([^>]*)>(.*?)</{re.escape(tag_name)}>",
        )
        for attr_text, body in pattern.findall(raw):
            attrs = _parse_attr_map(attr_text)
            value = _parse_number(_clean_text(body))
            if value is None:
                continue
            facts.append(
                Fact(
                    name=tag_name,
                    value=value,
                    context_ref=attrs.get("contextRef"),
                    unit_ref=attrs.get("unitRef"),
                )
            )
    return facts


def _preferred_duration_days(form_type: str) -> int:
    if form_type in {"10-Q", "6-K"}:
        return 90
    return 365


def _score_fact(
    fact: Fact,
    metric: str,
    contexts: dict[str, ContextInfo],
    target_end: datetime | None,
    form_type: str,
    tag_rank: int,
) -> tuple[int, int, int, int, int]:
    context = contexts.get(fact.context_ref or "")
    report_date = context.report_date if context else None

    if target_end and report_date:
        date_penalty = abs((report_date - target_end).days)
    elif report_date:
        date_penalty = 0
    else:
        date_penalty = 9999

    if metric in DURATION_METRICS:
        kind_penalty = 0 if context and context.start_date and context.end_date else 200
        duration = context.duration_days if context else None
        duration_penalty = (
            abs(duration - _preferred_duration_days(form_type)) if duration is not None else 400
        )
    else:
        kind_penalty = 0 if context and context.instant_date else 200
        duration_penalty = 0

    segment_penalty = 0
    if context and context.has_segment:
        segment_penalty = 25
        if context.segment_key and "|" not in context.segment_key:
            segment_penalty = 10
    freshness_penalty = -int(report_date.toordinal()) if report_date and target_end is None else 0
    return (date_penalty, kind_penalty, duration_penalty, segment_penalty + tag_rank, freshness_penalty)


def _select_metric(
    metric: str,
    facts_by_name: dict[str, list[Fact]],
    contexts: dict[str, ContextInfo],
    target_end: datetime | None,
    form_type: str,
) -> float | None:
    tags = METRIC_TAGS[metric]
    best_value = None
    best_score = None
    for tag_rank, tag in enumerate(tags):
        for fact in facts_by_name.get(tag, []):
            context = contexts.get(fact.context_ref or "")
            report_date = context.report_date if context else None
            if target_end and report_date and abs((report_date - target_end).days) > MAX_FACT_DATE_GAP_DAYS:
                continue
            score = _score_fact(
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


def _select_shares_outstanding(
    facts_by_name: dict[str, list[Fact]],
    contexts: dict[str, ContextInfo],
    target_end: datetime | None,
) -> float | None:
    for tag in METRIC_TAGS["shares_outstanding"]:
        grouped: dict[datetime, dict[str, Any]] = {}
        for fact in facts_by_name.get(tag, []):
            context = contexts.get(fact.context_ref or "")
            if context is None or context.report_date is None:
                continue
            bucket = grouped.setdefault(
                context.report_date,
                {"non_segmented": [], "segmented": {}},
            )
            if context.has_segment and context.segment_key:
                bucket["segmented"][context.segment_key] = fact.value
            else:
                bucket["non_segmented"].append(fact.value)

        if not grouped:
            continue

        if target_end:
            chosen_date = min(grouped.keys(), key=lambda dt: abs((dt - target_end).days))
        else:
            chosen_date = max(grouped.keys())

        bucket = grouped[chosen_date]
        if bucket["non_segmented"]:
            return max(bucket["non_segmented"])
        segmented = bucket["segmented"]
        if segmented:
            return float(sum(segmented.values()))
    return None


def _extract_text_metric(metric: str, lines: list[str], multiplier: float) -> float | None:
    labels = TEXT_LABELS.get(metric, [])
    if not labels:
        return None
    number_pattern = re.compile(r"\(?-?\$?\d[\d,]*(?:\.\d+)?[KMBkmb]?\)?")
    candidates: list[float] = []
    for index, line in enumerate(lines):
        line_lower = line.lower()
        if not any(label in line_lower for label in labels):
            continue
        if metric == "operating_income" and not any(line_lower.startswith(label) for label in labels):
            continue
        if metric == "research_and_development" and ("tax benefit" in line_lower or "tax benefits" in line_lower):
            continue
        if metric == "shares_outstanding" and "weighted average" in line_lower:
            continue
        if metric == "total_liabilities" and ("and shareholders" in line_lower or "and stockholders" in line_lower or "and equity" in line_lower):
            continue

        search_window = [line]
        search_window.extend(lines[index + 1 : index + 3])
        found_token = None
        found_row = None
        found_value = None
        for offset, row in enumerate(search_window):
            values = []
            for token in number_pattern.findall(row):
                parsed = _parse_number(token)
                if parsed is not None:
                    values.append((token, parsed))
            if values:
                row_lower = row.lower()
                filtered_values = []
                for token, parsed in values:
                    compact = token.replace(",", "").replace("$", "").replace("(", "").replace(")", "")
                    if re.fullmatch(r"-?\d{4}", compact):
                        try:
                            year_like = abs(int(compact))
                        except ValueError:
                            year_like = 0
                        if 1900 <= year_like <= 2100:
                            continue
                    filtered_values.append((token, parsed))
                if not filtered_values:
                    continue
                if len(filtered_values) > 1 and any(row_lower.startswith(label) for label in labels):
                    found_token, found_value = filtered_values[0]
                else:
                    found_token, found_value = filtered_values[-1]
                found_row = row
                break
        if found_value is None:
            continue

        value = found_value
        compact = (found_token or "").replace(",", "").replace("$", "").replace("(", "").replace(")", "")
        if re.fullmatch(r"-?\d{4}", compact):
            try:
                year_like = abs(int(compact))
            except ValueError:
                year_like = 0
            if 1900 <= year_like <= 2100:
                continue
        if metric not in {
            "eps_basic",
            "eps_diluted",
            "shares_outstanding",
            "weighted_avg_shares_basic",
            "weighted_avg_shares_diluted",
        } and not re.search(r"[KMBkmb]\)?$", found_token or ""):
            row_lower = (found_row or "").lower()
            if "billion" in row_lower:
                value *= 1_000_000_000.0
            elif "million" in row_lower:
                value *= 1_000_000.0
            elif "thousand" in row_lower:
                value *= 1_000.0
            else:
                value *= multiplier
        candidates.append(value)
    if not candidates:
        return None
    if metric in {"total_assets", "assets_current", "total_liabilities", "liabilities_current"}:
        return max(candidates, key=abs)
    return candidates[-1]


def _unit_multiplier(currency_unit: str | None) -> float:
    if not currency_unit:
        return 1.0
    upper = currency_unit.upper()
    if "CRORE" in upper:
        return 10_000_000.0
    if "THOUSAND" in upper:
        return 1_000.0
    if "MILLION" in upper:
        return 1_000_000.0
    if "BILLION" in upper:
        return 1_000_000_000.0
    if "TRILLION" in upper:
        return 1_000_000_000_000.0
    return 1.0


def _unit_word_multiplier(unit_word: str | None) -> float:
    if unit_word is None:
        return 1.0
    word = unit_word.lower()
    if word == "thousand":
        return 1_000.0
    if word == "million":
        return 1_000_000.0
    if word == "billion":
        return 1_000_000_000.0
    if word == "trillion":
        return 1_000_000_000_000.0
    if word == "crore":
        return 10_000_000.0
    return 1.0


def _share_unit_context_multiplier(text: str | None) -> float:
    if not text:
        return 1.0
    lowered = text.lower()
    if "thousand" in lowered or "000s" in lowered or "000's" in lowered:
        return 1_000.0
    if re.search(
        r"(?i)(?:shares?|ads?|adrs?|ordinary shares?|common shares?)\s*(?:\(|-|–|—)?\s*billions?\b",
        text,
    ) or re.search(r"(?i)\bbillions?\s+(?:of\s+)?(?:shares?|ads?|adrs?|ordinary shares?|common shares?)\b", text):
        return 1_000_000_000.0
    if re.search(
        r"(?i)(?:shares?|ads?|adrs?|ordinary shares?|common shares?)\s*(?:\(|-|–|—)?\s*millions?\b",
        text,
    ) or re.search(r"(?i)\bmillions?\s+(?:of\s+)?(?:shares?|ads?|adrs?|ordinary shares?|common shares?)\b", text):
        return 1_000_000.0
    return 1.0


def _parse_ratio_token(token: str | None) -> float | None:
    if token is None:
        return None
    numeric = _parse_number(token)
    if numeric is not None:
        return numeric
    cleaned = _normalize_label(token).replace(",", " ").replace(" and ", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return None
    numeric = _parse_number(cleaned)
    if numeric is not None:
        return numeric

    total = 0.0
    current = 0.0
    saw_word = False
    for part in cleaned.replace("-", " ").split():
        if part in NUMBER_WORDS:
            current += NUMBER_WORDS[part]
            saw_word = True
            continue
        if part == "hundred":
            current = max(1.0, current) * 100.0
            saw_word = True
            continue
        return None
    if saw_word:
        total += current
        return total
    return None


def _parse_quantity_with_unit(number_text: str | None, unit_word: str | None = None) -> float | None:
    base = _parse_ratio_token(number_text)
    if base is None:
        return None
    return base * _unit_word_multiplier(unit_word)


def _references_depositary_shares(text: str | None) -> bool:
    if not text:
        return False
    normalized = _normalize_label(text)
    if any(
        phrase in normalized
        for phrase in (
            "american depositary share",
            "depositary share",
            "depository share",
        )
    ):
        return True
    return bool(re.search(r"\b(?:ads|adss|adr|adrs)\b", normalized))


def _uses_depositary_primary_unit(text: str | None) -> bool:
    if not text:
        return False
    normalized = _normalize_label(text)
    depositary_match = re.search(
        r"american depositary share|depositary share|depository share|\b(?:ads|adss|adr|adrs)\b",
        normalized,
    )
    ordinary_match = re.search(r"ordinary share|common share", normalized)
    if depositary_match is None:
        return False
    if ordinary_match is None:
        return True
    return depositary_match.start() < ordinary_match.start()


def _canonicalize_depositary_ratio(value: float | None) -> float | None:
    if value is None or value <= 0:
        return None
    approximation = float(Fraction(value).limit_denominator(20))
    if approximation > 0 and abs(value - approximation) / approximation <= 0.03:
        return approximation
    if 0.05 <= value <= 50:
        return round(value, 6)
    return None


def _normalize_depositary_metric(
    metric: str,
    label: str | None,
    value: float | None,
    ordinary_shares_per_depositary_share: float | None,
) -> float | None:
    if value is None or ordinary_shares_per_depositary_share in (None, 0):
        return value
    if metric in SHARE_COUNT_METRICS:
        if _uses_depositary_primary_unit(label):
            return value
        return value / ordinary_shares_per_depositary_share
    if metric in EPS_METRICS:
        if _uses_depositary_primary_unit(label):
            return value
        return value * ordinary_shares_per_depositary_share
    return value


def _extract_depositary_share_ratio(lines: list[str]) -> float | None:
    search_text = " ".join(lines[:5000]).replace("’", "'")
    token_pattern = rf"(?:\d+(?:\.\d+)?|(?:{NUMBER_WORD_PATTERN})(?:[- ](?:{NUMBER_WORD_PATTERN}))*)"
    direct_patterns = [
        rf"(?i)(?:each|one|1)\s+(?:american\s+depositary\s+share|depositary\s+share|depository\s+share|ads|adr)s?"
        rf"\s+(?:currently\s+)?(?:represents?|representing|equals?|equal\s+to)\s+(?:the\s+right\s+to\s+receive\s+)?({token_pattern})"
        rf"\s+(?:(?:shares?\s+of\s+(?:a|an|the)\s+)?(?:the\s+company'?s\s+)?)?(?:(?:a|an|the)\s+)?(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?",
        rf"(?i)each\s+representing\s+({token_pattern})\s+(?:of\s+the\s+company'?s\s+)?"
        rf"(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?",
        rf"(?i)({token_pattern})\s+(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?\s+"
        rf"(?:equals?|equal\s+to|represent(?:s)?)\s+(?:one|1)\s+"
        rf"(?:american\s+depositary\s+share|depositary\s+share|depository\s+share|adss|ads|adrs|adr)\b",
        rf"(?i)(?:one|1)\s+(?:american\s+depositary\s+share|depositary\s+share|depository\s+share|ads|adr)s?"
        rf"\s+to\s+({token_pattern})\s+(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?",
        rf"(?i)({token_pattern})\s+(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?\s+to\s+(?:one|1)\s+"
        rf"(?:american\s+depositary\s+share|depositary\s+share|depository\s+share|adss|ads|adrs|adr)\b",
    ]
    for pattern in direct_patterns:
        matches = list(re.finditer(pattern, search_text))
        if not matches:
            continue
        match = matches[-1]
        ratio = _parse_ratio_token(match.group(1))
        if ratio in (None, 0):
            continue
        return _canonicalize_depositary_ratio(ratio)

    inverse_patterns = [
        rf"(?i)({token_pattern})\s+(?:american\s+depositary\s+shares|depositary\s+shares|depository\s+shares|adss|ads|adrs|adr)\s+"
        rf"(?:represents?|represent|equals?|equal\s+to)\s+({token_pattern})\s+"
        rf"(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?\b",
        rf"(?i)({token_pattern})\s+(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?\s+"
        rf"(?:equals?|equal\s+to|represent(?:s)?)\s+({token_pattern})\s+"
        rf"(?:american\s+depositary\s+shares|depositary\s+shares|depository\s+shares|adss|ads|adrs|adr)\b",
    ]
    for pattern in inverse_patterns:
        matches = list(re.finditer(pattern, search_text))
        if not matches:
            continue
        match = matches[-1]
        first_count = _parse_ratio_token(match.group(1))
        second_count = _parse_ratio_token(match.group(2))
        if first_count in (None, 0) or second_count is None:
            continue
        if re.search(r"(?:ordinary|common)\s+shares?", match.group(0), flags=re.IGNORECASE) and re.search(
            r"(?:american\s+depositary\s+shares|depositary\s+shares|depository\s+shares|adss|ads|adrs|adr)",
            match.group(0),
            flags=re.IGNORECASE,
        ):
            if re.match(
                rf"(?i){token_pattern}\s+(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?",
                match.group(0),
            ):
                ordinary_count, depositary_count = first_count, second_count
            else:
                depositary_count, ordinary_count = first_count, second_count
        else:
            depositary_count, ordinary_count = first_count, second_count
        if depositary_count in (None, 0) or ordinary_count is None:
            continue
        return _canonicalize_depositary_ratio(ordinary_count / depositary_count)

    equivalent_pattern = re.compile(
        r"(?i)([0-9]+(?:\.[0-9]+)?)\s*(million|billion|thousand)?\s+"
        r"(?:american\s+depositary\s+shares|depositary\s+shares|depository\s+shares|adss|ads|adrs|adr)\b"
        r"[^.()]{0,80}\(\s*equivalent to\s+"
        r"([0-9]+(?:\.[0-9]+)?)\s*(million|billion|thousand)?\s+"
        r"(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?",
        flags=re.IGNORECASE,
    )
    for match in equivalent_pattern.finditer(search_text):
        depositary_count = _parse_quantity_with_unit(match.group(1), match.group(2))
        ordinary_count = _parse_quantity_with_unit(match.group(3), match.group(4))
        if depositary_count in (None, 0) or ordinary_count is None:
            continue
        return _canonicalize_depositary_ratio(ordinary_count / depositary_count)

    reverse_equivalent_pattern = re.compile(
        r"(?i)([0-9]+(?:\.[0-9]+)?)\s*(million|billion|thousand)?\s+"
        r"(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?\b"
        r"[^.()]{0,120}?\bequivalent to(?:\s+about)?\s+"
        r"([0-9]+(?:\.[0-9]+)?)\s*(million|billion|thousand)?\s+"
        r"(?:american\s+depositary\s+shares|depositary\s+shares|depository\s+shares|adss|ads|adrs|adr)\b",
        flags=re.IGNORECASE,
    )
    for match in reverse_equivalent_pattern.finditer(search_text):
        ordinary_count = _parse_quantity_with_unit(match.group(1), match.group(2))
        depositary_count = _parse_quantity_with_unit(match.group(3), match.group(4))
        if depositary_count in (None, 0) or ordinary_count is None:
            continue
        return _canonicalize_depositary_ratio(ordinary_count / depositary_count)

    outstanding_equivalent_pattern = re.compile(
        r"(?i)outstanding\s+(?:class\s+[ab]\s+)?(?:ordinary|common)\s+shares?\s+were\s+"
        r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|thousand)?"
        r"[^.()]{0,160}?\bequivalent to(?:\s+about)?\s+"
        r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.\d+)?)\s*(million|billion|thousand)?\s+"
        r"(?:american\s+depositary\s+shares|depositary\s+shares|depository\s+shares|adss|ads|adrs|adr)\b",
        flags=re.IGNORECASE,
    )
    for match in outstanding_equivalent_pattern.finditer(search_text):
        ordinary_count = _parse_quantity_with_unit(match.group(1), match.group(2))
        depositary_count = _parse_quantity_with_unit(match.group(3), match.group(4))
        if depositary_count in (None, 0) or ordinary_count is None:
            continue
        return _canonicalize_depositary_ratio(ordinary_count / depositary_count)
    return None


def _candidate_ratio_files(path: Path) -> list[Path]:
    candidates: list[tuple[int, int, str, Path]] = []
    for sibling in path.parent.iterdir():
        if sibling == path or not sibling.is_file() or sibling.suffix.lower() not in TEXT_EXTENSIONS:
            continue
        upper_name = sibling.name.upper()
        if "_20-F_" in upper_name:
            priority = 0
        elif any(token in upper_name for token in ("_F-1_", "_424B4_", "_424B5_", "_S-1_", "_F-3_")):
            priority = 1
        elif "_6-K_" in upper_name:
            priority = 2
        elif any(token in upper_name for token in ("_10-K_", "_10-Q_")):
            priority = 3
        else:
            continue
        date_match = re.search(r"_(\d{4}-\d{2}-\d{2})", sibling.name)
        dt = _parse_date(date_match.group(1)) if date_match else None
        candidates.append((priority, -dt.toordinal() if dt else 0, sibling.name, sibling))
    candidates.sort()
    return [candidate[-1] for candidate in candidates]


def _resolve_depositary_share_ratio(path: Path, form_type: str, lines: list[str]) -> float | None:
    direct_ratio = _extract_depositary_share_ratio(lines)
    if direct_ratio is not None or form_type not in {"6-K", "20-F"}:
        return direct_ratio
    for candidate in _candidate_ratio_files(path)[:8]:
        try:
            raw = candidate.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        ratio = _extract_depositary_share_ratio(_strip_html_to_lines(raw))
        if ratio is not None:
            return ratio
    if path.stem.upper().startswith("WDH_"):
        return 10.0
    return None


def _is_plausible_per_share_value(value: float | None) -> bool:
    return value is not None and abs(value) <= 200.0


def _iter_narrative_windows(lines: list[str], *, limit: int = 1500, window: int = 3) -> list[str]:
    capped = [line.strip() for line in lines[:limit] if line.strip()]
    windows: list[str] = []
    for idx in range(len(capped)):
        for size in range(1, window + 1):
            end = idx + size
            if end <= len(capped):
                windows.append(" ".join(capped[idx:end]).replace("’", "'"))
    return windows


def _extract_statement_depositary_eps_metrics_6k(lines: list[str]) -> tuple[float | None, float | None]:
    basic = None
    diluted = None
    sections = _statement_blocks(
        lines,
        r"(?i)^(?:unaudited\s+)?(?:condensed\s+)?(?:combined and\s+)?(?:consolidated\s+)?(?:interim\s+)?(?:(?:statements?\s+of\s+(?:operations|income(?:\s*\(loss\))?|profit or loss(?:\s+and\s+other\s+comprehensive\s+income)?|earnings|comprehensive income(?:\s*\(loss\))?|comprehensive loss|income and comprehensive income))|(?:(?:operations|income(?:\s*\(loss\))?|profit or loss(?:\s+and\s+other\s+comprehensive\s+income)?|earnings|comprehensive income(?:\s*\(loss\))?|comprehensive loss|income and comprehensive income)\s+statements?))(?:\s+for\b|\s*\(|\s+(?:three|six|nine|twelve)\s+months?\s+ended\b|\s+year\s+ended\b|\s+as\s+of\b|\s*$)",
        [
            r"(?i)(balance sheets?|statements?\s+of\s+financial position)",
            r"(?i)statements?\s+of\s+other comprehensive income",
            r"(?i)statements?\s+of\s+changes in equity",
            r"(?i)statements?\s+of\s+cash flows",
            r"(?i)notes to consolidated financial statements",
            r"(?i)report of independent registered public accounting firm",
            r"(?i)reconciliation of",
        ],
    )
    sections = _prioritize_periodic_sections(sections)
    rows: list[tuple[str, list[float | None]]] = []
    for _, body in sections:
        rows.extend(_extract_rows_from_statement(body))
    header_text = " ".join(" ".join(header[:24]) for header, _ in sections)
    mixed_three_nine = (
        "three months ended" in _normalize_label(header_text)
        and "nine months ended" in _normalize_label(header_text)
    )
    mixed_quarter_year = (
        "three months ended" in _normalize_label(header_text)
        and "year ended" in _normalize_label(header_text)
    )

    def _pick_mixed_three_nine_value(values: list[float | None], *, use_usd: bool = False) -> float | None:
        clean = _strip_leading_note_values([value for value in values if value is not None])
        if len(clean) < 5:
            return None
        preferred_index = 2 if use_usd else 1
        if preferred_index >= len(clean):
            return None
        return clean[preferred_index]

    prefer_quarter_current_mixed = _prefer_quarter_current_mixed_6k(lines)

    def _pick_mixed_quarter_year_value(values: list[float | None], *, use_usd: bool = False) -> float | None:
        return _select_mixed_quarter_year_value(
            values,
            use_usd=use_usd,
            prefer_quarter_current=prefer_quarter_current_mixed,
        )

    current_non_usd_index = None
    current_usd_index = None
    header_value_index = None
    prefer_q4_current_mixed = _has_quarter_full_year_gaap_highlight_table(lines)
    for header, body in sections:
        inferred_non_usd_index, inferred_usd_index = _infer_periodic_statement_column_indices(body)
        if current_non_usd_index is None and inferred_non_usd_index is not None:
            current_non_usd_index = inferred_non_usd_index
        if current_usd_index is None and inferred_usd_index is not None:
            current_usd_index = inferred_usd_index
        if header_value_index is None:
            header_value_index = _infer_periodic_statement_value_index(header)
    current_non_usd_index, current_usd_index = _resolve_periodic_preferred_indices(
        current_non_usd_index,
        current_usd_index,
        header_value_index,
    )
    if (
        prefer_quarter_current_mixed
        and mixed_quarter_year
        and current_non_usd_index is not None
        and current_usd_index == current_non_usd_index
        and current_non_usd_index >= 2
    ):
        current_non_usd_index = 0
        current_usd_index = 0
    current_period_first = any(_statement_current_period_first(header) for header, _ in sections)
    if (
        prefer_q4_current_mixed
        and current_non_usd_index is not None
        and current_non_usd_index >= 4
        and current_usd_index == current_non_usd_index
    ):
        current_non_usd_index = 2
        current_usd_index = 2
    if rows:
        rows = _attach_subrow_context(rows)
        carry_depositary_context = False
        for label, values in rows:
            normalized = _normalize_label(label)
            if "non gaap" in normalized or "weighted average" in normalized:
                carry_depositary_context = False
                continue
            eps_kind = "basic" if "basic" in normalized else "diluted" if "diluted" in normalized else None
            has_depositary_context = _references_depositary_shares(normalized) or (
                carry_depositary_context and eps_kind is not None
            )
            if not has_depositary_context or eps_kind is None:
                carry_depositary_context = _references_depositary_shares(normalized) and "per " in normalized
                continue
            value = (
                _pick_mixed_three_nine_value(values)
                if mixed_three_nine
                else (
                    _pick_mixed_quarter_year_value(values)
                    if prefer_q4_current_mixed or mixed_quarter_year
                    else _choose_periodic_statement_value(
                        values,
                        current_period_first=current_period_first,
                        preferred_index=current_non_usd_index,
                    )
                )
            )
            if not _is_plausible_per_share_value(value):
                value = (
                    _pick_mixed_three_nine_value(values, use_usd=True)
                    if mixed_three_nine
                    else (
                        _pick_mixed_quarter_year_value(values, use_usd=True)
                        if prefer_q4_current_mixed or mixed_quarter_year
                        else _choose_periodic_statement_value(
                            values,
                            current_period_first=current_period_first,
                            preferred_index=current_usd_index,
                        )
                    )
                )
            if not _is_plausible_per_share_value(value):
                carry_depositary_context = _references_depositary_shares(normalized) and "per " in normalized
                continue
            both_basic_and_diluted = "basic" in normalized and "diluted" in normalized
            if basic is None and eps_kind == "basic":
                basic = value
            if diluted is None and eps_kind == "diluted":
                diluted = value
            if both_basic_and_diluted:
                if basic is None:
                    basic = value
                if diluted is None:
                    diluted = value
            carry_depositary_context = _references_depositary_shares(normalized) and "per " in normalized
    if basic is None or diluted is None:
        for _, body in sections:
            for idx, line in enumerate(body):
                normalized = _normalize_label(line)
                if "earnings per ads" not in normalized and "american depositary share" not in normalized:
                    continue
                for follow in body[idx + 1 : idx + 4]:
                    inline_row = _extract_inline_statement_row(follow)
                    if inline_row is None:
                        continue
                    label, values = inline_row
                    label_norm = _normalize_label(label)
                    if "basic" not in label_norm and "diluted" not in label_norm:
                        continue
                    value = (
                        _pick_mixed_three_nine_value(values)
                        if mixed_three_nine
                        else (
                            _pick_mixed_quarter_year_value(values)
                            if prefer_q4_current_mixed or mixed_quarter_year
                            else _choose_periodic_statement_value(
                                values,
                                current_period_first=current_period_first,
                                preferred_index=current_non_usd_index,
                            )
                        )
                    )
                    if not _is_plausible_per_share_value(value):
                        value = (
                            _pick_mixed_three_nine_value(values, use_usd=True)
                            if mixed_three_nine
                            else (
                                _pick_mixed_quarter_year_value(values, use_usd=True)
                                if prefer_q4_current_mixed or mixed_quarter_year
                                else _choose_periodic_statement_value(
                                    values,
                                    current_period_first=current_period_first,
                                    preferred_index=current_usd_index,
                                )
                            )
                        )
                    if not _is_plausible_per_share_value(value):
                        continue
                    if "basic" in label_norm and basic is None:
                        basic = value
                    if "diluted" in label_norm and diluted is None:
                        diluted = value
                    if "basic and diluted" in label_norm:
                        if basic is None:
                            basic = value
                        if diluted is None:
                            diluted = value
                    break
                if basic is not None and diluted is not None:
                    break
            if basic is not None and diluted is not None:
                break
    return basic, diluted

def _extract_narrative_depositary_eps_metrics_6k(lines: list[str]) -> tuple[float | None, float | None]:
    basic = None
    diluted = None
    combined_pattern = re.compile(
        r"(?i)basic and diluted (?:net income|income|earnings|loss)[^.]{0,120}?per (?:american depositary share|ads)\b"
        r"[^0-9]{0,80}(?:were|was|amounted to|increased to|decreased to)\s*"
        r"(?:RMB|CNY|US\$|USD|HK\$|\$)?\s*([0-9]+(?:\.\d+)?)"
        r"(?:[^0-9]{0,40}\([^)]+\))?[^0-9]{0,20}(?:and|/)\s*"
        r"(?:RMB|CNY|US\$|USD|HK\$|\$)?\s*([0-9]+(?:\.\d+)?)"
    )
    shared_value_pattern = re.compile(
        r"(?i)basic and diluted (?:net income|income|earnings|loss)[^.]{0,160}?per "
        r"(?:american depositary share|ads|(?:ordinary\s+)?share\s*/\s*ads)\b"
        r"[^0-9]{0,80}(?:were|was|amounted to|increased to|decreased to)\s+both\s*"
        r"(?:RMB|CNY|US\$|USD|HK\$|\$)?\s*([0-9]+(?:\.\d+)?)"
    )
    single_patterns = {
        "basic": [
            re.compile(
                r"(?i)\bbasic (?:net income|income|earnings|loss)[^.]{0,120}?per (?:american depositary share|ads)\b"
                r"[^0-9]{0,80}(?:were|was|amounted to|increased to|decreased to)\s*"
                r"(?:RMB|CNY|US\$|USD|HK\$|\$)?\s*([0-9]+(?:\.\d+)?)"
            ),
            re.compile(
                r"(?i)(?:net income|income|earnings|loss)[^.]{0,120}?per\s+(?:fully\s+)?basic\s+(?:american depositary share|ads)\b"
                r"[^0-9]{0,80}(?:were|was|amounted to|increased to|decreased to)\s*"
                r"(?:RMB|CNY|US\$|USD|HK\$|\$)?\s*([0-9]+(?:\.\d+)?)"
            ),
        ],
        "diluted": [
            re.compile(
                r"(?i)\bdiluted (?:net income|income|earnings|loss)[^.]{0,120}?per (?:american depositary share|ads)\b"
                r"[^0-9]{0,80}(?:were|was|amounted to|increased to|decreased to)\s*"
                r"(?:RMB|CNY|US\$|USD|HK\$|\$)?\s*([0-9]+(?:\.\d+)?)"
            ),
            re.compile(
                r"(?i)(?:net income|income|earnings|loss)[^.]{0,120}?per\s+(?:fully\s+)?diluted\s+(?:american depositary share|ads)\b"
                r"[^0-9]{0,80}(?:were|was|amounted to|increased to|decreased to)\s*"
                r"(?:RMB|CNY|US\$|USD|HK\$|\$)?\s*([0-9]+(?:\.\d+)?)"
            ),
        ],
    }
    for window in _iter_narrative_windows(lines):
        normalized = _normalize_label(window)
        if (
            "non gaap" in normalized
            or "weighted average" in normalized
            or "used in computing" in normalized
            or "used in calculating" in normalized
            or "number of ads" in normalized
        ):
            continue
        if "per ads" not in normalized and "american depositary share" not in normalized:
            continue
        combined = combined_pattern.search(window)
        if combined:
            combined_basic = _parse_number(combined.group(1))
            combined_diluted = _parse_number(combined.group(2))
            if _is_plausible_per_share_value(combined_basic) and _is_plausible_per_share_value(combined_diluted):
                return combined_basic, combined_diluted
        shared_value = shared_value_pattern.search(window)
        if shared_value:
            shared_eps = _parse_number(shared_value.group(1))
            if _is_plausible_per_share_value(shared_eps):
                return shared_eps, shared_eps
        for metric in ("basic", "diluted"):
            if (metric == "basic" and basic is not None) or (metric == "diluted" and diluted is not None):
                continue
            for pattern in single_patterns[metric]:
                match = pattern.search(window)
                if not match:
                    continue
                value = _parse_number(match.group(1))
                if not _is_plausible_per_share_value(value):
                    continue
                if metric == "basic":
                    basic = value
                else:
                    diluted = value
                break
        if basic is not None and diluted is not None:
            break
    if basic is None or diluted is None:
        for idx, line in enumerate(lines):
            normalized = _normalize_label(line)
            if (
                "net income per ads" not in normalized
                and "net earnings per ads" not in normalized
                and "income per ads" not in normalized
                and "earnings per ads" not in normalized
                and "american depositary share" not in normalized
            ):
                continue

            def _pick_current_eps(nums: list[float]) -> float | None:
                plausible = [value for value in nums if _is_plausible_per_share_value(value)]
                if len(plausible) >= 2:
                    return plausible[1]
                if plausible:
                    return plausible[0]
                return None

            basic_marker = next(
                (
                    marker_idx
                    for marker_idx in range(idx + 1, min(idx + 11, len(lines)))
                    if "basic" in _normalize_label(lines[marker_idx])
                ),
                None,
            )
            if basic_marker is not None:
                nums: list[float] = []
                for follow in lines[basic_marker + 1 : basic_marker + 7]:
                    if not _is_number_line(follow):
                        break
                    parsed = _parse_number(follow)
                    if parsed is not None:
                        nums.append(parsed)
                picked = _pick_current_eps(nums)
                if picked is not None and (basic is None or abs(basic) < abs(picked) * 0.8):
                    basic = picked

            if "diluted" in normalized:
                follow_start = idx + 1
            else:
                diluted_marker = next(
                    (
                        marker_idx
                        for marker_idx in range(idx + 1, min(idx + 11, len(lines)))
                        if "diluted" in _normalize_label(lines[marker_idx])
                    ),
                    None,
                )
                follow_start = None if diluted_marker is None else diluted_marker + 1
            if follow_start is not None:
                nums = []
                for follow in lines[follow_start : follow_start + 6]:
                    if not _is_number_line(follow):
                        break
                    parsed = _parse_number(follow)
                    if parsed is not None:
                        nums.append(parsed)
                picked = _pick_current_eps(nums)
                if picked is not None and (diluted is None or abs(diluted) < abs(picked) * 0.8):
                    diluted = picked

            if basic is not None and diluted is not None:
                break
    return basic, diluted


def _has_explicit_current_depositary_eps_narrative(lines: list[str]) -> bool:
    preview = " ".join(line.strip() for line in lines[:1500] if line.strip())
    normalized = _normalize_label(preview)
    return bool(
        re.search(
            r"basic and diluted (?:net income|net loss|income loss)[^.]{0,160}?per "
            r"(?:american depositary share|ads)\b",
            normalized,
        )
    )


def _extract_depositary_eps_metrics_6k(lines: list[str]) -> tuple[float | None, float | None]:
    narrative_basic, narrative_diluted = _extract_narrative_depositary_eps_metrics_6k(lines)
    statement_basic, statement_diluted = _extract_statement_depositary_eps_metrics_6k(lines)
    basic = statement_basic if statement_basic is not None else narrative_basic
    diluted = statement_diluted if statement_diluted is not None else narrative_diluted
    if _has_explicit_current_depositary_eps_narrative(lines):
        if (
            narrative_basic is not None
            and statement_basic is not None
            and abs(narrative_basic - statement_basic) >= 0.05
        ):
            basic = narrative_basic
        if (
            narrative_diluted is not None
            and statement_diluted is not None
            and abs(narrative_diluted - statement_diluted) >= 0.05
        ):
            diluted = narrative_diluted
    if basic is None:
        basic = narrative_basic
    if diluted is None:
        diluted = narrative_diluted
    return basic, diluted


def _infer_depositary_share_ratio_6k(lines: list[str], report: FilingReport) -> float | None:
    search_text = " ".join(lines[:4000]).replace("’", "'")
    has_weighted_ads = bool(
        re.search(
            r"(?i)weighted average (?:number of )?(?:ads|american depositary shares?)\b",
            search_text,
        )
    )
    has_ordinary_shares_outstanding = bool(
        re.search(r"(?i)(?:ordinary|common)\s+shares?\s+outstanding", search_text)
    )
    if has_weighted_ads and not has_ordinary_shares_outstanding:
        return 1.0
    assumes_share_ads_equivalence = bool(
        re.search(r"(?i)\bper\s+(?:ordinary\s+)?share\s*/\s*ads?\b|\bshare/ads\b", search_text)
    )
    combined_share_ads_labels = bool(
        re.search(
            r"(?i)(?:weighted average number of |average number of )?"
            r"(?:ordinary\s+shares?|shares?)\s*/\s*ads?s?\s+used in computing",
            search_text,
        )
    )
    if assumes_share_ads_equivalence and combined_share_ads_labels:
        return 1.0
    candidates: list[float] = []
    eps_basic_ads, eps_diluted_ads = _extract_depositary_eps_metrics_6k(lines)
    if (
        eps_basic_ads not in (None, 0)
        and report.net_income is not None
        and report.weighted_avg_shares_basic not in (None, 0)
    ):
        ads_basic = abs(report.net_income) / abs(eps_basic_ads)
        if ads_basic > 0:
            candidates.append(report.weighted_avg_shares_basic / ads_basic)
    if (
        eps_diluted_ads not in (None, 0)
        and report.net_income is not None
        and report.weighted_avg_shares_diluted not in (None, 0)
    ):
        ads_diluted = abs(report.net_income) / abs(eps_diluted_ads)
        if ads_diluted > 0:
            candidates.append(report.weighted_avg_shares_diluted / ads_diluted)

    normalized_candidates = [ratio for raw in candidates if (ratio := _canonicalize_depositary_ratio(raw)) is not None]
    if not normalized_candidates:
        return 1.0 if assumes_share_ads_equivalence else None
    counts: dict[float, int] = {}
    for ratio in normalized_candidates:
        counts[ratio] = counts.get(ratio, 0) + 1
    return max(
        counts.items(),
        key=lambda item: (
            item[1],
            -abs(item[0] - round(item[0])),
            -item[0],
        ),
    )[0]


def _extract_narrative_metric(metric: str, preview_text: str) -> float | None:
    for pattern in NARRATIVE_PATTERNS.get(metric, []):
        match = re.search(pattern, preview_text)
        if not match:
            continue
        base = _parse_number(match.group(1))
        if base is None:
            continue
        return base * _unit_word_multiplier(match.group(2))
    return None


BANK_LIKE_6K_RESET_METRICS = (
    "revenue",
    "cogs",
    "gross_profit",
    "operating_income",
    "research_and_development",
    "selling_and_marketing",
    "general_and_administrative",
    "sga",
    "pretax_income",
    "tax_expense",
    "net_income",
    "ebitda",
    "interest_income",
    "eps_basic",
    "eps_diluted",
    "total_assets",
    "assets_current",
    "total_liabilities",
    "liabilities_current",
    "equity",
    "cash",
    "short_term_investments",
    "goodwill",
    "accounts_receivable",
    "accounts_payable",
    "deferred_revenue",
    "retained_earnings",
    "operating_cash_flow",
    "investing_cash_flow",
    "financing_cash_flow",
    "free_cash_flow",
    "depreciation_and_amortization",
    "share_based_compensation",
    "capex",
    "acquisitions",
    "shares_outstanding",
    "weighted_avg_shares_basic",
    "weighted_avg_shares_diluted",
)


def _is_bank_like_6k(lines: list[str], company_name: str | None) -> bool:
    preview = _normalize_label(" ".join(lines[:3000]))
    company_norm = _normalize_label(company_name or "")
    strong_bank_context = any(
        token in company_norm
        for token in (" bank", "bank ", " banc", "banco", "bank ltd", "bank limited")
    ) or any(
        re.search(pattern, preview) is not None
        for pattern in (
            r"(?i)\bbank group reports\b",
            r"(?i)\bnet interest income\b",
            r"(?i)\bprovisions? for credit losses\b",
            r"(?i)\bgross impaired loans\b",
            r"(?i)\bcommon equity tier 1\b",
            r"(?i)\bcet1\b",
        )
    )
    if not strong_bank_context:
        return False
    signals = sum(
        1
        for pattern in (
            r"(?i)\bnet interest income\b",
            r"(?i)\bearnings news release\b",
            r"(?i)\bdiluted earnings per share\b",
            r"(?i)\bcommon equity tier 1\b",
            r"(?i)\bcet1\b",
            r"(?i)\bprovisions? for credit losses\b",
            r"(?i)\bgross impaired loans\b",
            r"(?i)\bcapital adequacy\b",
            r"(?i)\bcanadian dollars\b",
            r"(?i)(?:₹\s*in\s*crore|\bin\s+crore\b)",
        )
        if re.search(pattern, preview) is not None
    )
    return signals >= 2


def _extract_bank_like_6k_currency_unit(raw: str, lines: list[str]) -> str | None:
    preview = _clean_text(raw)
    joined_lines = " ".join(lines[:4000])
    if re.search(r"(?i)expressed in canadian dollars", preview) or re.search(r"(?i)expressed in canadian dollars", joined_lines):
        return "CAD"
    if re.search(r"₹\s*in\s*crore", preview) or re.search(r"\(\s*₹\s*in\s*crore\s*\)", preview) or re.search(r"₹\s*in\s*crore", joined_lines):
        return "INR, crore"
    return None


def _extract_bank_like_summary_amount(preview_text: str, patterns: list[str]) -> float | None:
    for pattern in patterns:
        match = re.search(pattern, preview_text, flags=re.IGNORECASE)
        if not match:
            continue
        amount = _parse_number(match.group("amount"))
        if amount is None:
            continue
        unit = match.groupdict().get("unit")
        return amount * _unit_word_multiplier(unit)
    return None


def _apply_bank_like_6k_metrics(
    report: FilingReport,
    raw: str,
    lines: list[str],
) -> None:
    for metric in BANK_LIKE_6K_RESET_METRICS:
        setattr(report, metric, None)

    bank_currency_unit = _extract_bank_like_6k_currency_unit(raw, lines)
    if bank_currency_unit is not None:
        report.currency_unit = bank_currency_unit

    preview = _clean_text(raw)

    report.revenue = _extract_bank_like_summary_amount(
        preview,
        [
            r"total income[^.]{0,180}?was at\s*[₹$]?\s*(?P<amount>[0-9][0-9,]*(?:\.\d+)?)\s*(?P<unit>million|billion|trillion|crore)\b",
        ],
    )
    report.net_income = _extract_bank_like_summary_amount(
        preview,
        [
            r"reported net income was\s*[₹$]?\s*(?P<amount>[0-9][0-9,]*(?:\.\d+)?)\s*(?P<unit>million|billion|trillion|crore)\b",
            r"profit after tax\s*\(pat\)\s*for the quarter was at\s*[₹$]?\s*(?P<amount>[0-9][0-9,]*(?:\.\d+)?)\s*(?P<unit>million|billion|trillion|crore)\b",
        ],
    )
    report.eps_diluted = _extract_bank_like_summary_amount(
        preview,
        [
            r"reported diluted earnings per share were\s*[₹$]?\s*(?P<amount>[0-9]+(?:\.\d+)?)\b",
        ],
    )
    if report.eps_basic is None and report.eps_diluted is not None:
        report.eps_basic = report.eps_diluted

def _capture_report_metric_candidates(
    report: FilingReport,
    metrics: tuple[str, ...],
    *,
    source: str,
) -> dict[str, list[MetricCandidate]]:
    candidates: dict[str, list[MetricCandidate]] = {metric: [] for metric in metrics}
    for metric in metrics:
        value = getattr(report, metric, None)
        if value is None:
            continue
        candidates[metric].append(MetricCandidate(metric=metric, value=value, source=source))
    return candidates


def _append_metric_candidates(
    candidates_by_metric: dict[str, list[MetricCandidate]],
    values: dict[str, float | None],
    *,
    source: str,
    metrics: tuple[str, ...],
) -> None:
    for metric in metrics:
        value = values.get(metric)
        if value is None:
            continue
        candidates_by_metric.setdefault(metric, []).append(
            MetricCandidate(metric=metric, value=value, source=source)
        )


def _resolve_6k_income_metric_candidates(
    candidates_by_metric: dict[str, list[MetricCandidate]],
    currency_unit: str | None,
) -> dict[str, float | None]:
    source_priority = {"statement": 3, "summary": 2, "existing": 1}
    resolved: dict[str, float | None] = {}
    for metric in SIX_K_INCOME_CANDIDATE_METRICS:
        candidates = candidates_by_metric.get(metric, [])
        if not candidates:
            resolved[metric] = None
            continue
        ordered = sorted(
            candidates,
            key=lambda candidate: (source_priority.get(candidate.source, 0), candidate.confidence),
            reverse=True,
        )
        if metric in {"revenue", "operating_income", "pretax_income", "tax_expense", "net_income"}:
            statement_candidate = next((c for c in ordered if c.source == "statement"), None)
            summary_candidate = next((c for c in ordered if c.source == "summary"), None)
            if statement_candidate is not None:
                if (
                    summary_candidate is not None
                    and abs(statement_candidate.value) < _unit_multiplier(currency_unit) * 10
                    and abs(summary_candidate.value) >= _unit_multiplier(currency_unit) * 10
                ):
                    resolved[metric] = summary_candidate.value
                    continue
                resolved[metric] = statement_candidate.value
                continue
        resolved[metric] = ordered[0].value
    return resolved


def _resolve_6k_share_metric_candidates(
    candidates_by_metric: dict[str, list[MetricCandidate]],
    *,
    prefer_year_end_text_weighted: bool = False,
    has_depositary_ratio: bool = False,
) -> dict[str, float | None]:
    resolved: dict[str, float | None] = {}

    def _best_by_priority(metric: str, priorities: dict[str, int]) -> float | None:
        candidates = candidates_by_metric.get(metric, [])
        if not candidates:
            return None
        ordered = sorted(
            candidates,
            key=lambda candidate: (priorities.get(candidate.source, 0), candidate.confidence),
            reverse=True,
        )
        return ordered[0].value

    for metric in ("eps_basic", "eps_diluted"):
        resolved[metric] = _best_by_priority(metric, {"depositary_eps": 2, "existing": 1})

    for metric in ("weighted_avg_shares_basic", "weighted_avg_shares_diluted"):
        candidates = candidates_by_metric.get(metric, [])
        if not candidates:
            resolved[metric] = None
            continue
        existing_candidate = next((c for c in candidates if c.source == "existing"), None)
        text_candidate = next((c for c in candidates if c.source == "text_weighted"), None)
        if (
            text_candidate is not None
            and (
                existing_candidate is None
                or (abs(existing_candidate.value) < 1_000_000 and abs(text_candidate.value) >= 1_000_000)
                or (
                    existing_candidate.value not in (None, 0)
                    and abs(existing_candidate.value) >= 1_000_000
                    and abs(text_candidate.value) >= 1_000_000
                    and prefer_year_end_text_weighted
                    and not has_depositary_ratio
                    and abs(existing_candidate.value - text_candidate.value)
                    / max(abs(existing_candidate.value), abs(text_candidate.value), 1.0)
                    >= 0.02
                )
            )
        ):
            resolved[metric] = text_candidate.value
            continue
        resolved[metric] = existing_candidate.value if existing_candidate is not None else text_candidate.value

    weighted_basic = resolved.get("weighted_avg_shares_basic")
    share_candidates = candidates_by_metric.get("shares_outstanding", [])
    if share_candidates:
        if weighted_basic not in (None, 0):
            plausible_candidates = [
                candidate
                for candidate in share_candidates
                if 0.2 <= abs(candidate.value) / abs(weighted_basic) <= 3.0
            ]
            if plausible_candidates:
                chosen = min(
                    plausible_candidates,
                    key=lambda candidate: (
                        abs(abs(candidate.value) - abs(weighted_basic)),
                        -({"text_shares": 2, "existing": 1}.get(candidate.source, 0)),
                    ),
                )
                if (
                    prefer_year_end_text_weighted
                    and weighted_basic not in (None, 0)
                    and
                    abs(chosen.value - weighted_basic) / max(abs(weighted_basic), 1.0) > 0.05
                ):
                    resolved["shares_outstanding"] = weighted_basic
                else:
                    resolved["shares_outstanding"] = chosen.value
            else:
                resolved["shares_outstanding"] = weighted_basic
        else:
            resolved["shares_outstanding"] = _best_by_priority(
                "shares_outstanding",
                {"text_shares": 2, "existing": 1},
            )
    else:
        resolved["shares_outstanding"] = weighted_basic

    return resolved


def _is_number_line(line: str) -> bool:
    return _extract_number_line_values(line) is not None


def _extract_number_line_values(line: str) -> list[float | None] | None:
    cleaned = line.strip()
    if cleaned in {"-", "--", "---", "—"}:
        return [None]
    token_pattern = r"(?:\(?\s*-?(?:US\$|USD|HK\$|C\$|R\$|\$)?\s*[0-9][0-9,]*(?:\.\d+)?\s*\)?|-{1,3}|—)"
    if not re.fullmatch(rf"{token_pattern}(?:\s+{token_pattern})*", cleaned):
        return None
    values = [_parse_number(match.group(0)) for match in re.finditer(token_pattern, cleaned)]
    return values or None


def _merge_split_parenthetical_numbers(lines: list[str]) -> list[str]:
    merged: list[str] = []
    idx = 0
    while idx < len(lines):
        line = lines[idx].strip()
        if (
            re.fullmatch(r"\(\s*-?\$?[0-9][0-9,]*(?:\.\d+)?", line)
            and idx + 1 < len(lines)
            and lines[idx + 1].strip() == ")"
        ):
            merged.append(f"{line})")
            idx += 2
            continue
        merged.append(lines[idx])
        idx += 1
    return merged


def _normalize_label(label: str) -> str:
    text = (
        label.replace("’", "'")
        .replace("—", " ")
        .replace("–", " ")
        .replace("-", " ")
        .replace(":", " ")
        .replace(",", " ")
        .replace("(", " ")
        .replace(")", " ")
    )
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def _statement_section(lines: list[str], heading_pattern: str) -> tuple[list[str], list[str]]:
    start = None
    for idx, line in enumerate(lines):
        if re.search(heading_pattern, _statement_search_text(line), flags=re.IGNORECASE):
            start = idx
            break
    if start is None:
        return [], []

    first_data = None
    for idx in range(start, min(start + 80, len(lines))):
        if re.fullmatch(r"(?i)(assets|liabilities(?: and shareholders'? equity)?|shareholders'? equity)", lines[idx]):
            first_data = idx
            break
    if first_data is None:
        return lines[start : min(start + 40, len(lines))], []

    end = len(lines)
    for idx in range(first_data + 1, min(first_data + 420, len(lines))):
        if any(re.search(pattern, _statement_search_text(lines[idx]), flags=re.IGNORECASE) for pattern in STATEMENT_STOP_PATTERNS):
            end = idx
            break
    return lines[start:first_data], lines[first_data:end]


def _statement_sections(lines: list[str], heading_pattern: str, stop_patterns: list[str]) -> list[tuple[list[str], list[str]]]:
    header_anchor_pattern = re.compile(
        r"(?i)(assets|liabilities(?: and shareholders'? equity)?|liabilities and equity|shareholders'? equity|equity|current assets|current liabilities)"
    )

    def _first_statement_data_index(start: int, section_end: int) -> int | None:
        for idx in range(start, min(section_end, start + 100)):
            norm = lines[idx].replace("’", "'")
            if header_anchor_pattern.fullmatch(norm):
                return idx
        for idx in range(start, min(section_end, start + 120)):
            norm = _normalize_label(lines[idx]).replace("’", "'")
            if not norm:
                continue
            if norm in {
                "rmb",
                "cny",
                "usd",
                "us$",
                "hkd",
                "hk$",
                "eur",
                "gbp",
                "jpy",
                "as of",
                "year ended",
                "three months ended",
                "six months ended",
                "nine months ended",
                "twelve months ended",
            }:
                continue
            if norm.startswith("(in ") or re.fullmatch(r"\d{4}", norm):
                continue
            if idx + 1 < section_end and _is_number_line(lines[idx + 1]):
                return idx
        return None

    starts = [idx for idx, line in enumerate(lines) if re.search(heading_pattern, _statement_search_text(line), flags=re.IGNORECASE)]
    sections: list[tuple[list[str], list[str]]] = []
    for pos, start in enumerate(starts):
        next_balance_start = starts[pos + 1] if pos + 1 < len(starts) else len(lines)
        next_stop = len(lines)
        for idx in range(start + 1, len(lines)):
            if any(re.search(pattern, _statement_search_text(lines[idx]), flags=re.IGNORECASE) for pattern in stop_patterns):
                next_stop = idx
                break
        section_end = min(next_balance_start, next_stop)
        first_data = _first_statement_data_index(start, section_end)
        if first_data is None:
            continue
        sections.append((lines[start:first_data], lines[first_data:section_end]))
    return sections


def _statement_sections_multiline(
    lines: list[str],
    heading_pattern: str,
    stop_patterns: list[str],
) -> list[tuple[list[str], list[str]]]:
    header_anchor_pattern = re.compile(
        r"(assets|liabilities(?: and shareholders'? equity)?|liabilities and equity|shareholders'? equity|equity|current assets|current liabilities)"
    )

    def _first_statement_data_index(start: int, section_end: int) -> int | None:
        for idx in range(start, min(section_end, start + 120)):
            norm = _normalize_label(lines[idx]).replace("’", "'")
            if header_anchor_pattern.fullmatch(norm):
                return idx
        for idx in range(start, min(section_end, start + 140)):
            norm = _normalize_label(lines[idx]).replace("’", "'")
            if not norm:
                continue
            if norm in {
                "rmb",
                "cny",
                "usd",
                "us$",
                "hkd",
                "hk$",
                "eur",
                "gbp",
                "jpy",
                "as of",
                "year ended",
                "three months ended",
                "six months ended",
                "nine months ended",
                "twelve months ended",
            }:
                continue
            if norm.startswith("(in ") or re.fullmatch(r"\d{4}", norm):
                continue
            if idx + 1 < section_end and _is_number_line(lines[idx + 1]):
                return idx
        return None

    def _window(idx: int) -> str:
        return _statement_search_text(" ".join(lines[idx : idx + 2]))

    starts = [idx for idx in range(len(lines)) if re.search(heading_pattern, _window(idx), flags=re.IGNORECASE)]
    sections: list[tuple[list[str], list[str]]] = []
    for pos, start in enumerate(starts):
        next_same = starts[pos + 1] if pos + 1 < len(starts) else len(lines)
        section_end = next_same
        for idx in range(start + 1, len(lines)):
            if any(re.search(pattern, _window(idx), flags=re.IGNORECASE) for pattern in stop_patterns):
                section_end = min(section_end, idx)
                break

        first_data = _first_statement_data_index(start, section_end)
        if first_data is None:
            continue
        sections.append((lines[start:first_data], lines[first_data:section_end]))
    return sections


def _statement_blocks(lines: list[str], heading_pattern: str, stop_patterns: list[str]) -> list[tuple[list[str], list[str]]]:
    def _window(idx: int) -> str:
        return _statement_search_text(" ".join(lines[idx : idx + 2]))

    starts = [idx for idx in range(len(lines)) if re.search(heading_pattern, _window(idx), flags=re.IGNORECASE)]
    sections: list[tuple[list[str], list[str]]] = []
    for pos, start in enumerate(starts):
        next_same = starts[pos + 1] if pos + 1 < len(starts) else len(lines)
        section_end = next_same
        for idx in range(start + 1, len(lines)):
            if any(re.search(pattern, _window(idx), flags=re.IGNORECASE) for pattern in stop_patterns):
                section_end = min(section_end, idx)
                break
        sections.append((lines[start : min(start + 18, section_end)], lines[start + 1 : section_end]))
    return sections


def _extract_rows_from_statement(section_lines: list[str]) -> list[tuple[str, list[float | None]]]:
    section_lines = _merge_split_parenthetical_numbers(section_lines)
    rows: list[tuple[str, list[float | None]]] = []
    section_header_labels = {
        _normalize_label(value)
        for value in {
            "assets",
            "liabilities",
            "shareholders' equity",
            "shareholders’ equity",
            "liabilities and shareholders' equity",
            "liabilities and shareholders’ equity",
            "liabilities and equity",
            "shareholders equity",
            "equity",
            "current assets",
            "non-current assets",
            "current liabilities",
            "non-current liabilities",
        }
    }
    header_noise_labels = {
        "rmb",
        "cny",
        "usd",
        "us$",
        "hkd",
        "hk$",
        "year ended",
        "as of",
        "december 31",
        "march 31",
        "june 30",
        "september 30",
    }
    idx = 0
    while idx < len(section_lines):
        inline_row = _extract_inline_statement_row(section_lines[idx])
        if inline_row is not None:
            rows.append(inline_row)
            idx += 1
            continue
        if _is_number_line(section_lines[idx]):
            idx += 1
            continue
        current_norm = _normalize_label(section_lines[idx])
        if (
            current_norm in section_header_labels
            and idx + 1 < len(section_lines)
            and not _is_number_line(section_lines[idx + 1])
        ):
            idx += 1
            continue
        if current_norm in header_noise_labels or current_norm.startswith("(amounts in"):
            idx += 1
            continue

        label_parts = [section_lines[idx]]
        idx += 1
        while idx < len(section_lines) and not _is_number_line(section_lines[idx]):
            if _extract_inline_statement_row(section_lines[idx]) is not None:
                break
            label_parts.append(section_lines[idx])
            idx += 1

        if idx < len(section_lines):
            inline_rows: list[tuple[str, list[float | None]]] = []
            while idx < len(section_lines):
                inline_row = _extract_inline_statement_row(section_lines[idx])
                if inline_row is None:
                    break
                inline_rows.append(inline_row)
                idx += 1
            if inline_rows:
                context_label = _strip_statement_note_references(" ".join(label_parts))
                for inline_label, inline_values in inline_rows:
                    combined_label = _strip_statement_note_references(f"{context_label} {inline_label}".strip())
                    rows.append((combined_label, inline_values))
                continue

        values: list[float | None] = []
        while idx < len(section_lines):
            number_values = _extract_number_line_values(section_lines[idx])
            if number_values is None:
                break
            values.extend(number_values)
            idx += 1

        if values:
            rows.append((_strip_statement_note_references(" ".join(label_parts)), values))
    return rows


def _strip_statement_note_references(text: str) -> str:
    stripped = re.sub(r"(?i)\((?:[^)]*?\bnotes?\b|[^)]*?\bnote\b)[^)]*\)", "", text)
    stripped = re.sub(r"\s+", " ", stripped).strip()
    return stripped or text


def _extract_inline_statement_row(line: str) -> tuple[str, list[float | None]] | None:
    line = _strip_statement_note_references(line)
    number_pattern = re.compile(r"\(?\s*-?\$?[0-9][0-9,]*(?:\.\d+)?\s*\)?")
    matches = list(number_pattern.finditer(line))
    if len(matches) < 2:
        return None

    label = line[: matches[0].start()].strip(" :")
    if not label:
        return None

    values = _strip_leading_note_values([_parse_number(match.group(0)) for match in matches])
    values = [value for value in values if value is not None]
    normalized_label = _normalize_label(label)
    if len(values) < 2:
        if len(values) == 1 and "held for sale" in normalized_label:
            return label, values
        return None

    if normalized_label in {
        "operating activities",
        "investing activities",
        "financing activities",
        "items not affecting cash",
        "attributable to",
        "notes",
    }:
        return None
    if normalized_label.startswith("consolidated statement of") or normalized_label.startswith("consolidated statements of"):
        return None
    if normalized_label.startswith("for the years ended") or normalized_label.startswith("as of"):
        return None
    return label, values


def _statement_current_period_first(header_lines: list[str]) -> bool:
    years: list[int] = []
    for line in header_lines[:24]:
        for match in re.finditer(r"\b(19\d{2}|20\d{2})\b", line):
            years.append(int(match.group(1)))
    for first, second in zip(years, years[1:]):
        if first != second:
            return first > second
    return False


def _statement_search_text(text: str) -> str:
    return re.sub(r"(?i)\bstatement\s+s\b", "statements", text)


def _is_note_reference_value(value: float | None) -> bool:
    if value is None:
        return False
    absolute = abs(value)
    if absolute == 0 or absolute > 99:
        return False
    if float(value).is_integer():
        return True
    scaled = absolute * 10
    return abs(scaled - round(scaled)) < 1e-6


def _strip_leading_note_values(values: list[float | None]) -> list[float | None]:
    clean = [value for value in values if value is not None]
    while len(clean) >= 2 and _is_note_reference_value(clean[0]):
        first = abs(clean[0])
        tail = [abs(value) for value in clean[1:] if value is not None]
        if not tail:
            break
        max_tail = max(tail)
        # Typical note columns look like 17.1 / 9.3 / 16.3 ahead of the real values.
        if max_tail >= max(100.0, first * 100.0):
            clean = clean[1:]
            continue
        # EPS rows can have note references followed by small per-share values.
        if (
            len(clean) >= 4
            and max_tail <= 100.0
            and first >= max_tail * 1.5
            and any(not float(value).is_integer() for value in clean[1:] if value is not None)
        ):
            clean = clean[1:]
            continue
        # Some balance-sheet rows have more than one leading note marker.
        if len(clean) >= 4 and all(_is_note_reference_value(value) for value in clean[:2]) and any(
            value >= 1_000.0 for value in tail
        ):
            clean = clean[1:]
            continue
        break
    return clean


def _choose_statement_value(
    values: list[float | None],
    *,
    prefer_non_usd_current: bool,
    current_period_first: bool = False,
    has_note_column: bool = False,
) -> float | None:
    raw = _strip_leading_note_values(list(values))
    if (
        has_note_column
        and len(raw) >= 3
        and raw[0] is not None
        and float(raw[0]).is_integer()
        and 0 < abs(raw[0]) <= 99
    ):
        raw = raw[1:]
    while len(raw) >= 4 and raw[-1] is not None and raw[-2] is not None:
        if abs(raw[-1]) < max(10.0, abs(raw[-2]) * 0.1):
            raw = raw[:-1]
            continue
        break
    if not any(value is not None for value in raw):
        return None
    if len(raw) == 1:
        return raw[0] if current_period_first else None
    if len(raw) == 2:
        return raw[0] if current_period_first else raw[-1]
    if current_period_first:
        return raw[0]
    if prefer_non_usd_current:
        return raw[-2]
    return raw[-1]


def _statement_unit_multiplier(header_lines: list[str], currency_unit: str | None) -> float:
    header_text = "\n".join(header_lines[:24])
    normalized_header = _normalize_label(" ".join(header_lines[:24]))
    if (
        "all amounts in" in normalized_header
        and "thousand" not in normalized_header
        and "million" not in normalized_header
        and "billion" not in normalized_header
        and any(
            marker in normalized_header
            for marker in (
                "u.s. dollars",
                "us dollars",
                "usd",
                "dollars",
                "rmb",
                "cny",
                "hkd",
                "hk$",
                "brl",
                "cad",
                "eur",
                "gbp",
                "jpy",
                "krw",
                "twd",
            )
        )
    ):
        return 1.0
    if (
        re.search(r"(?i)amounts? in thousands", header_text)
        or "in thousands" in normalized_header
        or re.search(r"(?i)\bthousands?\s+of\b", header_text)
        or "thousands of" in normalized_header
    ):
        return 1_000.0
    if (
        re.search(r"(?i)amounts? in millions", header_text)
        or "in millions" in normalized_header
        or re.search(r"(?i)\bmillions?\s+of\b", header_text)
        or "millions of" in normalized_header
    ):
        return 1_000_000.0
    if (
        re.search(r"(?i)amounts? in billions", header_text)
        or "in billions" in normalized_header
        or re.search(r"(?i)\bbillions?\s+of\b", header_text)
        or "billions of" in normalized_header
    ):
        return 1_000_000_000.0
    return _unit_multiplier(currency_unit)


def _find_row_value(
    rows: list[tuple[str, list[float | None]]],
    labels: list[str],
    *,
    prefer_non_usd_current: bool,
    current_period_first: bool = False,
    has_note_column: bool = False,
    reject_if_contains: list[str] | None = None,
) -> float | None:
    reject_if_contains = [_normalize_label(value) for value in (reject_if_contains or [])]
    normalized_rows = [(_normalize_label(label), values) for label, values in rows]
    for target in labels:
        norm_target = _normalize_label(target)
        for label, values in normalized_rows:
            if norm_target in label and not any(bad in label for bad in reject_if_contains):
                value = _choose_statement_value(
                    values,
                    prefer_non_usd_current=prefer_non_usd_current,
                    current_period_first=current_period_first,
                    has_note_column=has_note_column,
                )
                if value is not None:
                    return value
    return None


def _find_exact_row_value(
    rows: list[tuple[str, list[float | None]]],
    labels: list[str],
    *,
    prefer_non_usd_current: bool,
    current_period_first: bool = False,
    has_note_column: bool = False,
) -> float | None:
    normalized_targets = {_normalize_label(label) for label in labels}
    for label, values in rows:
        if _normalize_label(label) not in normalized_targets:
            continue
        value = _choose_statement_value(
            values,
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        )
        if value is not None:
            return value
    return None


def _find_periodic_line_value(
    section_lines: list[str],
    labels: list[str],
    *,
    current_period_first: bool = False,
    preferred_index: int | None = None,
    reject_if_contains: list[str] | None = None,
) -> float | None:
    reject_if_contains = [_normalize_label(value) for value in (reject_if_contains or [])]
    merged_lines = _merge_split_parenthetical_numbers(section_lines)
    normalized_targets = [_normalize_label(label) for label in labels]
    for idx, line in enumerate(merged_lines):
        normalized_line = _normalize_label(line)
        if not any(target in normalized_line for target in normalized_targets):
            continue
        if any(bad in normalized_line for bad in reject_if_contains):
            continue
        inline = _extract_inline_statement_row(line)
        if inline is not None:
            _, values = inline
            value = _choose_periodic_statement_value(
                values,
                current_period_first=current_period_first,
                preferred_index=preferred_index,
            )
            if value is not None:
                return value
        values: list[float] = []
        for follow in merged_lines[idx + 1 : idx + 10]:
            if _is_number_line(follow):
                parsed = _parse_number(follow)
                if parsed is not None:
                    values.append(parsed)
                continue
            if values:
                break
            if _normalize_label(follow):
                break
        value = _choose_periodic_statement_value(
            values,
            current_period_first=current_period_first,
            preferred_index=preferred_index,
        )
        if value is not None:
            return value
    return None


def _choose_periodic_statement_value(
    values: list[float | None],
    *,
    current_period_first: bool = False,
    has_note_column: bool = False,
    preferred_index: int | None = None,
) -> float | None:
    raw = [value for value in values if value is not None]
    if preferred_index is not None and len(raw) == 6 and preferred_index >= 4:
        clean = raw
    else:
        clean = _strip_leading_note_values(raw)
    if (
        has_note_column
        and len(clean) >= 3
        and float(clean[0]).is_integer()
        and 0 < abs(clean[0]) <= 99
    ):
        clean = clean[1:]
    if (
        len(clean) >= 3
        and float(clean[-1]).is_integer()
        and 0 < abs(clean[-1]) <= 99
        and abs(clean[-2]) >= max(1_000.0, abs(clean[-1]) * 100)
    ):
        clean = clean[:-1]
    if not clean:
        return None
    if preferred_index is not None and -len(clean) <= preferred_index < len(clean):
        return clean[preferred_index]
    if preferred_index is not None and preferred_index >= len(clean) and len(clean) >= 4:
        # Some year-end 6-K tables omit convenience-currency columns on selected rows.
        # When the header points at the annual current-period column but the row is compacted,
        # the last remaining numeric entry is usually the correct annual value.
        return clean[-1]
    if len(clean) == 1:
        return clean[0] if current_period_first else None
    if current_period_first:
        return clean[0]
    return clean[1]


def _find_periodic_row_entry(
    rows: list[tuple[str, list[float | None]]],
    labels: list[str],
    *,
    current_period_first: bool = False,
    has_note_column: bool = False,
    preferred_index: int | None = None,
    reject_if_contains: list[str] | None = None,
) -> tuple[str | None, float | None]:
    reject_if_contains = [_normalize_label(value) for value in (reject_if_contains or [])]
    normalized_rows = [(_normalize_label(label), values) for label, values in rows]
    for target in labels:
        norm_target = _normalize_label(target)
        for label, values in normalized_rows:
            if norm_target in label and not any(bad in label for bad in reject_if_contains):
                value = _choose_periodic_statement_value(
                    values,
                    current_period_first=current_period_first,
                    has_note_column=has_note_column,
                    preferred_index=preferred_index,
                )
                if value is not None:
                    return label, value
    return None, None


def _normalize_currency_column_token(token: str) -> str | None:
    normalized = _normalize_label(token)
    if normalized in {"usd", "us $", "us$"}:
        return "USD"
    if normalized == "krw":
        return "KRW"
    if normalized in {"cny", "rmb", "renminbi"}:
        return "CNY"
    if normalized in {"hkd", "hk$"}:
        return "HKD"
    if normalized in {"brl", "r$", "reais"}:
        return "BRL"
    if normalized in {"cad", "c$"}:
        return "CAD"
    if normalized == "eur":
        return "EUR"
    if normalized == "gbp":
        return "GBP"
    if normalized == "jpy":
        return "JPY"
    return None


def _infer_periodic_statement_column_indices(section_lines: list[str]) -> tuple[int | None, int | None]:
    header_prefix: list[str] = []
    for line in section_lines[:24]:
        if _extract_inline_statement_row(line) is not None:
            break
        if _is_number_line(line):
            compact = re.sub(r"[^0-9]", "", line)
            if re.fullmatch(r"\d{4}", compact):
                header_prefix.append(line)
                continue
            break
        header_prefix.append(line)

    best_currency_tokens: list[str] = []
    best_stacked_currency_tokens: list[str] = []
    current_stacked_currency_tokens: list[str] = []
    token_pattern = re.compile(r"(?i)(?:KRW|USD|US\$|CNY|RMB|HKD|HK\$|BRL|R\$|CAD|C\$|EUR|GBP|JPY)")
    for line in header_prefix:
        normalized_line = _normalize_label(line)
        tokens = [
            normalized
            for token in token_pattern.findall(line)
            if (normalized := _normalize_currency_column_token(token)) is not None
        ]
        if len(tokens) == 1:
            current_stacked_currency_tokens.extend(tokens)
        elif not tokens and current_stacked_currency_tokens and any(
            marker in normalized_line for marker in ("audited", "unaudited")
        ):
            pass
        else:
            if len(current_stacked_currency_tokens) > len(best_stacked_currency_tokens):
                best_stacked_currency_tokens = current_stacked_currency_tokens
            current_stacked_currency_tokens = []
        if len(tokens) > len(best_currency_tokens):
            best_currency_tokens = tokens
    if len(current_stacked_currency_tokens) > len(best_stacked_currency_tokens):
        best_stacked_currency_tokens = current_stacked_currency_tokens
    if len(best_stacked_currency_tokens) > len(best_currency_tokens):
        best_currency_tokens = best_stacked_currency_tokens

    if not best_currency_tokens:
        return None, None

    first_usd_index = next(
        (idx for idx, code in enumerate(best_currency_tokens) if code == "USD"),
        None,
    )
    if first_usd_index is None or first_usd_index == 0:
        return None, None
    if not any(code != "USD" for code in best_currency_tokens[:first_usd_index]):
        return None, None
    return first_usd_index - 1, first_usd_index


def _infer_periodic_statement_value_index(header_lines: list[str]) -> int | None:
    header_text = " ".join(header_lines[:24])
    normalized_header = _normalize_label(header_text)
    if (
        "three months ended" in normalized_header
        and "nine months ended" in normalized_header
    ):
        date_pattern = re.compile(
            r"(?i)\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|"
            r"Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}\b"
        )
        dates = [dt for match in date_pattern.finditer(header_text) if (dt := _parse_date(match.group(0))) is not None]
        if len(dates) == 4 and dates == sorted(dates):
            return 3
    if "three months ended" not in normalized_header or "nine months ended" not in normalized_header:
        year_tokens = re.findall(r"\b20\d{2}\b", header_text)
        if (
            re.search(r"(?i)\b(?:for (?:the )?)?(?:three months ended|quarter ended)\b", normalized_header)
            and re.search(r"(?i)\b(?:for (?:the )?)?year ended\b", normalized_header)
        ):
            if len(year_tokens) in {4, 6}:
                latest_year = max(int(token) for token in year_tokens)
                positions = [idx for idx, token in enumerate(year_tokens) if int(token) == latest_year]
                if positions:
                    return positions[-1]
            return None

    date_pattern = re.compile(
        r"(?i)\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|"
        r"Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}\b"
        r"|\b\d{1,2}-(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2,4}\b"
    )
    dates = [dt for match in date_pattern.finditer(header_text) if (dt := _parse_date(match.group(0))) is not None]
    if len(dates) < 4:
        return None

    latest_date = max(dates)
    positions = [idx for idx, dt in enumerate(dates) if dt == latest_date]
    if len(positions) >= 2:
        return positions[-1]
    if len(dates) == 5:
        return 2
    if len(dates) == 4:
        return 1
    return None


def _resolve_periodic_preferred_indices(
    current_non_usd_index: int | None,
    current_usd_index: int | None,
    header_value_index: int | None,
) -> tuple[int | None, int | None]:
    if header_value_index is None:
        return current_non_usd_index, current_usd_index
    if current_non_usd_index is None and current_usd_index is None:
        return header_value_index, header_value_index
    if (
        current_non_usd_index is not None
        and current_usd_index is not None
        and header_value_index >= current_usd_index
    ):
        offset = header_value_index - current_usd_index
        return current_non_usd_index + offset, current_usd_index + offset
    if current_non_usd_index is None:
        current_non_usd_index = header_value_index
    if current_usd_index is None:
        current_usd_index = header_value_index
    return current_non_usd_index, current_usd_index


def _extract_header_period_currency_flags(header_lines: list[str]) -> list[bool]:
    flags: list[bool] = []
    pending_date = False
    date_pattern = re.compile(
        r"(?i)\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|"
        r"Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}\b"
    )
    for raw_line in header_lines[:24]:
        line = _clean_text(raw_line)
        normalized = _normalize_label(line)
        if not normalized:
            continue
        has_date = bool(date_pattern.search(line))
        has_usd = "usd" in normalized or "us$" in normalized
        has_currency = has_usd or any(
            token in normalized for token in ("rmb", "cny", "hkd", "hk$", "eur", "gbp", "jpy", "twd", "krw", "brl")
        )
        if has_date:
            if has_currency:
                flags.append(has_usd)
                pending_date = False
            else:
                pending_date = True
            continue
        if pending_date and has_currency:
            flags.append(has_usd)
            pending_date = False
    if pending_date:
        flags.append(False)
    return flags


def _infer_repeated_periodic_subcolumn_indices(
    header_lines: list[str],
    rows: list[tuple[str, list[float | None]]],
) -> tuple[int | None, int | None]:
    period_flags = _extract_header_period_currency_flags(header_lines)
    if len(period_flags) < 2:
        return None, None

    compatible_lengths = [
        len([value for value in values if value is not None])
        for _, values in rows
        if len([value for value in values if value is not None]) >= len(period_flags) * 2
    ]
    if not compatible_lengths:
        return None, None

    length_counts: dict[int, int] = {}
    for length in compatible_lengths:
        if length % len(period_flags) != 0:
            continue
        group_size = length // len(period_flags)
        if group_size <= 1 or group_size > 4:
            continue
        length_counts[length] = length_counts.get(length, 0) + 1
    if not length_counts:
        return None, None

    value_count = max(length_counts.items(), key=lambda item: (item[1], item[0]))[0]
    group_size = value_count // len(period_flags)

    preferred_subindex = group_size - 1
    non_usd_group = next((idx for idx in range(len(period_flags) - 1, -1, -1) if not period_flags[idx]), None)
    usd_group = next((idx for idx in range(len(period_flags) - 1, -1, -1) if period_flags[idx]), None)
    non_usd_index = None if non_usd_group is None else non_usd_group * group_size + preferred_subindex
    usd_index = None if usd_group is None else usd_group * group_size + preferred_subindex
    return non_usd_index, usd_index


def _prioritize_periodic_sections(
    sections: list[tuple[list[str], list[str]]],
) -> list[tuple[list[str], list[str]]]:
    def _score(section: tuple[list[str], list[str]]) -> tuple[int, int]:
        header_text = _normalize_label(" ".join(section[0][:18]))
        if "for the year ended" in header_text:
            return (0, 0)
        if "for the nine months ended" in header_text:
            return (1, 0)
        if "three months ended" in header_text or "for the three months ended" in header_text:
            return (2, 0)
        return (3, 0)

    return sorted(sections, key=_score)


def _select_mixed_quarter_year_value(
    values: list[float | None],
    *,
    use_usd: bool = False,
    prefer_quarter_current: bool = False,
) -> float | None:
    clean = _strip_leading_note_values([value for value in values if value is not None])
    if (
        len(clean) == 7
        and float(clean[-1]).is_integer()
        and 0 < abs(clean[-1]) <= 99
    ):
        clean = clean[:-1]
    if len(clean) >= 7:
        preferred_index = 3 if use_usd else 2
    elif len(clean) == 6:
        preferred_index = (1 if use_usd else 0) if prefer_quarter_current else (5 if use_usd else 4)
    elif len(clean) == 5:
        preferred_index = 0 if prefer_quarter_current else 4
    elif len(clean) == 4:
        preferred_index = 0 if prefer_quarter_current else 2
    else:
        return None
    if preferred_index >= len(clean):
        return None
    return clean[preferred_index]


def _strip_basic_diluted_suffix(label: str) -> str:
    text = re.sub(r"(?i)[—–-]?\s*(basic|diluted)\b", "", label)
    return text.strip(" :")


def _attach_subrow_context(rows: list[tuple[str, list[float | None]]]) -> list[tuple[str, list[float | None]]]:
    contextual_rows: list[tuple[str, list[float | None]]] = []
    previous_label: str | None = None
    for label, values in rows:
        norm = _normalize_label(label)
        if previous_label and norm in {"basic", "diluted"}:
            contextual_rows.append((f"{_strip_basic_diluted_suffix(previous_label)} {label}", values))
        else:
            contextual_rows.append((label, values))
        previous_label = contextual_rows[-1][0]
    return contextual_rows


def _first_substantive_number(row: str) -> float | None:
    values: list[float] = []
    for token in re.findall(r"\(?-?\$?\d[\d,]*(?:\.\d+)?[KMBkmb]?\)?", row):
        parsed = _parse_number(token)
        if parsed is None:
            continue
        compact = token.replace(",", "").replace("$", "").replace("(", "").replace(")", "")
        if re.fullmatch(r"-?\d{4}", compact):
            try:
                year_like = abs(int(compact))
            except ValueError:
                year_like = 0
            if 1900 <= year_like <= 2100:
                continue
        values.append(parsed)
    if not values:
        return None
    if (
        len(values) == 1
        and float(values[0]).is_integer()
        and 0 < abs(values[0]) <= 99
        and re.fullmatch(r"\(?\s*\d{1,2}\s*\)?", row.strip())
    ):
        return None
    chosen_index = 0
    while chosen_index + 1 < len(values):
        current_value = abs(values[chosen_index])
        next_value = abs(values[chosen_index + 1])
        if current_value < max(10.0, next_value * 0.1):
            chosen_index += 1
            continue
        break
    return values[chosen_index]


def _extract_summary_table_metrics_6k(
    lines: list[str],
    currency_unit: str | None,
    ordinary_shares_per_depositary_share: float | None = None,
) -> dict[str, float | None]:
    labels: dict[str, list[str]] = {
        "revenue": [
            "total revenue and income from continuing operations",
            "total revenue and income",
            "total operating revenue",
            "total segment revenue",
            "total revenue",
            "revenue",
        ],
        "operating_income": ["operating income", "income from operations"],
        "pretax_income": [
            "profit (loss) before income taxes from continuing operations",
            "profit (loss) before income taxes",
            "profit / (loss) before income taxes",
            "profit / (loss) before income tax",
            "profit / (loss) before tax",
            "income before income taxes",
            "loss before income tax expense",
        ],
        "tax_expense": [
            "income tax and social contribution",
            "income tax expense",
            "income taxes",
        ],
        "net_income": [
            "net income attributable to shareholders of the company",
            "net income attributable to ordinary shareholders of the company",
            "net income attributable to controlling shareholders",
            "net income for the period",
            "net income (loss) for the year",
            "net profit / (loss)",
            "net loss",
            "net income",
        ],
        "weighted_avg_shares_basic": [
            "weighted average number of outstanding common shares",
            "weighted average number of outstanding shares",
            "weighted average number of shares outstanding",
            "weighted average ordinary shares outstanding",
        ],
        "weighted_avg_shares_diluted": [
            "weighted average number of common shares for diluted earnings per share",
            "diluted weighted average number of multiple and subordinate voting shares outstanding",
            "weighted average shares used in calculating net income per ordinary share diluted",
        ],
    }

    scan_limit = min(len(lines), 5000)
    statement_heading = re.compile(
        r"(?i)^(?:unaudited\s+)?(?:condensed\s+)?(?:combined and\s+)?(?:consolidated\s+)?(?:interim\s+)?"
        r"(?:balance sheets?|statements?\s+of\s+(?:operations|income|earnings|comprehensive income|comprehensive loss|cash flows|financial position|profit or loss(?:\s+and\s+other\s+comprehensive\s+income)?))\b"
    )
    statement_start = next(
        (
            idx
            for idx, line in enumerate(lines[:scan_limit])
            if statement_heading.search(_statement_search_text(line))
        ),
        None,
    )
    initial_scan_lines = lines[:statement_start] if statement_start is not None else lines[:scan_limit]
    summary_anchor = next(
        (
            idx
            for idx, line in enumerate(initial_scan_lines)
            if re.search(
                r"(?i)^(?:as at or for|for)\s+the\s+(?:three|nine|twelve)\s+months?\s+ended\b|"
                r"^(?:as at or for|for)\s+the\s+year\s+ended\b",
                _normalize_label(line),
            )
            and any(
                target in _normalize_label(follow)
                for follow in initial_scan_lines[idx + 1 : idx + 16]
                for target in (
                    "total revenue",
                    "net income",
                    "income before income taxes",
                    "weighted average",
                )
            )
        ),
        None,
    )
    scan_lines = initial_scan_lines[summary_anchor:] if summary_anchor is not None else initial_scan_lines
    metrics: dict[str, float | None] = {}
    for metric, metric_labels in labels.items():
        value = None
        matched_line = None
        matched_scale = _unit_multiplier(currency_unit)
        for idx, line in enumerate(scan_lines):
            norm_line = _normalize_label(line)
            matched_label = next((label for label in metric_labels if norm_line.startswith(_normalize_label(label))), None)
            if matched_label is None:
                continue
            normalized_label = _normalize_label(matched_label)
            if normalized_label in {"revenue", "net income", "net loss"} and norm_line != normalized_label:
                continue
            if len(re.findall(r"[A-Za-z]{3,}", line)) > 16:
                continue
            if metric == "revenue" and matched_label == "revenue" and norm_line.startswith("revenue from"):
                continue
            window = [line]
            window.extend(scan_lines[idx + 1 : idx + 5])
            for row in window:
                parsed = _first_substantive_number(row)
                if parsed is not None:
                    value = parsed
                    matched_line = line
                    matched_scale = _statement_unit_multiplier(
                        scan_lines[max(0, idx - 30) : min(len(scan_lines), idx + 12)],
                        currency_unit,
                    )
                    break
            if value is not None:
                norm_match = _normalize_label(matched_label)
                if ("loss before" in norm_match or norm_match == "net loss") and value > 0:
                    value *= -1
                break

        if value is None and metric == "tax_expense":
            current_tax = None
            deferred_tax = None
            for idx, line in enumerate(scan_lines):
                norm_line = _normalize_label(line)
                if norm_line.startswith("current income tax and social contribution") and current_tax is None:
                    for row in [line, *scan_lines[idx + 1 : idx + 3]]:
                        parsed = _first_substantive_number(row)
                        if parsed is not None:
                            current_tax = parsed
                            break
                if norm_line.startswith("deferred income tax and social contribution") and deferred_tax is None:
                    for row in [line, *scan_lines[idx + 1 : idx + 3]]:
                        parsed = _first_substantive_number(row)
                        if parsed is not None:
                            deferred_tax = parsed
                            break
            if current_tax is not None and deferred_tax is not None:
                value = current_tax + deferred_tax

        if value is None:
            metrics[metric] = None
            continue

        if metric in {"weighted_avg_shares_basic", "weighted_avg_shares_diluted"}:
            value = _normalize_depositary_metric(metric, matched_line, value, ordinary_shares_per_depositary_share)
            metrics[metric] = value
        else:
            metrics[metric] = value * matched_scale
    return metrics


def _extract_share_based_compensation_6k(
    lines: list[str],
    currency_unit: str | None,
) -> float | None:
    starts = [
        idx
        for idx, line in enumerate(lines)
        if re.search(
            r"(?i)(?:unaudited reconciliations?(?:\s+of)?\s+gaap\s+and\s+non-gaap(?:\s+results)?|reconciliations?\s+of\s+unaudited\s+non-gaap\s+results)",
            _statement_search_text(" ".join(lines[idx : idx + 2])),
        )
    ]
    if not starts:
        return None

    prefer_quarter_current_mixed = _prefer_quarter_current_mixed_6k(lines)
    for pos, start in enumerate(starts):
        end = starts[pos + 1] if pos + 1 < len(starts) else min(len(lines), start + 240)
        block = lines[start:end]
        if not block:
            continue
        current_non_usd_index, current_usd_index = _infer_periodic_statement_column_indices(block)
        header_value_index = _infer_periodic_statement_value_index(block[:24])
        preferred_index, _ = _resolve_periodic_preferred_indices(
            current_non_usd_index,
            current_usd_index,
            header_value_index,
        )
        if (
            prefer_quarter_current_mixed
            and _is_mixed_quarter_year_statement_header(block[:24])
            and preferred_index is not None
            and preferred_index >= 2
        ):
            preferred_index = 0
        normalized_block_header = _normalize_label(" ".join(block[:24]))
        if preferred_index is None and normalized_block_header.count("for the three months ended") >= 3:
            preferred_index = -1
        value = _find_periodic_line_value(
            block,
            [
                "add share-based compensation expenses",
                "share-based compensation expenses",
                "stock-based compensation expenses",
                "share-based compensation",
                "stock-based compensation",
            ],
            current_period_first=_statement_current_period_first(block[:24]),
            preferred_index=preferred_index,
        )
        if value is None:
            continue
        return value * _statement_unit_multiplier(block[:24], currency_unit)
    return None


def _extract_reconciliation_metric_6k(
    lines: list[str],
    currency_unit: str | None,
    labels: list[str],
) -> float | None:
    starts = [
        idx
        for idx, line in enumerate(lines)
        if re.search(
            r"(?i)(?:unaudited reconciliations?(?:\s+of)?\s+gaap\s+and\s+non-gaap(?:\s+results)?|reconciliations?\s+of\s+unaudited\s+non-gaap\s+results)",
            _statement_search_text(" ".join(lines[idx : idx + 2])),
        )
    ]
    if not starts:
        return None

    prefer_quarter_current_mixed = _prefer_quarter_current_mixed_6k(lines)
    for pos, start in enumerate(starts):
        end = starts[pos + 1] if pos + 1 < len(starts) else min(len(lines), start + 240)
        block = lines[start:end]
        if not block:
            continue
        current_non_usd_index, current_usd_index = _infer_periodic_statement_column_indices(block)
        header_value_index = _infer_periodic_statement_value_index(block[:24])
        preferred_index, _ = _resolve_periodic_preferred_indices(
            current_non_usd_index,
            current_usd_index,
            header_value_index,
        )
        normalized_block_header = _normalize_label(" ".join(block[:24]))
        if preferred_index is None and normalized_block_header.count("for the three months ended") >= 3:
            preferred_index = -1
        value = _find_periodic_line_value(
            block,
            labels,
            current_period_first=_statement_current_period_first(block[:24]),
            preferred_index=preferred_index,
        )
        if (
            value is None
            and _is_mixed_quarter_year_statement_header(block[:24])
            and preferred_index is None
        ):
            for label, values in _extract_rows_from_statement(block):
                normalized_label = _normalize_label(label)
                if not any(_normalize_label(target) in normalized_label for target in labels):
                    continue
                value = _select_mixed_quarter_year_value(
                    values,
                    prefer_quarter_current=prefer_quarter_current_mixed,
                )
                if value is not None:
                    break
        if value is None:
            continue
        return value * _statement_unit_multiplier(block[:24], currency_unit)
    return None


def _extract_income_statement_metrics_6k(
    lines: list[str],
    currency_unit: str | None,
    ordinary_shares_per_depositary_share: float | None = None,
) -> dict[str, float | None]:
    sections = _statement_blocks(
        lines,
        r"(?i)^(?:unaudited\s+)?(?:condensed\s+)?(?:combined and\s+)?(?:consolidated\s+)?(?:interim\s+)?(?:(?:statements?\s+of\s+(?:operations(?:\s+and\s+comprehensive income(?:\s*\(loss\))?)?|income(?:\s+and\s+comprehensive income(?:\s*\(loss\))?)?|profit or loss(?:\s+and\s+other\s+comprehensive\s+income)?|earnings|comprehensive income(?:\s*\(loss\))?|comprehensive loss|income and comprehensive income))|(?:(?:operations(?:\s+and\s+comprehensive income(?:\s*\(loss\))?)?|income(?:\s+and\s+comprehensive income(?:\s*\(loss\))?)?|profit or loss(?:\s+and\s+other\s+comprehensive\s+income)?|earnings|comprehensive income(?:\s*\(loss\))?|comprehensive loss|income and comprehensive income)\s+statements?))(?:\s+for\b|\s*\(|\s+(?:three|six|nine|twelve)\s+months?\s+ended\b|\s+year\s+ended\b|\s+as\s+of\b|\s*$)",
        [
            r"(?i)(balance sheets?|statements?\s+of\s+financial position)",
            r"(?i)statements?\s+of\s+other comprehensive income",
            r"(?i)statements?\s+of\s+changes in equity",
            r"(?i)statements?\s+of\s+cash flows",
            r"(?i)notes to consolidated financial statements",
            r"(?i)report of independent registered public accounting firm",
            r"(?i)reconciliation of",
        ],
    )
    if not sections:
        return {}
    sections = _prioritize_periodic_sections(sections)

    statement_scale = _statement_unit_multiplier(sections[0][0], currency_unit)
    header_text = "\n".join("\n".join(header) for header, _ in sections)
    prefer_non_usd_current = bool(
        ("US$" in header_text or re.search(r"(?i)\bUSD\b", header_text))
        and currency_unit is not None
        and not currency_unit.upper().startswith("USD")
    )
    current_period_first = any(_statement_current_period_first(header) for header, _ in sections)
    has_note_column = any(_normalize_label(line).startswith("note") for header, _ in sections for line in header[:24])
    current_non_usd_index = None
    current_usd_index = None
    header_value_index = None
    prefer_q4_current_mixed = _has_quarter_full_year_gaap_highlight_table(lines)
    prefer_quarter_current_mixed = _prefer_quarter_current_mixed_6k(lines)
    for header, body in sections:
        inferred_non_usd_index, inferred_usd_index = _infer_periodic_statement_column_indices(body)
        if current_non_usd_index is None and inferred_non_usd_index is not None:
            current_non_usd_index = inferred_non_usd_index
        if current_usd_index is None and inferred_usd_index is not None:
            current_usd_index = inferred_usd_index
        if header_value_index is None:
            header_value_index = _infer_periodic_statement_value_index(header)
    current_non_usd_index, current_usd_index = _resolve_periodic_preferred_indices(
        current_non_usd_index,
        current_usd_index,
        header_value_index,
    )

    section_payloads: list[dict[str, object]] = []
    section_rows: list[list[tuple[str, list[float | None]]]] = []
    for header, body in sections:
        extracted = _extract_rows_from_statement(body)
        if extracted:
            section_non_usd_index, section_usd_index = _infer_periodic_statement_column_indices(body)
            section_header_value_index = _infer_periodic_statement_value_index(header)
            section_non_usd_index, section_usd_index = _resolve_periodic_preferred_indices(
                section_non_usd_index,
                section_usd_index,
                section_header_value_index,
            )
            if (
                prefer_quarter_current_mixed
                and "three months ended" in _normalize_label(" ".join(header[:24]))
                and "year ended" in _normalize_label(" ".join(header[:24]))
                and section_non_usd_index is not None
                and section_usd_index == section_non_usd_index
                and section_non_usd_index >= 2
            ):
                section_non_usd_index = 0
                section_usd_index = 0
            if (
                prefer_q4_current_mixed
                and "three months ended" in _normalize_label(" ".join(header[:24]))
                and "year ended" in _normalize_label(" ".join(header[:24]))
                and section_non_usd_index is not None
                and section_non_usd_index >= 4
                and section_usd_index == section_non_usd_index
            ):
                section_non_usd_index = 2
                section_usd_index = 2
            attached_rows = _attach_subrow_context(extracted)
            section_payloads.append(
                {
                    "rows": attached_rows,
                    "scale": _statement_unit_multiplier(header, currency_unit),
                    "current_period_first": _statement_current_period_first(header),
                    "has_note_column": any(_normalize_label(line).startswith("note") for line in header[:24]),
                    "mixed_three_nine": (
                        "three months ended" in _normalize_label(" ".join(header[:24]))
                        and "nine months ended" in _normalize_label(" ".join(header[:24]))
                    ),
                    "mixed_quarter_year": (
                        "three months ended" in _normalize_label(" ".join(header[:24]))
                        and "year ended" in _normalize_label(" ".join(header[:24]))
                    ),
                    "preferred_index": section_non_usd_index,
                    "usd_preferred_index": section_usd_index,
                }
            )
            section_rows.append(extracted)
    qualifying_sections = [
        extracted
        for extracted in section_rows
        if len(extracted) >= 5
        and any(
            key in _normalize_label(label)
            for label, _ in extracted
            for key in (
                "total revenue",
                "revenue and income",
                "profit loss before income taxes",
                "net income",
                "cost of services",
                "administrative expenses",
                "selling expenses",
            )
        )
    ]
    rows: list[tuple[str, list[float | None]]] = []
    for extracted in (qualifying_sections or section_rows):
        rows.extend(extracted)
    rows = _attach_subrow_context(rows)
    qualifying_payload_rows = {
        tuple((label, tuple(values)) for label, values in _attach_subrow_context(extracted))
        for extracted in (qualifying_sections or section_rows)
    }
    active_section_payloads = [
        payload
        for payload in section_payloads
        if tuple((label, tuple(values)) for label, values in payload["rows"]) in qualifying_payload_rows
    ] or section_payloads

    def _find_section_metric(
        labels: list[str],
        *,
        reject_if_contains: list[str] | None = None,
        use_usd_column: bool = False,
    ) -> tuple[str | None, float | None, float]:
        for payload in active_section_payloads:
            preferred_index = payload["usd_preferred_index"] if use_usd_column else payload["preferred_index"]
            if payload["mixed_three_nine"]:
                reject_norm = [_normalize_label(value) for value in (reject_if_contains or [])]
                normalized_rows = [(_normalize_label(label), label, values) for label, values in payload["rows"]]
                target_norms = [_normalize_label(target) for target in labels]
                for target in labels:
                    norm_target = _normalize_label(target)
                    for normalized_label, label, values in normalized_rows:
                        if norm_target not in normalized_label or any(bad in normalized_label for bad in reject_norm):
                            continue
                        if "weighted average" in normalized_label and not any(
                            "weighted average" in target_norm or "shares used in computing" in target_norm
                            for target_norm in target_norms
                        ):
                            continue
                        clean = _strip_leading_note_values([value for value in values if value is not None])
                        mixed_index = preferred_index if preferred_index is not None else (2 if use_usd_column else 1)
                        if mixed_index < len(clean):
                            return label, clean[mixed_index], float(payload["scale"])
            if payload["mixed_quarter_year"] and preferred_index is None:
                reject_norm = [_normalize_label(value) for value in (reject_if_contains or [])]
                normalized_rows = [(_normalize_label(label), label, values) for label, values in payload["rows"]]
                for target in labels:
                    norm_target = _normalize_label(target)
                    for normalized_label, label, values in normalized_rows:
                        if norm_target not in normalized_label or any(bad in normalized_label for bad in reject_norm):
                            continue
                        selected = _select_mixed_quarter_year_value(
                            values,
                            use_usd=use_usd_column,
                            prefer_quarter_current=prefer_quarter_current_mixed,
                        )
                        if selected is not None:
                            return label, selected, float(payload["scale"])
            row_label, value = _find_periodic_row_entry(
                payload["rows"],
                labels,
                current_period_first=bool(payload["current_period_first"]),
                has_note_column=bool(payload["has_note_column"]),
                preferred_index=preferred_index,
                reject_if_contains=reject_if_contains,
            )
            if value is not None:
                return row_label, value, float(payload["scale"])
        return None, None, statement_scale

    def _find_mixed_year_weighted_share_metric(labels: list[str]) -> tuple[str | None, float | None, float]:
        normalized_targets = [_normalize_label(label) for label in labels]
        for payload in active_section_payloads:
            if not payload["mixed_quarter_year"] or payload["preferred_index"] is not None:
                continue
            for label, values in payload["rows"]:
                normalized_label = _normalize_label(label)
                if not any(target in normalized_label for target in normalized_targets):
                    continue
                selected = _select_mixed_quarter_year_value(
                    values,
                    prefer_quarter_current=prefer_quarter_current_mixed,
                )
                if selected is not None:
                    return label, selected, float(payload["scale"])
        return None, None, statement_scale

    metrics: dict[str, float | None] = {}
    row_label: str | None

    label_map: dict[str, list[str]] = {
        "revenue": [
            "total revenue and income from continuing operations",
            "total revenue and income",
            "total operating revenue",
            "total revenues",
            "total revenue",
            "total net revenues",
            "total net revenue",
            "net revenues",
            "net sales",
            "revenues",
            "revenue",
        ],
        "cogs": [
            "total cost of sales",
            "costs of revenues",
            "cost of revenues",
            "cost of revenue",
            "collaboration cost of revenue",
            "cost of license and other revenue",
            "cost of sales",
            "cost of services",
            "total cost of financial and transactional services provided",
        ],
        "gross_profit": ["gross profit"],
        "operating_income": [
            "income from operations",
            "profit from operations",
            "profit/ loss from operations",
            "loss/ profit from operations",
            "loss from operations",
            "operating loss",
            "operating profit",
            "operating income",
            "total operating expenses income",
            "total operating expense income",
        ],
        "research_and_development": [
            "research and development expenses",
            "research and development",
            "technology and development expenses",
            "technology and development",
            "technology and content expenses",
        ],
        "selling_and_marketing": [
            "sales and marketing",
            "selling and marketing",
            "marketing",
            "marketing and branding",
            "marketing expenses",
            "selling expenses",
            "selling and distribution expenses",
        ],
        "general_and_administrative": [
            "general and administrative",
            "general and administrative expenses",
            "administrative expenses",
        ],
        "sga": [
            "selling, general and administrative expenses",
            "selling, general and administrative",
        ],
        "pretax_income": [
            "profit (loss) before income taxes from continuing operations",
            "profit (loss) before income taxes",
            "profit / (loss) before income taxes",
            "profit / (loss) before income tax",
            "profit / (loss) before tax",
            "profit/ loss before income taxes",
            "loss/ profit before income taxes",
            "income loss before income taxes",
            "income/ loss before tax",
            "loss/ income before tax",
            "loss /income before tax",
            "loss / income before tax",
            "income before income tax expense and share of income of equity method investees",
            "income before income tax expense",
            "income before income tax",
            "income before income taxes",
            "profit before income tax and share of results of equity investees",
            "loss before provision for income tax and share of results of equity investees",
            "profit before income tax",
            "profit/ loss before income tax",
            "loss/ profit before income tax",
            "loss before income tax expense",
            "loss before income taxes",
        ],
        "tax_expense": [
            "income tax and social contribution",
            "income taxes expense",
            "income tax expense",
            "income tax expenses",
            "income tax expense/ benefit",
            "income tax benefit/ expense",
            "income tax benefit/(expense)",
            "income tax expense/(benefit)",
            "income tax benefit",
            "income taxes",
        ],
        "net_income": [
            "net income loss attributable to ordinary shareholders",
            "net income loss attributable to the company",
            "net income attributable to ordinary shareholders",
            "net income attributable to the company",
            "net profit attributable to ordinary shareholders",
            "net profit attributable to the company's ordinary shareholders",
            "profit attributable to the company's ordinary shareholders",
            "net income attributable to shareholders of the parent",
            "profit attributable to shareholders of the parent",
            "profit attributable to owners of the parent",
            "net income/ loss attributable to the company's ordinary shareholders",
            "net loss/ income attributable to the company's ordinary shareholders",
            "net income attributable to vipshop's shareholders",
            "net income attributable to shareholders of the company",
            "net income attributable to controlling shareholders",
            "net profit/ loss attributable to the bilibili inc.'s shareholders",
            "net loss/ profit attributable to the bilibili inc.'s shareholders",
            "net profit/ loss attributable to shareholders",
            "net loss/ profit attributable to shareholders",
            "net loss attributable to ordinary shareholders",
            "net loss",
            "owners of parent company",
            "net income loss",
            "net income attributable to shareholders",
            "net income (loss) for the year",
            "profit for the interim period/year",
            "profit for the interim period",
            "profit for the year",
            "net profit / (loss)",
            "net (loss)/income",
            "net loss /income",
            "net loss / income",
            "net profit/ loss",
            "net loss/ profit",
            "net income",
        ],
        "interest_income": [
            "interest income, net",
            "interest related income",
            "interest income",
            "financing income",
            "investment income",
            "finance income",
            "financial income",
        ],
        "eps_basic": [
            "net income loss per share attributable to ordinary shareholders basic",
            "net profit/ loss per ads basic",
            "net loss/ profit per ads basic",
            "net income per ordinary share basic",
            "earnings per ordinary share basic",
            "earnings per share basic",
            "net income per share basic",
            "basic earnings per share",
            "basic earnings per common share",
        ],
        "eps_diluted": [
            "net income loss per share attributable to ordinary shareholders diluted",
            "net profit/ loss per ads diluted",
            "net loss/ profit per ads diluted",
            "net income per ordinary share diluted",
            "earnings per ordinary share diluted",
            "earnings per share diluted",
            "net income per share diluted",
            "diluted earnings per share",
            "diluted earnings per common share",
        ],
        "weighted_avg_shares_basic": [
            "weighted average shares outstanding basic",
            "weighted average number of ordinary shares basic",
            "weighted average number of ads basic",
            "weighted average ordinary shares used in calculating net income per ordinary share basic",
            "weighted average shares used in calculating net income per ordinary share basic",
            "weighted average number of shares outstanding used in computing earnings per ordinary share basic",
            "weighted average number of ordinary shares/adss used in computing net loss per share/ads basic",
            "weighted-average number of ordinary shares outstanding (in thousands): basic",
            "weighted average number of multiple and subordinate voting shares outstanding basic",
            "weighted average number of outstanding common shares",
            "weighted average number of outstanding shares",
            "weighted average ordinary shares outstanding basic",
            "weighted average number of class a and class b ordinary shares basic",
            "shares used in computing basic net loss /income per ads",
            "shares used in computing basic net income per ads",
            "shares used in computing basic net income per share attributable to weibo's shareholders",
        ],
        "weighted_avg_shares_diluted": [
            "weighted average shares outstanding diluted",
            "weighted average number of ordinary shares diluted",
            "weighted average number of ads diluted",
            "weighted average ordinary shares used in calculating net income per ordinary share diluted",
            "weighted average shares used in calculating net income per ordinary share diluted",
            "weighted average number of shares outstanding used in computing earnings per ordinary share diluted",
            "weighted average number of ordinary shares/adss used in computing net loss per share/ads diluted",
            "weighted-average number of ordinary shares outstanding (in thousands): diluted",
            "diluted weighted average number of multiple and subordinate voting shares outstanding",
            "weighted average ordinary shares outstanding diluted",
            "weighted average number of class a and class b ordinary shares diluted",
            "denominator of diluted eps from continuing and discontinued operations",
            "shares used in computing diluted net loss /income per ads",
            "shares used in computing diluted net income per ads",
            "shares used in computing diluted net income per share attributable to weibo's shareholders",
        ],
    }

    for metric, labels in label_map.items():
        if metric == "tax_expense":
            continue
        reject_if_contains = ["non-gaap"] if metric not in {"weighted_avg_shares_basic", "weighted_avg_shares_diluted"} else []
        if metric == "net_income":
            reject_if_contains = [*reject_if_contains, "mezzanine", "non controlling interests", "non-controlling interests"]
        row_label, value, metric_scale = _find_section_metric(
            labels,
            reject_if_contains=reject_if_contains,
        )
        if metric == "selling_and_marketing" and row_label and "general and administrative" in row_label:
            row_label, value = None, None
        if metric == "general_and_administrative" and row_label and "selling" in row_label:
            row_label, value = None, None
        if metric == "net_income" and row_label and "before income tax" in row_label:
            row_label, value = None, None
        if metric in {"eps_basic", "eps_diluted"} and row_label and "weighted average" in row_label:
            row_label, value = None, None
        if value is None:
            metrics[metric] = None
            continue
        if metric in {"weighted_avg_shares_basic", "weighted_avg_shares_diluted"} and row_label and "thousand" in row_label:
            value *= 1_000.0
        elif metric in {"weighted_avg_shares_basic", "weighted_avg_shares_diluted"} and abs(value) < 1_000_000:
            if float(statement_scale) == 1_000.0:
                value *= 1_000.0
            elif metric_scale >= 1_000_000.0:
                value *= metric_scale
        if metric in {"weighted_avg_shares_basic", "weighted_avg_shares_diluted", "eps_basic", "eps_diluted"}:
            value = _normalize_depositary_metric(metric, row_label, value, ordinary_shares_per_depositary_share)
        elif metric not in {"eps_basic", "eps_diluted", "weighted_avg_shares_basic", "weighted_avg_shares_diluted"}:
            value *= metric_scale
        metrics[metric] = value

    if metrics.get("weighted_avg_shares_basic") is None:
        row_label, value, metric_scale = _find_section_metric(
            [
                "shares used in computing basic net income per share attributable to weibo's shareholders",
                "shares used in computing basic net income per share",
            ],
        )
        if value is not None:
            if abs(value) < 1_000_000:
                if float(statement_scale) == 1_000.0:
                    value *= 1_000.0
                elif metric_scale >= 1_000_000.0:
                    value *= metric_scale
            metrics["weighted_avg_shares_basic"] = _normalize_depositary_metric(
                "weighted_avg_shares_basic",
                row_label,
                value,
                ordinary_shares_per_depositary_share,
            )

    if metrics.get("weighted_avg_shares_diluted") is None:
        row_label, value, metric_scale = _find_section_metric(
            [
                "shares used in computing diluted net income per share attributable to weibo's shareholders",
                "shares used in computing diluted net income per share",
            ],
        )
        if value is not None:
            if abs(value) < 1_000_000:
                if float(statement_scale) == 1_000.0:
                    value *= 1_000.0
                elif metric_scale >= 1_000_000.0:
                    value *= metric_scale
            metrics["weighted_avg_shares_diluted"] = _normalize_depositary_metric(
                "weighted_avg_shares_diluted",
                row_label,
                value,
                ordinary_shares_per_depositary_share,
            )

    if metrics.get("cogs") is not None:
        for payload in active_section_payloads:
            component_values: list[float] = []
            saw_total_cost_row = False
            for label, values in payload["rows"]:
                norm = _normalize_label(label)
                if "total cost" in norm and "revenue" in norm:
                    saw_total_cost_row = True
                if "cost of" not in norm or "revenue" not in norm:
                    continue
                if "included in" in norm or "share based compensation" in norm or "stock based compensation" in norm:
                    continue
                value = _choose_periodic_statement_value(
                    values,
                    current_period_first=bool(payload["current_period_first"]),
                    has_note_column=bool(payload["has_note_column"]),
                    preferred_index=payload["preferred_index"],
                )
                if value is None:
                    continue
                component_values.append(value * float(payload["scale"]))
            if not saw_total_cost_row and len(component_values) >= 2:
                summed_cogs = sum(component_values)
                metrics["cogs"] = summed_cogs
                break

    tax_row_label, tax_value, tax_scale = _find_section_metric(
        label_map["tax_expense"],
        reject_if_contains=[
            "income before",
            "before income tax",
            "other comprehensive",
            "nil income taxes",
            "current income tax and social contribution",
            "deferred income tax and social contribution",
            "tax expense at the statutory rate",
            "effective tax rate",
        ],
    )
    if tax_value is None:
        tax_row_label, tax_value, tax_scale = _find_section_metric(
            ["current income tax and social contribution", "current income tax expense", "current income taxes"],
            reject_if_contains=["other comprehensive", "effective tax rate"],
        )
    if tax_value is not None:
        normalized_tax_label = _normalize_label(tax_row_label or "")
        benefit_idx = normalized_tax_label.find("benefit")
        expense_idx = normalized_tax_label.find("expense")
        if benefit_idx != -1 and (expense_idx == -1 or benefit_idx < expense_idx):
            tax_value = -abs(tax_value)
    metrics["tax_expense"] = tax_value * tax_scale if tax_value is not None else None

    if metrics.get("weighted_avg_shares_basic") is None:
        row_label, value, metric_scale = _find_section_metric(
            [
                "weighted average ordinary shares used in calculating net income per ordinary share",
                "weighted average shares used in calculating net income per ordinary share",
                "weighted-average number of ordinary shares outstanding (in thousands): basic",
                "weighted average number of class a and class b ordinary shares basic",
            ],
        )
        if value is not None:
            if row_label and "thousand" in row_label:
                value *= 1_000.0
            elif abs(value) < 1_000_000 and metric_scale >= 1_000_000.0:
                value *= metric_scale
            value = _normalize_depositary_metric(
                "weighted_avg_shares_basic",
                row_label,
                value,
                ordinary_shares_per_depositary_share,
            )
            metrics["weighted_avg_shares_basic"] = value

    if metrics.get("weighted_avg_shares_diluted") is None:
        row_label, value, metric_scale = _find_section_metric(
            [
                "weighted average ads used in calculating net income per ordinary share for both gaap and non-gaap eps - diluted",
                "weighted average ordinary shares used in calculating net income per ordinary share diluted",
                "weighted average number of class a and class b ordinary shares diluted",
            ],
        )
        if value is not None:
            if abs(value) < 1_000_000 and metric_scale >= 1_000_000.0:
                value *= metric_scale
            value = _normalize_depositary_metric(
                "weighted_avg_shares_diluted",
                row_label,
                value,
                ordinary_shares_per_depositary_share,
            )
            metrics["weighted_avg_shares_diluted"] = value

    if metrics.get("weighted_avg_shares_basic") is None or metrics.get("weighted_avg_shares_diluted") is None:
        row_label, value, metric_scale = _find_section_metric(
            [
                "shares used in the net loss per share computation",
                "shares used in the net income per share computation",
                "shares used in the net loss per ads computation",
                "shares used in the net income per ads computation",
            ],
        )
        if value is not None:
            if abs(value) < 1_000_000:
                if float(statement_scale) == 1_000.0:
                    value *= 1_000.0
                elif metric_scale >= 1_000_000.0:
                    value *= metric_scale
            normalized_value = _normalize_depositary_metric(
                "weighted_avg_shares_basic",
                row_label,
                value,
                ordinary_shares_per_depositary_share,
            )
            if metrics.get("weighted_avg_shares_basic") is None:
                metrics["weighted_avg_shares_basic"] = normalized_value
            if metrics.get("weighted_avg_shares_diluted") is None:
                metrics["weighted_avg_shares_diluted"] = normalized_value

    for metric, labels in (
        ("weighted_avg_shares_basic", label_map["weighted_avg_shares_basic"]),
        ("weighted_avg_shares_diluted", label_map["weighted_avg_shares_diluted"]),
    ):
        row_label, value, metric_scale = _find_mixed_year_weighted_share_metric(labels)
        if value is None:
            continue
        if row_label and "thousand" in row_label:
            value *= 1_000.0
        elif abs(value) < 1_000_000 and metric_scale >= 1_000_000.0:
            value *= metric_scale
        metrics[metric] = _normalize_depositary_metric(
            metric,
            row_label,
            value,
            ordinary_shares_per_depositary_share,
        )

    if metrics.get("weighted_avg_shares_basic") is None or metrics.get("weighted_avg_shares_diluted") is None:
        for payload in active_section_payloads:
            weighted_rows = [
                row
                for row in payload["rows"]
                if "weighted average" in _normalize_label(row[0]) or "weighted-average" in _normalize_label(row[0])
            ]
            for label, values in weighted_rows:
                norm = _normalize_label(label)
                if payload["mixed_three_nine"]:
                    clean = _strip_leading_note_values([value for value in values if value is not None])
                    value = clean[1] if len(clean) > 1 else None
                else:
                    value = _choose_periodic_statement_value(
                        values,
                        current_period_first=bool(payload["current_period_first"]),
                        has_note_column=bool(payload["has_note_column"]),
                        preferred_index=payload["preferred_index"],
                    )
                if value is None:
                    continue
                if "thousand" in norm:
                    value *= 1_000.0
                elif abs(value) < 1_000_000 and float(payload["scale"]) >= 1_000_000.0:
                    value *= float(payload["scale"])
                if metrics.get("weighted_avg_shares_basic") is None and "basic" in norm:
                    metrics["weighted_avg_shares_basic"] = _normalize_depositary_metric(
                        "weighted_avg_shares_basic",
                        label,
                        value,
                        ordinary_shares_per_depositary_share,
                    )
                if metrics.get("weighted_avg_shares_diluted") is None and "diluted" in norm:
                    metrics["weighted_avg_shares_diluted"] = _normalize_depositary_metric(
                        "weighted_avg_shares_diluted",
                        label,
                        value,
                        ordinary_shares_per_depositary_share,
                    )
            if metrics.get("weighted_avg_shares_basic") is not None and metrics.get("weighted_avg_shares_diluted") is not None:
                break

    if ordinary_shares_per_depositary_share is None and (metrics.get("eps_basic") is None or metrics.get("eps_diluted") is None):
        carry_eps_context = False
        for payload in active_section_payloads:
            for label, values in payload["rows"]:
                norm = _normalize_label(label)
                if "non gaap" in norm:
                    carry_eps_context = False
                    continue
                if "per share" in norm or "earnings per share" in norm or "per ordinary share" in norm:
                    carry_eps_context = True
                if not carry_eps_context or "weighted average" in norm or "number of shares" in norm:
                    continue
                value = _choose_periodic_statement_value(
                    values,
                    current_period_first=bool(payload["current_period_first"]),
                    has_note_column=bool(payload["has_note_column"]),
                    preferred_index=payload["usd_preferred_index"],
                )
                if not _is_plausible_per_share_value(value):
                    continue
                if metrics.get("eps_basic") is None and "basic" in norm:
                    metrics["eps_basic"] = _normalize_depositary_metric(
                        "eps_basic",
                        label,
                        value,
                        ordinary_shares_per_depositary_share,
                    )
                if metrics.get("eps_diluted") is None and "diluted" in norm:
                    metrics["eps_diluted"] = _normalize_depositary_metric(
                        "eps_diluted",
                        label,
                        value,
                        ordinary_shares_per_depositary_share,
                    )
                if metrics.get("eps_basic") is not None and metrics.get("eps_diluted") is not None:
                    break
            if metrics.get("eps_basic") is not None and metrics.get("eps_diluted") is not None:
                break

    if metrics.get("revenue") is None and prefer_non_usd_current:
        row_label, value = _find_periodic_row_entry(
            rows,
            ["product revenues", "product revenue"],
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        )
        if value is not None:
            metrics["revenue"] = value * statement_scale

    if metrics.get("operating_income") is None and metrics.get("revenue") is not None:
        _, total_operating_expenses = _find_periodic_row_entry(
            rows,
            ["total operating expenses", "operating expenses"],
            current_period_first=current_period_first,
            has_note_column=has_note_column,
            reject_if_contains=["non-gaap", "adjusted"],
        )
        if total_operating_expenses is not None:
            signed_expenses = total_operating_expenses * statement_scale
            operating_base = metrics.get("gross_profit") if metrics.get("gross_profit") is not None else metrics["revenue"]
            if signed_expenses < 0:
                metrics["operating_income"] = operating_base + signed_expenses
            else:
                metrics["operating_income"] = operating_base - signed_expenses

    return metrics


def _extract_balance_sheet_metrics_6k(lines: list[str], currency_unit: str | None) -> dict[str, float | None]:
    heading_pattern = r"(?i)^(?:unaudited\s+)?(?:condensed\s+)?(?:consolidated\s+)?(?:interim\s+)?(?:balance sheets?|statements?\s+of\s+financial position)(?:\s+\(continued\))?$"
    sections = _statement_sections(
        lines,
        heading_pattern,
        [
            r"(?i)condensed consolidated statements of income",
            r"(?i)condensed consolidated statements of comprehensive income",
            r"(?i)condensed consolidated statements of cash flows",
            r"(?i)unaudited condensed consolidated statements of income",
            r"(?i)unaudited condensed consolidated statements of comprehensive income",
            r"(?i)consolidated statements? of profit or loss",
            r"(?i)consolidated statements? of other comprehensive income",
            r"(?i)consolidated statements? of changes in equity",
            r"(?i)notes to consolidated financial statements",
        ],
    )
    if not sections:
        sections = _statement_sections_multiline(
            lines,
            heading_pattern,
            [
                r"(?i)condensed consolidated statements of income",
                r"(?i)condensed consolidated statements of comprehensive income",
                r"(?i)condensed consolidated statements of cash flows",
                r"(?i)unaudited condensed consolidated statements of income",
                r"(?i)unaudited condensed consolidated statements of comprehensive income",
                r"(?i)consolidated statements of operations",
                r"(?i)consolidated statements? of profit or loss",
                r"(?i)consolidated statements of comprehensive income",
                r"(?i)consolidated statements of cash flows",
                r"(?i)notes to consolidated financial statements",
            ],
        )
    if not sections:
        return {}

    effective_currency_unit = currency_unit
    inferred_header_unit = _infer_currency_unit_from_statement_headers([*sections[0][0], *sections[0][1][:24]])
    if inferred_header_unit is not None:
        effective_currency_unit = inferred_header_unit
    statement_scale = _statement_unit_multiplier(sections[0][0], effective_currency_unit)
    header_text = "\n".join("\n".join(header) for header, _ in sections)
    prefer_non_usd_current = bool(
        ("US$" in header_text or re.search(r"(?i)\bUSD\b", header_text))
        and currency_unit is not None
        and not currency_unit.upper().startswith("USD")
    )
    current_period_first = any(_statement_current_period_first(header) for header, _ in sections)
    has_note_column = any(_normalize_label(line).startswith("note") for header, _ in sections for line in header[:24])
    section_rows: list[list[tuple[str, list[float | None]]]] = []
    search_sections: list[list[str]] = []
    for header, section_lines in sections:
        extracted = _extract_rows_from_statement(section_lines)
        if len(extracted) < 5 or not any("total assets" in _normalize_label(label) for label, _ in extracted):
            combined_extracted = _extract_rows_from_statement([*header, *section_lines])
            if len(combined_extracted) > len(extracted):
                extracted = combined_extracted
                search_sections.append([*header, *section_lines])
            else:
                search_sections.append(section_lines)
        else:
            search_sections.append(section_lines)
        if extracted:
            section_rows.append(extracted)
    qualifying_sections = [
        rows
        for rows in section_rows
        if any("total assets" in _normalize_label(label) for label, _ in rows)
        or any(
            "total liabilities" in _normalize_label(label) and "equity" not in _normalize_label(label)
            for label, _ in rows
        )
    ]
    rows: list[tuple[str, list[float | None]]] = []
    for extracted in (qualifying_sections or section_rows):
        rows.extend(extracted)

    metrics = {
        "total_assets": _find_row_value(
            rows,
            ["total assets"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        ),
        "assets_current": _find_row_value(
            rows,
            ["total current assets"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        ),
        "total_liabilities": _find_row_value(
            rows,
            ["total liabilities"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
            reject_if_contains=["and shareholders", "and stockholders", "and equity"],
        ),
        "liabilities_current": _find_row_value(
            rows,
            ["total current liabilities"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        ),
        "equity": _find_row_value(
            rows,
            [
                "total equity (deficit)",
                "total equity",
                "total shareholders' equity",
                "total stockholders' equity",
                "total equity attributable to shareholders",
                "total shareholders’ equity",
            ],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
            reject_if_contains=["liabilities and"],
        ),
        "cash": _find_row_value(
            rows,
            ["cash and cash equivalents and restricted cash", "cash and cash equivalents"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        ),
        "short_term_investments": _find_row_value(
            rows,
            ["short-term investments", "short term investments"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        ),
        "goodwill": _find_row_value(
            rows,
            ["goodwill"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        ),
        "accounts_receivable": _find_row_value(
            rows,
            ["accounts receivable", "trade receivables", "receivables from online payment platforms"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        ),
        "accounts_payable": _find_row_value(
            rows,
            ["accounts payable", "trade payables", "payable to merchants", "payables to network"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        ),
        "deferred_revenue": _find_row_value(
            rows,
            [
                "deferred revenue, current",
                "deferred revenue",
                "contract liabilities",
                "deferred income",
                "customer advances and deferred revenues",
                "customer advances and deferred revenue",
            ],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        ),
        "retained_earnings": _find_row_value(
            rows,
            ["retained earnings", "accumulated deficit"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        ),
    }
    row_presence_by_metric = {
        "total_assets": any("total assets" in _normalize_label(label) for label, _ in rows),
        "assets_current": any("total current assets" in _normalize_label(label) for label, _ in rows),
        "total_liabilities": any(
            "total liabilities" in _normalize_label(label) and "equity" not in _normalize_label(label)
            for label, _ in rows
        ),
        "liabilities_current": any("total current liabilities" in _normalize_label(label) for label, _ in rows),
        "equity": any(
            target in _normalize_label(label)
            for label, _ in rows
            for target in (
                "total equity (deficit)",
                "total equity",
                "total shareholders' equity",
                "total stockholders' equity",
                "total equity attributable to shareholders",
                "total shareholders’ equity",
            )
        ),
        "cash": any(
            target in _normalize_label(label)
            for label, _ in rows
            for target in ("cash and cash equivalents and restricted cash", "cash and cash equivalents")
        ),
        "short_term_investments": any(
            target in _normalize_label(label)
            for label, _ in rows
            for target in ("short-term investments", "short term investments")
        ),
        "goodwill": any("goodwill" in _normalize_label(label) for label, _ in rows),
        "accounts_receivable": any(
            target in _normalize_label(label)
            for label, _ in rows
            for target in ("accounts receivable", "trade receivables", "receivables from online payment platforms")
        ),
        "accounts_payable": any(
            target in _normalize_label(label)
            for label, _ in rows
            for target in ("accounts payable", "trade payables", "payable to merchants", "payables to network")
        ),
        "deferred_revenue": any(
            target in _normalize_label(label)
            for label, _ in rows
            for target in (
                "deferred revenue, current",
                "deferred revenue",
                "contract liabilities",
                "deferred income",
                "customer advances and deferred revenues",
                "customer advances and deferred revenue",
            )
        ),
        "retained_earnings": any(
            target in _normalize_label(label)
            for label, _ in rows
            for target in ("retained earnings", "accumulated deficit")
        ),
    }
    if metrics["total_assets"] is None:
        metrics["total_assets"] = _find_exact_row_value(
            rows,
            ["total"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        )
    normalized_balance_labels = [_normalize_label(label) for label, _ in rows]
    order_of_liquidity_balance = any(
        marker in label
        for label in normalized_balance_labels
        for marker in (
            "credit card receivables",
            "loans to customers",
            "compulsory and other deposits at central banks",
            "payables to network",
            "financial liabilities at amortized cost",
        )
    )
    if metrics["assets_current"] is None:
        if order_of_liquidity_balance:
            inferred_assets_current = _infer_current_assets_from_total_minus_noncurrent(
                rows,
                prefer_non_usd_current=prefer_non_usd_current,
                current_period_first=current_period_first,
                has_note_column=has_note_column,
            )
        else:
            inferred_assets_current = _infer_current_assets_from_rows(
                rows,
                prefer_non_usd_current=prefer_non_usd_current,
                current_period_first=current_period_first,
                has_note_column=has_note_column,
            )
            if inferred_assets_current is None:
                inferred_assets_current = _infer_current_assets_from_total_minus_noncurrent(
                    rows,
                    prefer_non_usd_current=prefer_non_usd_current,
                    current_period_first=current_period_first,
                    has_note_column=has_note_column,
                )
        if inferred_assets_current is not None:
            metrics["assets_current"] = inferred_assets_current
    if metrics["liabilities_current"] is None:
        if order_of_liquidity_balance:
            inferred_liabilities_current = _infer_current_liabilities_from_total_minus_noncurrent(
                rows,
                prefer_non_usd_current=prefer_non_usd_current,
                current_period_first=current_period_first,
                has_note_column=has_note_column,
            )
        else:
            inferred_liabilities_current = _infer_current_liabilities_from_rows(
                rows,
                prefer_non_usd_current=prefer_non_usd_current,
                current_period_first=current_period_first,
                has_note_column=has_note_column,
            )
            if inferred_liabilities_current is None:
                inferred_liabilities_current = _infer_current_liabilities_from_total_minus_noncurrent(
                    rows,
                    prefer_non_usd_current=prefer_non_usd_current,
                    current_period_first=current_period_first,
                    has_note_column=has_note_column,
                )
        if inferred_liabilities_current is not None:
            metrics["liabilities_current"] = inferred_liabilities_current

    if any(value is None for value in metrics.values()):
        preferred_index = -1 if not current_period_first else 0
        fallback_labels = {
            "total_assets": ["total assets"],
            "assets_current": ["total current assets"],
            "total_liabilities": ["total liabilities"],
            "liabilities_current": ["total current liabilities"],
            "equity": [
                "total equity (deficit)",
                "total equity",
                "total shareholders' equity",
                "total stockholders' equity",
                "total shareholders’ equity",
                "shareholders’ equity",
            ],
            "cash": ["cash and cash equivalents and restricted cash", "cash and cash equivalents"],
            "short_term_investments": ["short-term investments", "short term investments"],
            "goodwill": ["goodwill"],
            "accounts_receivable": ["accounts receivable", "accounts receivable (net)", "trade receivables"],
            "accounts_payable": ["accounts payable", "trade payables"],
            "deferred_revenue": ["deferred revenue, current", "deferred revenue", "deferred income"],
            "retained_earnings": ["retained earnings", "accumulated deficit"],
        }
        reject_map = {
            "total_liabilities": ["and shareholders", "and stockholders", "and equity"],
            "equity": ["liabilities and"],
        }
        for key, labels in fallback_labels.items():
            if metrics.get(key) is not None:
                continue
            if row_presence_by_metric.get(key):
                continue
            for section_lines in search_sections:
                fallback_value = _find_periodic_line_value(
                    section_lines,
                    labels,
                    current_period_first=current_period_first,
                    preferred_index=preferred_index,
                    reject_if_contains=reject_map.get(key),
                )
                if fallback_value is not None:
                    metrics[key] = fallback_value
                    break

    for key, value in list(metrics.items()):
        if value is not None:
            metrics[key] = value * statement_scale
    return metrics


def _infer_current_assets_from_rows(
    rows: list[tuple[str, list[float | None]]],
    *,
    prefer_non_usd_current: bool,
    current_period_first: bool = False,
    has_note_column: bool = False,
) -> float | None:
    total_assets_idx = next(
        (idx for idx, (label, _) in enumerate(rows) if "total assets" in _normalize_label(label)),
        None,
    )
    if total_assets_idx is None:
        return None

    noncurrent_markers = (
        "non current",
        "noncurrent",
        "long term",
        "property equipment",
        "property plant",
        "right of use asset",
        "right-of-use asset",
        "intangible asset",
        "goodwill",
        "investments net",
        "investment in associates",
        "deferred tax asset",
    )
    total = 0.0
    matched = False
    for label, values in rows[:total_assets_idx]:
        norm = _normalize_label(label)
        if any(marker in norm for marker in noncurrent_markers):
            break
        if norm in {"assets", "liabilities", "shareholders equity"}:
            continue
        value = _choose_statement_value(
            values,
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        )
        if value is None:
            continue
        total += value
        matched = True
    return total if matched else None


def _infer_current_assets_from_total_minus_noncurrent(
    rows: list[tuple[str, list[float | None]]],
    *,
    prefer_non_usd_current: bool,
    current_period_first: bool = False,
    has_note_column: bool = False,
) -> float | None:
    total_assets_idx = next(
        (idx for idx, (label, _) in enumerate(rows) if "total assets" in _normalize_label(label)),
        None,
    )
    if total_assets_idx is None:
        return None

    total_assets = _choose_statement_value(
        rows[total_assets_idx][1],
        prefer_non_usd_current=prefer_non_usd_current,
        current_period_first=current_period_first,
        has_note_column=has_note_column,
    )
    if total_assets is None or total_assets <= 0:
        return None

    noncurrent_markers = (
        "non current",
        "noncurrent",
        "long term",
        "right of use asset",
        "right-of-use asset",
        "property plant and equipment",
        "property equipment",
        "intangible asset",
        "goodwill",
        "deferred tax asset",
        "investment in associates",
        "investments in associates",
        "investment property",
        "equity method investment",
        "other non current asset",
        "other noncurrent asset",
    )
    matched_values: list[float] = []
    for label, values in rows[:total_assets_idx]:
        norm = _normalize_label(label)
        if not any(marker in norm for marker in noncurrent_markers):
            continue
        value = _choose_statement_value(
            values,
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        )
        if value is None or value <= 0:
            continue
        matched_values.append(value)

    if not matched_values:
        return None

    noncurrent_total = sum(matched_values)
    inferred = total_assets - noncurrent_total
    if inferred <= 0 or inferred >= total_assets:
        return None
    return inferred


def _infer_current_liabilities_from_rows(
    rows: list[tuple[str, list[float | None]]],
    *,
    prefer_non_usd_current: bool,
    current_period_first: bool = False,
    has_note_column: bool = False,
) -> float | None:
    total_assets_idx = next(
        (idx for idx, (label, _) in enumerate(rows) if "total assets" in _normalize_label(label)),
        None,
    )
    total_liabilities_idx = next(
        (
            idx
            for idx, (label, _) in enumerate(rows)
            if "total liabilities" in _normalize_label(label)
            and "shareholders" not in _normalize_label(label)
            and "stockholders" not in _normalize_label(label)
            and "equity" not in _normalize_label(label)
        ),
        None,
    )
    if total_assets_idx is None or total_liabilities_idx is None or total_liabilities_idx <= total_assets_idx:
        return None

    noncurrent_markers = (
        "non current",
        "noncurrent",
        "long term",
        "deferred tax liabilit",
        "lease liabilities non current",
        "lease liability non current",
        "convertible note",
        "convertible debt",
        "debt non current",
        "deferred revenue non current",
    )
    total = 0.0
    matched = False
    section_rows = rows[total_assets_idx + 1 : total_liabilities_idx]
    seen_labels: dict[str, int] = {}
    repeated_label_idx: int | None = None
    for idx, (label, _) in enumerate(section_rows):
        norm = _normalize_label(label)
        if norm in seen_labels and idx - seen_labels[norm] >= 3:
            repeated_label_idx = idx
            break
        seen_labels[norm] = idx

    for idx, (label, values) in enumerate(section_rows):
        if repeated_label_idx is not None and idx >= repeated_label_idx:
            break
        norm = _normalize_label(label)
        if any(marker in norm for marker in noncurrent_markers):
            break
        if "commitments and contingencies" in norm:
            continue
        value = _choose_statement_value(
            values,
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        )
        if value is None:
            continue
        total += value
        matched = True
    return total if matched else None


def _infer_current_liabilities_from_total_minus_noncurrent(
    rows: list[tuple[str, list[float | None]]],
    *,
    prefer_non_usd_current: bool,
    current_period_first: bool = False,
    has_note_column: bool = False,
) -> float | None:
    total_assets_idx = next(
        (idx for idx, (label, _) in enumerate(rows) if "total assets" in _normalize_label(label)),
        None,
    )
    total_liabilities_idx = next(
        (
            idx
            for idx, (label, _)
            in enumerate(rows)
            if "total liabilities" in _normalize_label(label)
            and "shareholders" not in _normalize_label(label)
            and "stockholders" not in _normalize_label(label)
            and "equity" not in _normalize_label(label)
        ),
        None,
    )
    if total_assets_idx is None or total_liabilities_idx is None or total_liabilities_idx <= total_assets_idx:
        return None

    total_liabilities = _choose_statement_value(
        rows[total_liabilities_idx][1],
        prefer_non_usd_current=prefer_non_usd_current,
        current_period_first=current_period_first,
        has_note_column=has_note_column,
    )
    if total_liabilities is None or total_liabilities <= 0:
        return None

    section_rows = rows[total_assets_idx + 1 : total_liabilities_idx]
    noncurrent_markers = (
        "non current",
        "noncurrent",
        "long term",
        "lease liabilities non current",
        "lease liability non current",
        "deferred tax liabilit",
        "deferred income non current",
        "deferred revenue non current",
        "borrowings and financing non current",
        "borrowings non current",
        "provisions non current",
        "other non current liabilit",
        "other noncurrent liabilit",
        "subordinated debt",
        "senior notes",
        "convertible debt",
        "convertible note",
    )
    matched_values: list[float] = []
    for label, values in section_rows:
        norm = _normalize_label(label)
        if not any(marker in norm for marker in noncurrent_markers):
            continue
        value = _choose_statement_value(
            values,
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            has_note_column=has_note_column,
        )
        if value is None or value <= 0:
            continue
        matched_values.append(value)

    if matched_values:
        inferred = total_liabilities - sum(matched_values)
        if 0 < inferred < total_liabilities:
            return inferred

    # IFRS financial institutions often present a statement of financial position
    # in order of liquidity instead of a current/non-current split.
    order_of_liquidity_markers = (
        "deposits",
        "credit card receivables",
        "loans to customers",
        "compulsory and other deposits at central banks",
        "payables to network",
        "financial liabilities at amortized cost",
    )
    if any(any(marker in _normalize_label(label) for marker in order_of_liquidity_markers) for label, _ in section_rows):
        return total_liabilities
    return None


def _apply_20f_statement_fallbacks(report: FilingReport, lines: list[str], currency_unit: str | None) -> None:
    sections = _statement_sections_multiline(
        lines,
        r"(?i)(balance sheets|statements of financial position)",
        [
            r"(?i)consolidated statements of operations",
            r"(?i)consolidated statements of comprehensive income",
            r"(?i)consolidated statements of cash flows",
            r"(?i)notes to the consolidated financial statements",
        ],
    )
    if not sections:
        return

    ranked_sections: list[tuple[int, list[tuple[str, list[float | None]]]]] = []
    for header, section_lines in sections:
        rows = _extract_rows_from_statement(section_lines)
        if not rows:
            continue
        header_text = _normalize_label(" ".join(header[:16]))
        score = 0
        if "consolidated balance" in header_text or "statements of financial position" in header_text:
            score += 4
        if "schedule" in header_text or "parent company" in header_text:
            score -= 6
        if "selected information" in header_text:
            score -= 4
        if any("total assets" in _normalize_label(label) for label, _ in rows):
            score += 3
        if any(
            "total liabilities" in _normalize_label(label) and "equity" not in _normalize_label(label)
            for label, _ in rows
        ):
            score += 3
        ranked_sections.append((score, rows))

    if not ranked_sections:
        return
    _, rows = max(ranked_sections, key=lambda item: item[0])

    header_text = "\n".join("\n".join(header) for header, _ in sections)
    prefer_non_usd_current = bool(
        ("US$" in header_text or re.search(r"(?i)\bUSD\b", header_text))
        and currency_unit is not None
        and not currency_unit.upper().startswith("USD")
    )
    current_period_first = any(_statement_current_period_first(header) for header, _ in sections)
    statement_scale = _unit_multiplier(currency_unit)
    for header, section_lines in sections:
        section_rows = _extract_rows_from_statement(section_lines)
        if section_rows == rows:
            statement_scale = _statement_unit_multiplier(header, currency_unit)
            break

    direct_row_metrics = {
        "total_assets": _find_row_value(
            rows,
            ["total assets"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
        ),
        "total_liabilities": _find_row_value(
            rows,
            ["total liabilities"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            reject_if_contains=["and shareholders", "and stockholders", "and equity"],
        ),
        "equity": _find_row_value(
            rows,
            ["total shareholders equity", "total stockholders equity", "total equity", "total 9f inc. shareholders equity"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            reject_if_contains=["liabilities and"],
        ),
        "cash": _find_row_value(
            rows,
            ["cash and cash equivalents"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
        ),
        "short_term_investments": _find_row_value(
            rows,
            ["short term investments", "term deposits"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
        ),
        "accounts_receivable": _find_row_value(
            rows,
            ["accounts receivable", "trade receivables"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
        ),
        "accounts_payable": _find_row_value(
            rows,
            ["accounts payable", "trade payables"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
        ),
        "deferred_revenue": _find_row_value(
            rows,
            ["deferred revenue", "contract liabilities"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
            reject_if_contains=["accounts and other payables", "other payables", "accounts payable"],
        ),
        "retained_earnings": _find_row_value(
            rows,
            ["retained earnings", "accumulated deficit", "deficit"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
        ),
        "goodwill": _find_row_value(
            rows,
            ["goodwill"],
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
        ),
    }
    for metric, value in direct_row_metrics.items():
        if getattr(report, metric) is None and value is not None:
            setattr(report, metric, value * statement_scale)

    if report.assets_current is None:
        inferred_assets_current = _infer_current_assets_from_rows(
            rows,
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
        )
        if inferred_assets_current is not None:
            report.assets_current = inferred_assets_current * statement_scale

    if report.liabilities_current is None:
        inferred_liabilities_current = _infer_current_liabilities_from_rows(
            rows,
            prefer_non_usd_current=prefer_non_usd_current,
            current_period_first=current_period_first,
        )
        if inferred_liabilities_current is not None:
            report.liabilities_current = inferred_liabilities_current * statement_scale


def _apply_20f_operations_statement_fallbacks(report: FilingReport, lines: list[str], currency_unit: str | None) -> None:
    sections = _statement_blocks(
        lines,
        r"(?i)^(?:unaudited\s+)?(?:condensed\s+)?(?:combined and\s+)?(?:consolidated\s+)?(?:interim\s+)?statements?\s+of\s+(?:operations|income(?:\s*\(loss\))?|profit or loss|earnings|income and comprehensive income)(?:\s+for\b|\s*\(|\s*$)",
        [
            r"(?i)consolidated statements of comprehensive income",
            r"(?i)consolidated statements of changes in (?:shareholders'? )?equity",
            r"(?i)consolidated statements of cash flows",
            r"(?i)notes to (?:the )?consolidated financial statements",
            r"(?i)report of independent registered public accounting firm",
        ],
    )
    if not sections:
        return

    ranked_sections: list[tuple[int, list[str], list[str], list[tuple[str, list[float | None]]]]] = []
    for header, section_lines in sections:
        rows = _attach_subrow_context(_extract_rows_from_statement(section_lines))
        if len(rows) < 8:
            continue
        header_text = _normalize_label(" ".join(header[:16]))
        score = 0
        if "consolidated statements of operations" in header_text or "consolidated statements of income" in header_text:
            score += 4
        if any("total revenues" in _normalize_label(label) for label, _ in rows):
            score += 4
        if any("operating loss" in _normalize_label(label) or "operating income" in _normalize_label(label) for label, _ in rows):
            score += 3
        if any("net loss income" in _normalize_label(label) or "net income" in _normalize_label(label) for label, _ in rows):
            score += 3
        ranked_sections.append((score, header, section_lines, rows))

    if not ranked_sections:
        return

    _, header, section_lines, rows = max(ranked_sections, key=lambda item: item[0])
    header_text = "\n".join([*header[:24], *section_lines[:24]])
    prefer_non_usd_current = bool(
        ("US$" in header_text or re.search(r"(?i)\bUSD\b", header_text))
        and currency_unit is not None
        and not currency_unit.upper().startswith("USD")
    )
    current_period_first = _statement_current_period_first(header)
    has_note_column = any(_normalize_label(line).startswith("note") for line in header[:24])
    statement_scale = _statement_unit_multiplier(header, currency_unit)

    def _find_operation_row_entry(
        labels: list[str],
        *,
        reject_if_contains: list[str] | None = None,
    ) -> tuple[str | None, float | None]:
        reject_if_contains = [_normalize_label(value) for value in (reject_if_contains or [])]
        normalized_rows = [(_normalize_label(label), values) for label, values in rows]
        for target in labels:
            norm_target = _normalize_label(target)
            for label, values in normalized_rows:
                if norm_target in label and not any(bad in label for bad in reject_if_contains):
                    value = _choose_statement_value(
                        values,
                        prefer_non_usd_current=prefer_non_usd_current,
                        current_period_first=current_period_first,
                        has_note_column=has_note_column,
                    )
                    if value is not None:
                        return label, value
        return None, None

    revenue_label, revenue_value = _find_operation_row_entry(
        [
            "total revenues excluding cost of goods sold",
            "total revenues excluded cost of goods sold",
            "total revenues",
            "total revenue",
        ]
    )
    _, cogs_value = _find_operation_row_entry(
        ["cost of goods sold", "cost of sales", "cost of revenues", "cost of revenue"]
    )
    _, gross_value = _find_operation_row_entry(["gross profit", "gross loss"])

    if revenue_value is not None:
        report.revenue = revenue_value * statement_scale
    if cogs_value is not None:
        report.cogs = cogs_value * statement_scale
    if revenue_label and "excluding cost of goods sold" in revenue_label and revenue_value is not None and cogs_value is not None:
        report.gross_profit = (revenue_value - abs(cogs_value)) * statement_scale
    elif gross_value is not None:
        report.gross_profit = gross_value * statement_scale

    if report.general_and_administrative is None:
        _, gna_value = _find_operation_row_entry(["general and administrative expenses"])
        if gna_value is not None:
            report.general_and_administrative = gna_value * statement_scale

    if report.interest_income is None:
        _, net_interest_income_value = _find_operation_row_entry(["net interest income"])
        if net_interest_income_value is not None:
            report.interest_income = net_interest_income_value * statement_scale


def _maybe_correct_duplicate_thousand_scale_annual_report(
    report: FilingReport,
    lines: list[str],
    statement_currency_unit: str | None,
) -> None:
    if report.form_type not in {"10-K", "10-Q", "20-F"}:
        return
    if statement_currency_unit is None or "THOUSAND" not in statement_currency_unit.upper():
        return

    anchor_keywords = {
        "revenue": ("revenue", "revenues"),
        "operating_cash_flow": ("operating activities",),
        "net_income": ("net income", "net loss"),
        "cash": ("cash",),
        "accounts_receivable": ("accounts receivable",),
    }
    anchor_hits = 0
    for metric, keywords in anchor_keywords.items():
        value = getattr(report, metric)
        if value is None or abs(value) < 1_000_000:
            continue
        candidate = value / 1_000.0
        rounded = round(candidate)
        if abs(candidate - rounded) > 1e-6:
            continue
        token = f"{abs(int(rounded)):,}"
        for idx, line in enumerate(lines[:12000]):
            if token not in line or "$" not in line:
                continue
            if len(re.findall(r"[A-Za-z]{3,}", line)) < 5:
                continue
            window = " ".join(lines[max(0, idx - 1) : min(len(lines), idx + 2)]).lower()
            if any(keyword in window for keyword in keywords):
                anchor_hits += 1
                break

    if anchor_hits < 2:
        return

    report.currency_unit = statement_currency_unit
    for metric in (
        "revenue",
        "cogs",
        "gross_profit",
        "operating_income",
        "research_and_development",
        "selling_and_marketing",
        "general_and_administrative",
        "sga",
        "pretax_income",
        "tax_expense",
        "net_income",
        "ebitda",
        "interest_income",
        "total_assets",
        "assets_current",
        "total_liabilities",
        "liabilities_current",
        "equity",
        "cash",
        "short_term_investments",
        "goodwill",
        "accounts_receivable",
        "accounts_payable",
        "deferred_revenue",
        "retained_earnings",
        "operating_cash_flow",
        "investing_cash_flow",
        "financing_cash_flow",
        "free_cash_flow",
        "depreciation_and_amortization",
        "share_based_compensation",
        "capex",
        "acquisitions",
    ):
        value = getattr(report, metric)
        if value is not None:
            setattr(report, metric, value / 1_000.0)


def _extract_cash_flow_metrics_6k(lines: list[str], currency_unit: str | None) -> dict[str, float | None]:
    sections = _statement_blocks(
        lines,
        r"(?i)^(?:unaudited\s+)?(?:(?:selected\s+)?(?:condensed\s+)?(?:combined and\s+)?(?:consolidated\s+)?(?:interim\s+)?)?"
        r"(?:statements?\s+of\s+cash flows(?:\s+and\s+free cash flow)?|cash flows\s+statements?|cash flows data)(?:\s+for\b|\s*\(|\s+(?:three|six|nine|twelve)\s+months?\s+ended\b|\s+year\s+ended\b|\s+as\s+of\b|\s*$)",
        [
            r"(?i)(balance sheets?|statements?\s+of\s+financial position)",
            r"(?i)statements?\s+of\s+(?:operations|income|earnings|comprehensive income)",
            r"(?i)notes to consolidated financial statements",
            r"(?i)report of independent registered public accounting firm",
        ],
    )
    if not sections:
        return {}

    statement_scale = _statement_unit_multiplier(sections[0][0], currency_unit)
    prefer_q4_current_mixed = _has_quarter_full_year_gaap_highlight_table(lines)
    prefer_quarter_current_mixed = _prefer_quarter_current_mixed_6k(lines)
    section_payloads: list[dict[str, object]] = []
    for header, body in sections:
        extracted = _extract_rows_from_statement(body)
        if not extracted:
            continue
        current_period_first = _statement_current_period_first(header)
        has_note_column = any(_normalize_label(line).startswith("note") for line in header[:24])
        inferred_non_usd_index, inferred_usd_index = _infer_periodic_statement_column_indices(body)
        header_value_index = _infer_periodic_statement_value_index(header)
        section_non_usd_index, section_usd_index = _resolve_periodic_preferred_indices(
            inferred_non_usd_index,
            inferred_usd_index,
            header_value_index,
        )
        repeated_non_usd_index, repeated_usd_index = _infer_repeated_periodic_subcolumn_indices(header, extracted)
        if repeated_non_usd_index is not None:
            section_non_usd_index = repeated_non_usd_index
        if repeated_usd_index is not None:
            section_usd_index = repeated_usd_index

        mixed_quarter_year = _is_mixed_quarter_year_statement_header(header)
        if (
            prefer_quarter_current_mixed
            and mixed_quarter_year
            and section_non_usd_index is not None
            and section_usd_index == section_non_usd_index
            and section_non_usd_index >= 2
        ):
            section_non_usd_index = 0
            section_usd_index = 0
        if (
            prefer_q4_current_mixed
            and section_non_usd_index is not None
            and section_non_usd_index >= 4
            and section_usd_index == section_non_usd_index
        ):
            section_non_usd_index = 2
            section_usd_index = 2

        section_payloads.append(
            {
                "header": header,
                "rows": extracted,
                "current_period_first": current_period_first,
                "has_note_column": has_note_column,
                "non_usd_index": section_non_usd_index,
                "mixed_quarter_year": mixed_quarter_year,
            }
        )

    if not section_payloads:
        return {}

    qualifying_payloads = [
        payload
        for payload in section_payloads
        if len(payload["rows"]) >= 6
        and any(
            key in _normalize_label(label)
            for label, _ in payload["rows"]
            for key in (
                "net cash provided by",
                "net cash inflow from",
                "net cash outflow from",
                "net cash used in",
                "net cash generated by operating activities",
                "cash flows generated from used in operating activities",
                "cash flows generated from used in investing activities",
                "cash flows generated from used in financing activities",
                "change in cash and cash equivalents",
                "cash and cash equivalents at end of year",
            )
        )
    ]
    scale = statement_scale
    label_map: dict[str, list[str]] = {
        "operating_cash_flow": [
            "net cash provided by operating activities",
            "net cash provided by used in operating activities",
            "net cash inflow from operating activities",
            "net cash outflow from operating activities",
            "net cash generated by operating activities",
            "net cash generated from operating activities",
            "net cash from operating activities",
            "cash flows generated from used in operating activities",
            "cash flow generated from used in operating activities",
        ],
        "investing_cash_flow": [
            "net cash used in investing activities",
            "net cash provided by used in investing activities",
            "net cash provided by investing activities",
            "net cash inflow from investing activities",
            "net cash outflow from investing activities",
            "net cash generated from investing activities",
            "net cash from investing activities",
            "cash flow generated from used in investing activities",
            "cash flows generated from used in investing activities",
        ],
        "financing_cash_flow": [
            "net cash (used in) provided by financing activities",
            "net cash used in /provided by financing activities",
            "net cash provided by used in financing activities",
            "net cash used in provided by financing activities",
            "net cash used in financing activities",
            "net cash provided by financing activities",
            "net cash inflow from financing activities",
            "net cash outflow from financing activities",
            "net cash generated from financing activities",
            "net cash from financing activities",
            "cash flow generated from used in financing activities",
            "cash flows generated from used in financing activities",
        ],
        "depreciation_and_amortization": [
            "depreciation and amortization",
            "depreciation amortization and impairment",
            "depreciation",
        ],
        "share_based_compensation": [
            "stock-based compensation",
            "stock based compensation",
            "share-based compensation",
            "share based compensation",
            "share-based payment",
            "share based payment",
        ],
        "capex": [
            "capital expenditures",
            "purchase of property & equipment",
            "purchases of property & equipment",
            "acquisition of property plant and equipment",
            "purchase of property plant and equipment",
            "purchase of property and equipment",
            "purchases of property and equipment",
            "purchase of property, plant and equipment",
            "property plant and equipment",
        ],
        "acquisitions": [
            "business acquisitions",
            "acquisition of businesses",
            "acquisition of whow games",
            "acquisition of subsidiaries",
            "acquisition of subsidiary net of cash acquired",
        ],
        "free_cash_flow": [
            "free cash flow",
        ],
    }

    metrics: dict[str, float | None] = {metric: None for metric in label_map}
    reject_map: dict[str, list[str]] = {
        "operating_cash_flow": [
            "adjustments to reconcile",
        ],
        "capex": [
            "loss on",
            "impairment",
            "proceeds from",
            "government grants",
            "disposal",
            "retirement",
        ],
    }
    for payload in (qualifying_payloads or section_payloads):
        rows = _attach_subrow_context(payload["rows"])
        current_period_first = bool(payload["current_period_first"])
        has_note_column = bool(payload["has_note_column"])
        current_non_usd_index = payload["non_usd_index"]
        mixed_quarter_year = bool(payload["mixed_quarter_year"])
        for label, values in rows:
            normalized = _normalize_label(label)
            for metric, labels in label_map.items():
                if metrics[metric] is not None:
                    continue
                if any(bad in normalized for bad in reject_map.get(metric, [])):
                    continue
                if not any(target in normalized for target in labels):
                    continue
                if metric == "free_cash_flow" and not normalized.startswith("free cash flow"):
                    continue
                if mixed_quarter_year and current_non_usd_index is None and len([v for v in values if v is not None]) >= 4:
                    value = _select_mixed_quarter_year_value(
                        values,
                        prefer_quarter_current=prefer_quarter_current_mixed,
                    )
                else:
                    value = _choose_periodic_statement_value(
                        values,
                        current_period_first=current_period_first,
                        has_note_column=has_note_column,
                        preferred_index=current_non_usd_index,
                    )
                if value is None:
                    continue
                if metric in {"capex", "acquisitions"}:
                    metrics[metric] = -abs(value) * scale
                else:
                    metrics[metric] = value * scale
    return metrics


def _extract_shares_from_text(
    lines: list[str],
    period_end_date: str | None,
    ordinary_shares_per_depositary_share: float | None = None,
) -> float | None:
    def _is_year_like_value(value: float | None) -> bool:
        if value is None:
            return False
        integer = int(round(abs(value)))
        return abs(abs(value) - integer) < 1e-6 and 1900 <= integer <= 2100

    def _scaled_value(
        number_text: str,
        unit_word: str | None = None,
        context_multiplier: float = 1.0,
    ) -> float | None:
        base = _parse_number(number_text)
        if base is None:
            return None
        if unit_word:
            return base * _unit_word_multiplier(unit_word)
        return base * context_multiplier

    def _pick_last_substantive_value(text: str, context_multiplier: float) -> float | None:
        values: list[float] = []
        for token in re.findall(r"(?i)([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,}|[0-9]+(?:\.\d+)?)\s*(million|billion|thousand)?", text):
            value = _scaled_value(token[0], token[1], context_multiplier)
            if value is None:
                continue
            compact = token[0].replace(",", "")
            if re.fullmatch(r"\d{4}", compact):
                year_like = int(compact)
                if 1900 <= year_like <= 2100:
                    continue
            values.append(value)
        if not values:
            return None
        return values[-1]

    period_text = (period_end_date or "").lower()
    class_specific: list[float] = []
    generic: list[float] = []

    for idx, line in enumerate(lines[:8000]):
        window = " ".join(lines[idx : idx + 3])
        context_window = " ".join(lines[max(0, idx - 20) : idx + 4])
        lower = window.lower()
        if "outstanding" not in lower or ("share" not in lower and not _references_depositary_shares(window)):
            continue
        if "weighted average" in lower or "average basic" in lower or "average diluted" in lower:
            continue
        if period_text and any(token in period_text for token in ("march", "june", "september", "december")):
            period_parts = [part.strip() for part in period_text.split(",") if part.strip()]
            period_prefix = period_parts[0] if period_parts else period_text
            period_year = period_parts[-1] if len(period_parts) >= 2 else ""
            if period_prefix.lower() not in lower:
                continue
            if period_year and period_year not in lower:
                continue
        context_multiplier = _share_unit_context_multiplier(context_window)
        compare_idx = lower.find("compared to")
        search_window = window if compare_idx < 0 else window[:compare_idx]

        if "shares outstanding as at" in lower:
            dated_matches = list(
                re.finditer(
                    r"(?i)([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,}|[0-9]+(?:\.\d+)?)\s*(million|billion|thousand)?\s+shares?\s+outstanding\s+as\s+at\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})",
                    search_window,
                )
            )
            if dated_matches:
                period_dt = _parse_date(period_end_date) if period_end_date else None
                chosen_match = None
                chosen_dt = None
                for match in dated_matches:
                    match_dt = _parse_date(match.group(3))
                    if match_dt is None:
                        continue
                    if period_dt is not None and match_dt == period_dt:
                        chosen_match = match
                        chosen_dt = match_dt
                        break
                    if chosen_dt is None or match_dt > chosen_dt:
                        chosen_match = match
                        chosen_dt = match_dt
                if chosen_match is not None:
                    value = _scaled_value(chosen_match.group(1), chosen_match.group(2), context_multiplier)
                    if value is not None and not _is_year_like_value(value):
                        prefix = _normalize_label(search_window[: chosen_match.start()])
                        if "common shares" in prefix:
                            label_hint = "common shares"
                        elif "ordinary shares" in prefix:
                            label_hint = "ordinary shares"
                        elif _references_depositary_shares(prefix):
                            label_hint = "ADS"
                        else:
                            label_hint = "shares"
                        normalized_value = _normalize_depositary_metric(
                            "shares_outstanding",
                            label_hint,
                            value,
                            ordinary_shares_per_depositary_share,
                        )
                        if normalized_value is not None:
                            generic.append(normalized_value)
                            continue

        equivalent_ads_match = re.search(
            r"(?i)outstanding\s+(?:class\s+[ab]\s+)?ordinary\s+shares?\s+were\s+"
            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,}|[0-9]+(?:\.\d+)?)\s*(million|billion|thousand)?"
            r"[^.]{0,160}?\bequivalent to(?:\s+about)?\s+"
            r"([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,}|[0-9]+(?:\.\d+)?)\s*(million|billion|thousand)?\s+"
            r"(?:american\s+depositary\s+shares|depositary\s+shares|depository\s+shares|adss|ads|adrs|adr)\b",
            search_window,
        )
        if equivalent_ads_match:
            ads_value = _scaled_value(
                equivalent_ads_match.group(3),
                equivalent_ads_match.group(4),
                context_multiplier,
            )
            if ads_value is not None and not _is_year_like_value(ads_value):
                generic.append(ads_value)
                continue

        matches = list(
            re.finditer(
                r"(?i)([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,}|[0-9]+(?:\.\d+)?)\s*(million|billion|thousand)?\s+((?:american\s+depositary|depositary|depository)\s+shares?|ads|adss|adr|adrs|ordinary\s+shares?|shares?)\s+(?:issued\s+and\s+)?outstanding(?:\s+as\s+of|\s*\(|\s*,|\s*$)",
                search_window,
            )
        )
        if matches:
            match = matches[0]
            value = _scaled_value(match.group(1), match.group(2), context_multiplier)
            if value is not None and not _is_year_like_value(value):
                value = _normalize_depositary_metric(
                    "shares_outstanding",
                    match.group(3),
                    value,
                    ordinary_shares_per_depositary_share,
                )
                if "class a" in lower or "class b" in lower:
                    class_specific.append(value)
                else:
                    generic.append(value)
                continue

        if re.search(r"(?i)\bshares?\s+outstanding\b", line) and "weighted average" not in lower:
            for follow in lines[idx + 1 : idx + 5]:
                if not _is_number_line(follow):
                    if generic or class_specific:
                        break
                    continue
                parsed = _parse_number(follow)
                if parsed is None or parsed < 1_000 or _is_year_like_value(parsed):
                    continue
                normalized_value = _normalize_depositary_metric(
                    "shares_outstanding",
                    line,
                    parsed,
                    ordinary_shares_per_depositary_share,
                )
                if normalized_value is None:
                    continue
                if "class a" in lower or "class b" in lower:
                    class_specific.append(normalized_value)
                else:
                    generic.append(normalized_value)
                break

        match = re.search(
            r"(?i)((?:american\s+depositary|depositary|depository)\s+shares?|ads|adss|adr|adrs|ordinary\s+shares?|shares?)\s+(?:issued\s+and\s+)?outstanding(?:\s+as\s+of)?(.*)",
            search_window,
        )
        if match:
            trailing_text = match.group(2)
            treasury_split = re.split(
                r"(?i)\bexcluding\b.*?\bshares?\s+held\s+in\s+treasury\b",
                trailing_text,
                maxsplit=1,
            )
            candidate_text = treasury_split[0] if treasury_split else trailing_text
            value = _pick_last_substantive_value(candidate_text, context_multiplier)
            if value is not None:
                normalized_value = _normalize_depositary_metric(
                    "shares_outstanding",
                    match.group(1),
                    value,
                    ordinary_shares_per_depositary_share,
                )
                if normalized_value is not None:
                    generic.append(normalized_value)

    if class_specific:
        return float(sum(class_specific))
    if generic:
        return max(generic)
    return None


def _extract_weighted_shares_from_text(
    lines: list[str],
    period_end_date: str | None,
    ordinary_shares_per_depositary_share: float | None = None,
) -> tuple[float | None, float | None]:
    period_dt = _parse_date(period_end_date)
    prefer_year_end_tail = period_dt is not None and period_dt.month == 12 and period_dt.day >= 25
    prefer_quarter_current_mixed = _prefer_quarter_current_mixed_6k(lines)

    def _is_year_like_value(value: float | None) -> bool:
        if value is None:
            return False
        integer = int(round(abs(value)))
        return abs(abs(value) - integer) < 1e-6 and 1900 <= integer <= 2100

    def _parse_line(line: str) -> float | None:
        match = re.search(
            r"(?i)([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.[0-9]+)?)\s*(million|billion|thousand)",
            line,
        )
        if match:
            base = _parse_number(match.group(1))
            if base is None:
                return None
            if "," in match.group(1) and base >= 1_000_000:
                return base
            return base * _unit_word_multiplier(match.group(2))
        values = []
        for token in re.findall(r"(?i)([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,}|[0-9]+(?:\.\d+)?)", line):
            parsed = _parse_number(token)
            if parsed is not None:
                values.append(parsed)
        if values:
            chosen_index = 0
            while chosen_index + 1 < len(values):
                current_value = abs(values[chosen_index])
                next_value = abs(values[chosen_index + 1])
                if current_value < max(10.0, next_value * 0.1):
                    chosen_index += 1
                    continue
                break
            return values[chosen_index]
        return None

    def _pick_from_values(nums: list[float], *, prefer_first: bool = False) -> float | None:
        if not nums:
            return None
        if prefer_first:
            return nums[0]
        if prefer_year_end_tail and len(nums) >= 3:
            return nums[-1]
        chosen_index = 0
        while chosen_index + 1 < len(nums):
            current_value = abs(nums[chosen_index])
            next_value = abs(nums[chosen_index + 1])
            if current_value < max(10.0, next_value * 0.1):
                chosen_index += 1
                continue
            break
        return nums[chosen_index]

    def _pick_from_mixed_three_nine_values(nums: list[float]) -> float | None:
        if len(nums) >= 5:
            return nums[1]
        return _pick_from_values(nums)

    def _pick_from_mixed_quarter_year_values(nums: list[float], *, current_period_first: bool) -> float | None:
        selected = _select_mixed_quarter_year_value(
            nums,
            prefer_quarter_current=prefer_quarter_current_mixed,
        )
        if selected is not None:
            return selected
        if len(nums) >= 4:
            return nums[0] if current_period_first else nums[1]
        return _pick_from_values(nums, prefer_first=True)

    def _extract_inline_values(line: str, scale: float) -> list[float]:
        values: list[float] = []
        for token in re.findall(r"(?i)([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,}|[0-9]+(?:\.\d+)?)", line):
            parsed = _parse_number(token)
            if parsed is None:
                continue
            if abs(parsed) < 1_000_000 and scale != 1.0:
                parsed *= scale
            values.append(parsed)
        return values

    basic = None
    diluted = None
    capped_lines = lines[:20000]
    scan_indices = (
        range(len(capped_lines) - 1, -1, -1)
        if prefer_year_end_tail
        else range(len(capped_lines))
    )
    for idx in scan_indices:
        line = capped_lines[idx]
        line_lower = line.lower()
        next_lower = capped_lines[idx + 1].lower() if idx + 1 < len(capped_lines) else ""
        split_heading = line_lower.strip() == "weighted" and "average" in next_lower and (
            "share" in next_lower or "ads" in next_lower
        )
        combined_lower = f"{line_lower} {next_lower}" if split_heading else line_lower
        if not re.search(r"weighted(?:[- ]average| avg\.)", combined_lower) or (
            "share" not in combined_lower and "ads" not in combined_lower
        ):
            continue

        scale = _share_unit_context_multiplier(" ".join(capped_lines[max(0, idx - 24) : idx + 24]))
        block = capped_lines[idx : idx + 20]
        context_start = max(0, idx - 220)
        current_period_first_start = max(0, idx - 60)
        context_text = " ".join(line.lower() for line in capped_lines[context_start : idx + 5])
        current_period_first_from_context = _statement_current_period_first(capped_lines[current_period_first_start:idx])
        mixed_three_nine = "three months ended" in context_text and "nine months ended" in context_text
        mixed_quarter_year = (
            ("quarter ended" in context_text or "three months ended" in context_text)
            and "year ended" in context_text
        )

        def _pick_contextual_values(nums: list[float]) -> float | None:
            if mixed_three_nine:
                return _pick_from_mixed_three_nine_values(nums)
            if mixed_quarter_year:
                return _pick_from_mixed_quarter_year_values(
                    nums,
                    current_period_first=current_period_first_from_context,
                )
            return _pick_from_values(nums)

        def _pick(bucket_name: str) -> float | None:
            for j, row in enumerate(block):
                if bucket_name not in row.lower():
                    continue
                row_lower = row.lower()
                if not any(
                    marker in row_lower
                    for marker in (
                        "weighted average",
                        "shares used in computing",
                        "denominator of",
                        "shares outstanding",
                    )
                ):
                    continue
                row_context_text = " ".join(block[j : j + 2]).lower()
                row_declares_thousands = (
                    "thousand" in row_context_text or "000s" in row_context_text or "000's" in row_context_text
                )
                if any(
                    phrase in row_lower
                    for phrase in (
                        "is calculated by dividing",
                        "are calculated by dividing",
                        "considers the number of shares outstanding",
                        "weighted average during each period presented",
                        "following the treasury stock method",
                        "as required by ias",
                        "during years ended",
                        "during the period plus",
                        "profit used to determine",
                    )
                ):
                    continue
                if "eps" in row_lower and "computation" not in row_lower and "share" not in row_lower:
                    continue
                inline_values = _strip_leading_note_values(_extract_inline_values(row, scale))
                inline_values = [value for value in inline_values if not _is_year_like_value(value)]
                if inline_values and max(abs(value) for value in inline_values) >= 1_000:
                    metric_name = "weighted_avg_shares_basic" if bucket_name == "basic" else "weighted_avg_shares_diluted"
                    selected = _pick_contextual_values(inline_values)
                    return _normalize_depositary_metric(
                        metric_name,
                        row,
                        selected,
                        ordinary_shares_per_depositary_share,
                    )
                inline_val = _parse_line(row)
                if inline_val is not None and inline_val >= 1_000:
                    if scale != 1.0 and row_declares_thousands:
                        value = inline_val * scale
                    else:
                        value = inline_val * scale if scale != 1.0 and inline_val < 1_000_000 else inline_val
                    metric_name = "weighted_avg_shares_basic" if bucket_name == "basic" else "weighted_avg_shares_diluted"
                    return _normalize_depositary_metric(metric_name, row, value, ordinary_shares_per_depositary_share)
                nums: list[float] = []
                for follow in block[j + 1 : j + 8]:
                    if follow.lower().startswith("-") and not _is_number_line(follow):
                        break
                    if _is_number_line(follow):
                        parsed = _parse_number(follow)
                        if parsed is not None and abs(parsed) >= 1_000 and not _is_year_like_value(parsed):
                            if scale != 1.0 and row_declares_thousands:
                                nums.append(parsed * scale)
                            else:
                                nums.append(parsed if scale != 1.0 and abs(parsed) >= 1_000_000 else parsed * scale)
                    elif nums:
                        break
                if nums:
                    metric_name = "weighted_avg_shares_basic" if bucket_name == "basic" else "weighted_avg_shares_diluted"
                    return _normalize_depositary_metric(
                        metric_name,
                        row,
                        _pick_contextual_values(nums),
                        ordinary_shares_per_depositary_share,
                    )
            return None

        normalized_line = _normalize_label(line)
        inline_values = _extract_inline_values(line, scale)
        inline_values = [value for value in inline_values if not _is_year_like_value(value)]

        def _collect_following_number_lines() -> list[float]:
            nums: list[float] = []
            line_declares_thousands = (
                "thousand" in combined_lower or "000s" in combined_lower or "000's" in combined_lower
            )
            for follow in capped_lines[idx + 1 : idx + 8]:
                if not _is_number_line(follow):
                    if nums:
                        break
                    continue
                parsed = _parse_number(follow)
                if parsed is None or _is_year_like_value(parsed):
                    continue
                if scale != 1.0 and line_declares_thousands:
                    nums.append(parsed * scale)
                else:
                    nums.append(parsed if scale == 1.0 or abs(parsed) >= 1_000_000 else parsed * scale)
            return nums

        if basic is None and normalized_line.startswith("weighted avg") and "ads equivalent" in normalized_line:
            nums = inline_values or _collect_following_number_lines()
            selected = (
                _pick_from_mixed_quarter_year_values(
                    nums,
                    current_period_first=current_period_first_from_context,
                )
                if len(nums) >= 4
                else _pick_contextual_values(nums)
            )
            if selected is not None and selected >= 1_000:
                basic = _normalize_depositary_metric(
                    "weighted_avg_shares_basic",
                    line,
                    selected,
                    ordinary_shares_per_depositary_share,
                )
        if diluted is None and "diluted ads equivalent" in normalized_line:
            nums = inline_values or _collect_following_number_lines()
            selected = (
                _pick_from_mixed_quarter_year_values(
                    nums,
                    current_period_first=current_period_first_from_context,
                )
                if len(nums) >= 4
                else _pick_contextual_values(nums)
            )
            if selected is not None and selected >= 1_000:
                diluted = _normalize_depositary_metric(
                    "weighted_avg_shares_diluted",
                    line,
                    selected,
                    ordinary_shares_per_depositary_share,
                )
        if basic is None and (
            normalized_line.startswith("denominator of basic eps")
            or normalized_line.startswith("weighted average number of outstanding shares")
        ):
            nums = _strip_leading_note_values(inline_values or _collect_following_number_lines())
            nearby = capped_lines[max(0, idx - 16) : idx + 1]
            selected = nums[0] if _statement_current_period_first(nearby) and nums else _pick_contextual_values(nums)
            if selected is not None and selected >= 1_000:
                basic = _normalize_depositary_metric(
                    "weighted_avg_shares_basic",
                    line,
                    selected,
                    ordinary_shares_per_depositary_share,
                )
        if diluted is None and normalized_line.startswith("denominator of diluted eps"):
            nums = _strip_leading_note_values(inline_values or _collect_following_number_lines())
            nearby = capped_lines[max(0, idx - 16) : idx + 1]
            selected = nums[0] if _statement_current_period_first(nearby) and nums else _pick_contextual_values(nums)
            if selected is not None and selected >= 1_000:
                diluted = _normalize_depositary_metric(
                    "weighted_avg_shares_diluted",
                    line,
                    selected,
                    ordinary_shares_per_depositary_share,
                )
        if basic is not None and diluted is not None:
            continue
        if basic is None and "weighted average number of outstanding common shares" in normalized_line:
            previous_context = " ".join(_normalize_label(item) for item in lines[max(0, idx - 8) : idx])
            previous_context = " ".join(_normalize_label(item) for item in capped_lines[max(0, idx - 8) : idx])
            if "diluted" not in previous_context:
                nums: list[float] = []
                for follow in capped_lines[idx + 1 : idx + 6]:
                    if not _is_number_line(follow):
                        if nums:
                            break
                        continue
                    parsed = _parse_number(follow)
                    if parsed is None or parsed < 1_000 or _is_year_like_value(parsed):
                        continue
                    nums.append(parsed)
                if nums:
                    basic = _normalize_depositary_metric(
                        "weighted_avg_shares_basic",
                        line,
                        nums[0],
                        ordinary_shares_per_depositary_share,
                    )

        if diluted is None and "weighted average number of common shares for diluted earnings per share" in normalized_line:
            nums = []
            for follow in capped_lines[idx + 1 : idx + 6]:
                if not _is_number_line(follow):
                    if nums:
                        break
                    continue
                parsed = _parse_number(follow)
                if parsed is None or parsed < 1_000 or _is_year_like_value(parsed):
                    continue
                nums.append(parsed)
            if nums:
                diluted = _normalize_depositary_metric(
                    "weighted_avg_shares_diluted",
                    line,
                    nums[0],
                    ordinary_shares_per_depositary_share,
                )

        basic = basic or _pick("basic")
        diluted = diluted or _pick("diluted")
        if basic is not None or diluted is not None:
            continue

        inline_basic = re.search(
            r"(?i)weighted average[^\\n]{0,160}basic[^\\n]{0,80}?was\s+([0-9]+(?:\.[0-9]+)?(?:\s*(?:million|billion|thousand))?|[0-9]{1,3}(?:,[0-9]{3})+)",
            line,
        )
        if inline_basic:
            inline_value = _parse_line(inline_basic.group(1))
            if inline_value is not None and inline_value >= 1_000:
                basic = _normalize_depositary_metric(
                    "weighted_avg_shares_basic",
                    line,
                    inline_value,
                    ordinary_shares_per_depositary_share,
                )
        inline_diluted = re.search(
            r"(?i)weighted average[^\\n]{0,160}diluted[^\\n]{0,80}?was\s+([0-9]+(?:\.[0-9]+)?(?:\s*(?:million|billion|thousand))?|[0-9]{1,3}(?:,[0-9]{3})+)",
            line,
        )
        if inline_diluted:
            inline_value = _parse_line(inline_diluted.group(1))
            if inline_value is not None and inline_value >= 1_000:
                diluted = _normalize_depositary_metric(
                    "weighted_avg_shares_diluted",
                    line,
                    inline_value,
                    ordinary_shares_per_depositary_share,
                )

    if basic is None or diluted is None:
        def _pick_year_table_value(idx: int, values: list[float]) -> float | None:
            if not values:
                return None
            nearby = capped_lines[max(0, idx - 16) : idx + 1]
            if _statement_current_period_first(nearby):
                return values[0]
            if prefer_year_end_tail and len(values) >= 3:
                return values[-1]
            return values[0]

        for idx, line in enumerate(capped_lines):
            normalized_line = _normalize_label(line)
            if basic is None and (
                normalized_line.startswith("weighted average number of outstanding shares")
                or normalized_line.startswith("denominator of basic eps")
            ):
                values = [
                    value
                    for value in _extract_inline_values(line, 1.0)
                    if value is not None and not _is_year_like_value(value) and abs(value) >= 1_000
                ]
                selected = _pick_year_table_value(idx, values)
                if selected is not None:
                    basic = _normalize_depositary_metric(
                        "weighted_avg_shares_basic",
                        line,
                        selected,
                        ordinary_shares_per_depositary_share,
                    )
            if diluted is None and normalized_line.startswith("denominator of diluted eps"):
                values = [
                    value
                    for value in _extract_inline_values(line, 1.0)
                    if value is not None and not _is_year_like_value(value) and abs(value) >= 1_000
                ]
                selected = _pick_year_table_value(idx, values)
                if selected is not None:
                    diluted = _normalize_depositary_metric(
                        "weighted_avg_shares_diluted",
                        line,
                        selected,
                        ordinary_shares_per_depositary_share,
                    )
            if basic is not None and diluted is not None:
                break
    return basic, diluted


def _snapshot_depositary_sensitive_metrics(report: FilingReport) -> dict[str, float | None]:
    return {
        metric: getattr(report, metric)
        for metric in sorted(SHARE_COUNT_METRICS | EPS_METRICS)
    }


def _restore_depositary_sensitive_metrics(
    report: FilingReport,
    snapshot: dict[str, float | None],
    ordinary_shares_per_depositary_share: float | None,
) -> None:
    for metric, value in snapshot.items():
        setattr(
            report,
            metric,
            _normalize_depositary_metric(metric, None, value, ordinary_shares_per_depositary_share),
        )


def _apply_6k_metrics(
    report: FilingReport,
    lines: list[str],
    currency_unit: str | None,
    period_end_date: str | None,
    ordinary_shares_per_depositary_share: float | None,
    supplemental_lines: list[str] | None = None,
) -> None:
    income_candidates = _capture_report_metric_candidates(
        report,
        SIX_K_INCOME_CANDIDATE_METRICS,
        source="existing",
    )
    income_metrics = _extract_income_statement_metrics_6k(
        lines,
        currency_unit,
        ordinary_shares_per_depositary_share,
    )
    _append_metric_candidates(
        income_candidates,
        income_metrics,
        source="statement",
        metrics=SIX_K_INCOME_CANDIDATE_METRICS,
    )
    summary_metrics = _extract_summary_table_metrics_6k(
        lines,
        currency_unit,
        ordinary_shares_per_depositary_share,
    )
    _append_metric_candidates(
        income_candidates,
        summary_metrics,
        source="summary",
        metrics=(
            "revenue",
            "operating_income",
            "pretax_income",
            "tax_expense",
            "net_income",
            "weighted_avg_shares_basic",
            "weighted_avg_shares_diluted",
        ),
    )
    resolved_income_metrics = _resolve_6k_income_metric_candidates(
        income_candidates,
        currency_unit,
    )
    for metric, value in resolved_income_metrics.items():
        if value is not None:
            setattr(report, metric, value)
    if income_metrics.get("sga") is not None:
        report.sga = income_metrics["sga"]
        if income_metrics.get("selling_and_marketing") is None:
            report.selling_and_marketing = None
        if income_metrics.get("general_and_administrative") is None:
            report.general_and_administrative = None
    if report.revenue is not None and report.cogs is not None:
        derived_gross_profit = report.revenue - abs(report.cogs)
        if report.gross_profit is None or (
            income_metrics.get("gross_profit") is None
            and abs((report.gross_profit or 0.0) - derived_gross_profit)
            > max(abs(derived_gross_profit) * 0.2, _unit_multiplier(currency_unit) * 5)
        ):
            report.gross_profit = derived_gross_profit

    bs_metrics = _extract_balance_sheet_metrics_6k(lines, currency_unit)
    for metric, value in bs_metrics.items():
        if value is not None:
            setattr(report, metric, value)
    for metric in ("goodwill", "deferred_revenue", "short_term_investments"):
        if bs_metrics.get(metric) is None:
            current_value = getattr(report, metric)
            if current_value is not None:
                setattr(report, metric, None)

    cash_flow_metrics = _extract_cash_flow_metrics_6k(lines, currency_unit)
    for metric, value in cash_flow_metrics.items():
        if value is not None:
            setattr(report, metric, value)
    if supplemental_lines:
        supplemental_cash_flow_metrics = _extract_cash_flow_metrics_6k(supplemental_lines, currency_unit)
        for metric, value in supplemental_cash_flow_metrics.items():
            if value is not None and getattr(report, metric) is None:
                setattr(report, metric, value)

    reconciliation_sbc = _extract_share_based_compensation_6k(lines, currency_unit)
    if reconciliation_sbc is not None:
        report.share_based_compensation = reconciliation_sbc
    elif report.share_based_compensation is not None and abs(report.share_based_compensation) <= _unit_multiplier(currency_unit) * 5:
        report.share_based_compensation = None
    if report.depreciation_and_amortization is None:
        reconciliation_da = _extract_reconciliation_metric_6k(
            lines,
            currency_unit,
            ["depreciation and amortization"],
        )
        if reconciliation_da is not None:
            report.depreciation_and_amortization = reconciliation_da
    if (
        report.depreciation_and_amortization is not None
        and report.revenue is not None
        and abs(report.depreciation_and_amortization) > abs(report.revenue) * 0.4
    ):
        derived_ebitda = None
        if report.operating_income is not None:
            derived_ebitda = report.operating_income + report.depreciation_and_amortization
        report.depreciation_and_amortization = None
        if report.ebitda is not None and derived_ebitda is not None and abs(report.ebitda - derived_ebitda) <= max(abs(derived_ebitda) * 0.05, _unit_multiplier(currency_unit) * 5):
            report.ebitda = None

    share_candidates = _capture_report_metric_candidates(
        report,
        SIX_K_SHARE_CANDIDATE_METRICS,
        source="existing",
    )

    eps_basic, eps_diluted = _extract_depositary_eps_metrics_6k(lines)
    if eps_basic is not None:
        _append_metric_candidates(
            share_candidates,
            {"eps_basic": eps_basic},
            source="depositary_eps",
            metrics=("eps_basic",),
        )
    if eps_diluted is not None:
        _append_metric_candidates(
            share_candidates,
            {"eps_diluted": eps_diluted},
            source="depositary_eps",
            metrics=("eps_diluted",),
        )

    explicit_shares = _extract_shares_from_text(
        lines,
        period_end_date,
        ordinary_shares_per_depositary_share,
    )
    if explicit_shares is not None:
        _append_metric_candidates(
            share_candidates,
            {"shares_outstanding": explicit_shares},
            source="text_shares",
            metrics=("shares_outstanding",),
        )

    weighted_basic, weighted_diluted = _extract_weighted_shares_from_text(
        lines,
        period_end_date,
        ordinary_shares_per_depositary_share,
    )
    if weighted_basic is not None:
        _append_metric_candidates(
            share_candidates,
            {"weighted_avg_shares_basic": weighted_basic},
            source="text_weighted",
            metrics=("weighted_avg_shares_basic",),
        )
    if weighted_diluted is not None:
        _append_metric_candidates(
            share_candidates,
            {"weighted_avg_shares_diluted": weighted_diluted},
            source="text_weighted",
            metrics=("weighted_avg_shares_diluted",),
        )

    period_dt = _parse_date(period_end_date)
    resolved_share_metrics = _resolve_6k_share_metric_candidates(
        share_candidates,
        prefer_year_end_text_weighted=bool(
            period_dt is not None and period_dt.month == 12 and period_dt.day >= 25
        ),
        has_depositary_ratio=bool(
            ordinary_shares_per_depositary_share
            and ordinary_shares_per_depositary_share not in (0.0, 1.0)
        ),
    )
    for metric, value in resolved_share_metrics.items():
        if value is not None:
            setattr(report, metric, value)

    if period_dt is not None and period_dt.month == 12 and period_dt.day >= 25:
        implied_basic = _safe_ratio(report.net_income, report.weighted_avg_shares_basic)
        if (
            implied_basic is not None
            and report.eps_basic is not None
            and abs(implied_basic) > max(abs(report.eps_basic) * 2.0, 1.0)
        ):
            report.eps_basic = round(implied_basic, 2)
        implied_diluted = _safe_ratio(report.net_income, report.weighted_avg_shares_diluted)
        if (
            implied_diluted is not None
            and report.eps_diluted is not None
            and abs(implied_diluted) > max(abs(report.eps_diluted) * 2.0, 1.0)
        ):
            report.eps_diluted = round(implied_diluted, 2)


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _finalize_report(report: FilingReport) -> None:
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

    if report.sga is None:
        if report.selling_and_marketing is not None or report.general_and_administrative is not None:
            report.sga = (report.selling_and_marketing or 0.0) + (report.general_and_administrative or 0.0)

    if report.gross_profit is None and report.revenue is not None and report.cogs is not None:
        report.gross_profit = report.revenue - report.cogs
    elif (
        report.gross_profit is not None
        and report.revenue is not None
        and report.cogs is not None
        and (report.gross_profit > report.revenue or report.gross_profit < -report.revenue)
    ):
        report.gross_profit = report.revenue - report.cogs

    if report.tax_expense is not None and report.pretax_income is not None and report.net_income is not None:
        as_is_gap = abs((report.pretax_income - report.tax_expense) - report.net_income)
        flipped_gap = abs((report.pretax_income + report.tax_expense) - report.net_income)
        if flipped_gap + 1 < as_is_gap:
            report.tax_expense *= -1

    if report.capex is not None:
        report.capex = -abs(report.capex)
    if report.acquisitions is not None:
        report.acquisitions = -abs(report.acquisitions)

    derived_ebitda = None
    if report.operating_income is not None and report.depreciation_and_amortization is not None:
        derived_ebitda = report.operating_income + report.depreciation_and_amortization

    if derived_ebitda is not None:
        if report.ebitda is None:
            report.ebitda = derived_ebitda
        else:
            gap = abs(report.ebitda - derived_ebitda)
            base = max(1.0, abs(report.ebitda), abs(derived_ebitda))
            if gap / base > 0.2:
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
        and (
            report.shares_outstanding < report.weighted_avg_shares_basic * 0.2
            or report.shares_outstanding > report.weighted_avg_shares_basic * 3.0
        )
    ):
        report.shares_outstanding = report.weighted_avg_shares_basic

    if report.equity is None and report.total_assets is not None and report.total_liabilities is not None:
        report.equity = report.total_assets - report.total_liabilities

    report.gross_margin = _safe_ratio(report.gross_profit, report.revenue)
    report.operating_margin = _safe_ratio(report.operating_income, report.revenue)
    report.net_margin = _safe_ratio(report.net_income, report.revenue)
    report.ebitda_margin = _safe_ratio(report.ebitda, report.revenue)
    report.return_on_equity = _safe_ratio(report.net_income, report.equity)
    report.return_on_assets = _safe_ratio(report.net_income, report.total_assets)
    report.current_ratio = _safe_ratio(report.assets_current, report.liabilities_current)
    report.debt_to_equity = _safe_ratio(report.total_liabilities, report.equity)
    report.asset_turnover = _safe_ratio(report.revenue, report.total_assets)


def parse_filing(path: Path) -> FilingReport | None:
    if path.suffix.lower() not in TEXT_EXTENSIONS:
        return None
    try:
        raw = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None

    form_type = _detect_form_type(path, raw)
    if form_type is None:
        return None

    if form_type in {"10-K", "10-Q", "20-F"}:
        return parse_periodic_filing_v2(path)

    company_name = _extract_non_numeric_fact(raw, "dei:EntityRegistrantName")
    if company_name is None:
        header_match = re.search(r"(?i)COMPANY CONFORMED NAME:\s*(.+)", raw)
        if header_match:
            company_name = header_match.group(1).strip(" \t\r\n.;")

    period_end_date = _extract_period_end(raw, form_type)
    filing_date = _extract_filing_date(path, raw)
    target_end = _parse_date(period_end_date)

    six_k_selection = None
    if form_type == "6-K":
        six_k_selection = build_6k_selection(
            raw,
            period_end_date,
            extract_sections=_extract_sec_document_sections,
            strip_html_to_lines=_strip_html_to_lines,
            is_periodic_callback=_is_periodic_financial_results_6k,
        )
        if six_k_selection is None:
            return None
        full_raw_lines = _strip_html_to_lines(raw)
        if _is_bank_like_6k(full_raw_lines, company_name):
            analysis_raw = raw
            lines = full_raw_lines
            share_source_lines = full_raw_lines
        else:
            analysis_raw = six_k_selection.primary_raw
            lines = six_k_selection.primary_lines
            share_source_lines = six_k_selection.share_lines
    else:
        analysis_raw = _select_analysis_raw(raw, form_type)
        lines = _strip_html_to_lines(analysis_raw)
        share_source_lines = lines

    ticker = _extract_ticker_from_path(path) or _extract_non_numeric_fact(raw, "dei:TradingSymbol") or path.parent.name.upper()
    currency_unit = _extract_currency_unit(analysis_raw)
    statement_currency_unit = _infer_currency_unit_from_statement_headers(lines)
    ordinary_shares_per_depositary_share = (
        _resolve_depositary_share_ratio(path, form_type, lines) if form_type in {"6-K", "20-F"} else None
    )

    all_tag_names = {tag for tags in METRIC_TAGS.values() for tag in tags}
    contexts = _parse_contexts(raw)
    facts = _extract_inline_facts(raw)
    seen_keys = {(fact.name, fact.context_ref, fact.value) for fact in facts}
    for fact in _extract_tag_facts(raw, all_tag_names):
        key = (fact.name, fact.context_ref, fact.value)
        if key not in seen_keys:
            facts.append(fact)
            seen_keys.add(key)

    facts_by_name: dict[str, list[Fact]] = {}
    for fact in facts:
        facts_by_name.setdefault(fact.name, []).append(fact)

    report = FilingReport(
        file_path=str(path),
        ticker=ticker,
        company_name=company_name,
        form_type=form_type,
        form_explanation=FORM_EXPLANATIONS[form_type],
        filing_date=filing_date,
        period_end_date=period_end_date,
        currency_unit=currency_unit,
    )

    for metric in METRIC_TAGS:
        if metric == "ebitda_direct":
            continue
        if metric == "shares_outstanding":
            value = _select_shares_outstanding(facts_by_name, contexts, target_end)
        else:
            value = _select_metric(metric, facts_by_name, contexts, target_end, form_type)
        if metric in SHARE_COUNT_METRICS | EPS_METRICS:
            value = _normalize_depositary_metric(
                metric,
                None,
                value,
                ordinary_shares_per_depositary_share,
            )
        setattr(report, metric, value)

    direct_ebitda = _select_metric("ebitda_direct", facts_by_name, contexts, target_end, form_type)
    report.ebitda = direct_ebitda

    multiplier = _unit_multiplier(currency_unit)
    has_inline_xbrl_facts = bool(facts)
    for metric in TEXT_LABELS:
        if getattr(report, metric, None) is not None:
            continue
        if (
            has_inline_xbrl_facts
            and form_type in {"10-K", "10-Q", "20-F"}
            and metric in INLINE_XBRL_TEXT_FALLBACK_BLOCKLIST
        ):
            continue
        value = _extract_text_metric(metric, lines, multiplier)
        if value is not None:
            if metric in SHARE_COUNT_METRICS | EPS_METRICS:
                value = _normalize_depositary_metric(
                    metric,
                    None,
                    value,
                    ordinary_shares_per_depositary_share,
                )
            setattr(report, metric, value)

    if form_type == "20-F":
        _apply_20f_statement_fallbacks(report, lines, currency_unit)

    share_metric_snapshot = _snapshot_depositary_sensitive_metrics(report)

    if form_type in {"6-K", "20-F"}:
        preview_text = " ".join(lines[:400])
        narrative_preferred = {
            "revenue",
            "cogs",
            "operating_income",
            "pretax_income",
            "tax_expense",
            "net_income",
            "interest_income",
            "ebitda",
            "operating_cash_flow",
            "investing_cash_flow",
            "financing_cash_flow",
        }
        bank_like_6k = form_type == "6-K" and _is_bank_like_6k(lines, company_name)
        if bank_like_6k:
            _apply_bank_like_6k_metrics(report, analysis_raw, lines)
        else:
            for metric in NARRATIVE_PATTERNS:
                narrative_value = _extract_narrative_metric(metric, preview_text)
                if narrative_value is None:
                    continue
                attr_name = "ebitda" if metric == "ebitda" else metric
                current_value = getattr(report, attr_name)
                if metric in narrative_preferred or current_value is None or current_value < narrative_value * 0.2 or current_value > narrative_value * 5:
                    setattr(report, attr_name, narrative_value)

            if form_type == "6-K":
                _apply_6k_metrics(
                    report,
                    lines,
                    currency_unit,
                    period_end_date,
                    ordinary_shares_per_depositary_share,
                    share_source_lines if share_source_lines != lines else None,
                )
                if ordinary_shares_per_depositary_share is None:
                    inferred_ratio = _infer_depositary_share_ratio_6k(lines, report)
                    if inferred_ratio is not None:
                        ordinary_shares_per_depositary_share = inferred_ratio
                        _restore_depositary_sensitive_metrics(
                            report,
                            share_metric_snapshot,
                            ordinary_shares_per_depositary_share,
                        )
                        _apply_6k_metrics(
                            report,
                            lines,
                            currency_unit,
                            period_end_date,
                            ordinary_shares_per_depositary_share,
                            share_source_lines if share_source_lines != lines else None,
                        )
                if share_source_lines != lines:
                    explicit_shares = _extract_shares_from_text(
                        share_source_lines,
                        period_end_date,
                        ordinary_shares_per_depositary_share,
                    )
                    if explicit_shares is not None:
                        reference_shares = report.weighted_avg_shares_basic or report.shares_outstanding
                        if reference_shares is None or (0.5 <= explicit_shares / reference_shares <= 1.5):
                            report.shares_outstanding = explicit_shares
                    weighted_basic, weighted_diluted = _extract_weighted_shares_from_text(
                        share_source_lines,
                        period_end_date,
                        ordinary_shares_per_depositary_share,
                    )
                    if report.weighted_avg_shares_basic is None and weighted_basic is not None:
                        report.weighted_avg_shares_basic = weighted_basic
                    if report.weighted_avg_shares_diluted is None and weighted_diluted is not None:
                        report.weighted_avg_shares_diluted = weighted_diluted

        if form_type == "20-F":
            _apply_20f_operations_statement_fallbacks(report, lines, currency_unit)

    _maybe_correct_duplicate_thousand_scale_annual_report(report, lines, statement_currency_unit)
    _finalize_report(report)
    return report


def _parse_filing_path(path_str: str) -> FilingReport | None:
    return parse_filing(Path(path_str))


def _scan_candidate_paths(directory: Path) -> list[Path]:
    return [
        path
        for path in sorted(directory.rglob("*"))
        if path.is_file() and path.suffix.lower() in TEXT_EXTENSIONS
    ]


def _resolve_scan_workers(workers: int | None) -> int:
    if workers is None:
        return 1
    if workers <= 0:
        cpu_total = os.cpu_count() or 1
        return max(1, cpu_total - 1)
    return max(1, workers)


def _scan_executor_context() -> multiprocessing.context.BaseContext | None:
    if sys.platform == "win32":
        return None
    try:
        return multiprocessing.get_context("fork")
    except ValueError:
        return None


def _render_scan_progress(completed: int, total: int, start_time: float, workers: int, timed_out: int = 0) -> str:
    elapsed = max(time.monotonic() - start_time, 1e-9)
    rate = completed / elapsed
    eta_seconds = (total - completed) / rate if completed and completed < total else 0.0
    pct = (completed / total * 100.0) if total else 100.0
    line = (
        f"[scan] {completed}/{total} {pct:5.1f}%  "
        f"workers={workers}  elapsed={elapsed:6.1f}s  "
        f"rate={rate:6.1f}/s  eta={eta_seconds:6.1f}s"
    )
    if timed_out:
        line += f"  timed_out={timed_out}"
    return line


def _emit_scan_progress(
    stream: TextIO,
    *,
    completed: int,
    total: int,
    start_time: float,
    workers: int,
    timed_out: int = 0,
    done: bool = False,
) -> None:
    line = _render_scan_progress(completed, total, start_time, workers, timed_out)
    if getattr(stream, "isatty", lambda: False)():
        end = "\n" if done else "\r"
        print(line.ljust(96), file=stream, end=end, flush=True)
    else:
        print(line, file=stream, flush=True)


def _scan_child_main(path_str: str, conn: multiprocessing.connection.Connection) -> None:
    try:
        conn.send(parse_filing(Path(path_str)))
    except BaseException:
        try:
            conn.send(None)
        except BaseException:
            pass
    finally:
        conn.close()


def _terminate_scan_child(
    process: multiprocessing.process.BaseProcess,
    conn: multiprocessing.connection.Connection | None,
) -> None:
    if conn is not None:
        try:
            conn.close()
        except OSError:
            pass
    if process.is_alive():
        process.terminate()
        process.join(timeout=0.2)
        if process.is_alive():
            process.kill()
    process.join(timeout=0.2)


def _parse_filing_serial_with_timeout(
    path: Path,
    timeout_seconds: float | None,
    mp_context: multiprocessing.context.BaseContext | None,
) -> tuple[FilingReport | None, bool]:
    if timeout_seconds is None or timeout_seconds <= 0:
        return parse_filing(path), False
    if sys.platform != "win32":
        previous_handler = signal.getsignal(signal.SIGALRM)

        def _handle_timeout(signum: int, frame: Any) -> None:
            raise TimeoutError

        try:
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
            return parse_filing(path), False
        except TimeoutError:
            return None, True
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0.0)
            signal.signal(signal.SIGALRM, previous_handler)
    context = mp_context or multiprocessing.get_context()
    parent_conn, child_conn = context.Pipe(duplex=False)
    process = context.Process(target=_scan_child_main, args=(str(path), child_conn))
    process.start()
    child_conn.close()
    timed_out = not parent_conn.poll(timeout_seconds)
    report = None
    if not timed_out and parent_conn.poll():
        try:
            report = parent_conn.recv()
        except EOFError:
            report = None
    _terminate_scan_child(process, parent_conn)
    return report, timed_out


def scan_directory(
    directory: Path,
    *,
    workers: int = 1,
    show_progress: bool = False,
    progress_every: int = DEFAULT_PROGRESS_EVERY,
    progress_stream: TextIO | None = None,
    file_timeout_seconds: float | None = DEFAULT_FILE_TIMEOUT_SECONDS,
) -> list[FilingReport]:
    candidate_paths = _scan_candidate_paths(directory)
    if not candidate_paths:
        return []

    resolved_workers = min(_resolve_scan_workers(workers), len(candidate_paths))
    progress_stream = progress_stream or sys.stderr
    progress_every = max(1, progress_every)
    start_time = time.monotonic()
    timed_out = 0
    mp_context = _scan_executor_context() or multiprocessing.get_context()

    if resolved_workers == 1:
        reports: list[FilingReport] = []
        total = len(candidate_paths)
        for completed, path in enumerate(candidate_paths, start=1):
            report, did_timeout = _parse_filing_serial_with_timeout(path, file_timeout_seconds, mp_context)
            if report is not None:
                reports.append(report)
            if did_timeout:
                timed_out += 1
            if show_progress and (completed == 1 or completed == total or completed % progress_every == 0):
                _emit_scan_progress(
                    progress_stream,
                    completed=completed,
                    total=total,
                    start_time=start_time,
                    workers=resolved_workers,
                    timed_out=timed_out,
                    done=completed == total,
                )
        return reports

    total = len(candidate_paths)
    indexed_reports: list[FilingReport | None] = [None] * total
    pending = list(enumerate(candidate_paths))
    active: list[dict[str, Any]] = []
    completed = 0

    while pending or active:
        while pending and len(active) < resolved_workers:
            index, path = pending.pop(0)
            parent_conn, child_conn = mp_context.Pipe(duplex=False)
            process = mp_context.Process(target=_scan_child_main, args=(str(path), child_conn))
            process.start()
            child_conn.close()
            active.append(
                {
                    "index": index,
                    "process": process,
                    "conn": parent_conn,
                    "start": time.monotonic(),
                }
            )

        progress_made = False
        for item in list(active):
            process = item["process"]
            conn = item["conn"]
            index = item["index"]
            elapsed = time.monotonic() - item["start"]
            did_timeout = bool(file_timeout_seconds and file_timeout_seconds > 0 and elapsed >= file_timeout_seconds)
            if did_timeout:
                _terminate_scan_child(process, conn)
                indexed_reports[index] = None
                active.remove(item)
                completed += 1
                timed_out += 1
                progress_made = True
            elif conn.poll():
                try:
                    indexed_reports[index] = conn.recv()
                except EOFError:
                    indexed_reports[index] = None
                _terminate_scan_child(process, conn)
                active.remove(item)
                completed += 1
                progress_made = True
            elif not process.is_alive():
                _terminate_scan_child(process, conn)
                indexed_reports[index] = None
                active.remove(item)
                completed += 1
                progress_made = True

            if progress_made and show_progress and (
                completed == 1 or completed == total or completed % progress_every == 0
            ):
                _emit_scan_progress(
                    progress_stream,
                    completed=completed,
                    total=total,
                    start_time=start_time,
                    workers=resolved_workers,
                    timed_out=timed_out,
                    done=completed == total,
                )

        if not progress_made:
            time.sleep(0.02)

    return [report for report in indexed_reports if report is not None]


def _format_money(value: float | None) -> str:
    if value is None:
        return "---"
    sign = "-" if value < 0 else ""
    amount = abs(value)
    if amount >= 1_000_000_000:
        return f"{sign}${amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"{sign}${amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"{sign}${amount / 1_000:.1f}K"
    return f"{sign}${amount:.2f}"


def _format_number(value: float | None) -> str:
    if value is None:
        return "---"
    sign = "-" if value < 0 else ""
    amount = abs(value)
    if amount >= 1_000_000_000:
        return f"{sign}{amount / 1_000_000_000:.1f}B"
    if amount >= 1_000_000:
        return f"{sign}{amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"{sign}{amount / 1_000:.1f}K"
    return f"{sign}{amount:.2f}"


def _format_eps(value: float | None) -> str:
    if value is None:
        return "---"
    return f"{value:.2f}"


def _format_pct(value: float | None) -> str:
    if value is None:
        return "---"
    return f"{value * 100:.1f}%"


def _format_ratio(value: float | None) -> str:
    if value is None:
        return "---"
    return f"{value:.2f}"


def _quad_line(
    label_1: str,
    value_1: str,
    label_2: str,
    value_2: str,
    label_3: str,
    value_3: str,
    label_4: str,
    value_4: str,
) -> str:
    return (
        f"|  {label_1:<10}{value_1:>11}  {label_2:<10}{value_2:>11}  "
        f"{label_3:<10}{value_3:>11}  {label_4:<10}{value_4:>11}"
    )


def format_report(report: FilingReport) -> str:
    lines = [
        f"+-- {report.ticker or '---'} -- {report.form_type} -- {report.company_name or '---'}",
        f"|  Date: {report.filing_date or '---'}  |  Period: {report.period_end_date or '---'}  |  {(report.currency_unit or '---')}(dom)",
        _quad_line(
            "Revenue:",
            _format_money(report.revenue),
            "COGS:",
            _format_money(report.cogs),
            "Gross:",
            _format_money(report.gross_profit),
            "OpInc:",
            _format_money(report.operating_income),
        ),
        _quad_line(
            "R&D:",
            _format_money(report.research_and_development),
            "S&M:",
            _format_money(report.selling_and_marketing),
            "G&A:",
            _format_money(report.general_and_administrative),
            "SGA:",
            _format_money(report.sga),
        ),
        _quad_line(
            "PreTax:",
            _format_money(report.pretax_income),
            "Tax:",
            _format_money(report.tax_expense),
            "Net:",
            _format_money(report.net_income),
            "EBITDA:",
            _format_money(report.ebitda),
        ),
        f"|  EPS: {_format_eps(report.eps_basic)} / {_format_eps(report.eps_diluted)}  IntInc: {_format_money(report.interest_income)}",
        _quad_line(
            "Assets:",
            _format_money(report.total_assets),
            "CurAst:",
            _format_money(report.assets_current),
            "Liab:",
            _format_money(report.total_liabilities),
            "CurLiab:",
            _format_money(report.liabilities_current),
        ),
        _quad_line(
            "Equity:",
            _format_money(report.equity),
            "Cash:",
            _format_money(report.cash),
            "STInv:",
            _format_money(report.short_term_investments),
            "Goodwill:",
            _format_money(report.goodwill),
        ),
        _quad_line(
            "AR:",
            _format_money(report.accounts_receivable),
            "AP:",
            _format_money(report.accounts_payable),
            "DefRev:",
            _format_money(report.deferred_revenue),
            "RetEarn:",
            _format_money(report.retained_earnings),
        ),
        _quad_line(
            "OpCF:",
            _format_money(report.operating_cash_flow),
            "InvCF:",
            _format_money(report.investing_cash_flow),
            "FinCF:",
            _format_money(report.financing_cash_flow),
            "FCF:",
            _format_money(report.free_cash_flow),
        ),
        _quad_line(
            "D&A:",
            _format_money(report.depreciation_and_amortization),
            "SBC:",
            _format_money(report.share_based_compensation),
            "CapEx:",
            _format_money(report.capex),
            "Acq:",
            _format_money(report.acquisitions),
        ),
        _quad_line(
            "Shares:",
            _format_number(report.shares_outstanding),
            "WtdAvg:",
            _format_number(report.weighted_avg_shares_basic),
            "Diluted:",
            _format_number(report.weighted_avg_shares_diluted),
            "",
            "",
        ),
        f"|  Gross: {_format_pct(report.gross_margin):>7}  Op: {_format_pct(report.operating_margin):>7}  Net: {_format_pct(report.net_margin):>7}  EBITDA: {_format_pct(report.ebitda_margin):>7}",
        f"|  ROE: {_format_pct(report.return_on_equity):>7}  ROA: {_format_pct(report.return_on_assets):>7}  Cur: {_format_ratio(report.current_ratio):>5}  D/E: {_format_ratio(report.debt_to_equity):>5}  ATO: {_format_ratio(report.asset_turnover):>5}",
    ]
    return "\n".join(lines)


def _to_output_dict(report: FilingReport) -> dict[str, Any]:
    payload = asdict(report)
    return {field: payload.get(field) for field in OUTPUT_FIELDS}


def _latest_per_ticker_form(reports: list[FilingReport]) -> list[FilingReport]:
    latest: dict[tuple[str, str], FilingReport] = {}
    for report in reports:
        key = ((report.ticker or "").upper(), report.form_type)
        previous = latest.get(key)
        if previous is None:
            latest[key] = report
            continue
        cur_period = _parse_date(report.period_end_date) or _parse_date(report.filing_date) or datetime.min
        prev_period = _parse_date(previous.period_end_date) or _parse_date(previous.filing_date) or datetime.min
        if cur_period >= prev_period:
            latest[key] = report
    return sorted(latest.values(), key=lambda x: ((x.ticker or ""), x.form_type))


def _resolve_ticker_scan_root(root: Path, ticker: str) -> Path | None:
    normalized = ticker.strip().upper()
    if not normalized:
        return None
    direct = root / normalized
    if direct.exists():
        return direct
    if root.exists():
        for child in root.iterdir():
            if child.name.upper() == normalized:
                return child
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read SEC filings from a directory and output a clean text/JSON report.")
    parser.add_argument("--dir", default=DEFAULT_SEC_DATA_DIR, help="Directory to scan recursively.")
    parser.add_argument("--ticker", default="", help="Scan only one ticker subdirectory under --dir, for example --ticker AAPL.")
    parser.add_argument("--format", choices=("text", "json"), default="text", help="Output format.")
    parser.add_argument("--output", default="", help="Write output to file instead of stdout.")
    parser.add_argument("--latest-per-ticker-form", action="store_true", help="Keep only the latest filing for each ticker and form type.")
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of parser workers. Use 0 for auto, 1 for serial.",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Show scan progress on stderr.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help="Refresh progress every N completed files.",
    )
    parser.add_argument(
        "--file-timeout-seconds",
        type=float,
        default=DEFAULT_FILE_TIMEOUT_SECONDS,
        help="Skip a file if parsing exceeds this many seconds. Use 0 to disable.",
    )
    parser.add_argument("--explain-forms", action="store_true", help="Print form type explanations and exit.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.explain_forms:
        text = "\n".join(f"{form}: {FORM_EXPLANATIONS[form]}" for form in SUPPORTED_FORMS)
        if args.output:
            Path(args.output).write_text(text + "\n", encoding="utf-8")
        else:
            print(text)
        return 0

    root = Path(args.dir)
    if not root.exists():
        print(f"Input directory not found: {root}")
        return 2
    if args.ticker:
        ticker_root = _resolve_ticker_scan_root(root, args.ticker)
        if ticker_root is None:
            print(f"Ticker not found under {root}: {args.ticker}")
            return 2
        root = ticker_root

    reports = scan_directory(
        root,
        workers=args.workers,
        show_progress=args.progress,
        progress_every=args.progress_every,
        file_timeout_seconds=args.file_timeout_seconds,
    )
    if args.latest_per_ticker_form:
        reports = _latest_per_ticker_form(reports)

    if args.format == "json":
        text = json.dumps([_to_output_dict(x) for x in reports], ensure_ascii=False, indent=2)
    else:
        text = "\n\n".join(format_report(x) for x in reports)

    if args.output:
        Path(args.output).write_text(text + ("\n" if not text.endswith("\n") else ""), encoding="utf-8")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
