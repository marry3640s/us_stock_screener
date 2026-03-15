from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class SixKCandidate:
    source_id: str
    doc_type: str
    filename: str
    description: str
    raw_text: str
    lines: list[str]
    periodic: bool
    primary_score: int
    share_score: int


@dataclass(frozen=True)
class SixKSelection:
    primary_raw: str
    primary_lines: list[str]
    share_lines: list[str]
    primary_source_id: str
    share_source_id: str


def _normalize_token(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def _is_machine_readable_candidate(candidate: SixKCandidate) -> bool:
    norm_type = _normalize_token(candidate.doc_type)
    file_upper = candidate.filename.upper()
    return (
        norm_type in {"XML", "JSON", "ZIP", "GRAPHIC", "EX101SCH"}
        or file_upper.endswith((".XML", ".JSON", ".XSD", ".ZIP", ".JPG", ".PNG", ".CSS", ".JS"))
    )


def _is_release_like_candidate(candidate: SixKCandidate) -> bool:
    norm_type = _normalize_token(candidate.doc_type)
    desc_upper = candidate.description.upper()
    file_upper = candidate.filename.upper()
    preview = _candidate_preview(candidate.lines, limit=400)
    return (
        norm_type in {"EX991", "991"}
        or "99-1" in file_upper
        or "99_1" in file_upper
        or "PRESS RELEASE" in desc_upper
        or "EARNINGS RELEASE" in desc_upper
        or re.search(r"(?i)\b(?:press|earnings)\s+release\b", preview) is not None
    )


def _candidate_preview(lines: list[str], limit: int = 1200) -> str:
    return "\n".join(lines[:limit])


def _looks_like_periodic_results(
    lines: list[str],
    period_end_date: str | None,
    is_periodic_callback: Callable[[list[str], str | None], bool],
) -> bool:
    if is_periodic_callback(lines, period_end_date):
        return True

    preview = _candidate_preview(lines)
    quarter_signals = sum(
        1
        for pattern in (
            r"(?i)\bq[1-4]\b",
            r"(?i)\bfirst quarter\b",
            r"(?i)\bsecond quarter\b",
            r"(?i)\bthird quarter\b",
            r"(?i)\bfourth quarter\b",
            r"(?i)\bquarter ended\b",
            r"(?i)\bthree months ended\b",
            r"(?i)\binterim condensed consolidated financial statements\b",
            r"(?i)\breport to shareholders\b",
            r"(?i)\bselected share data\b",
            r"(?i)\bearnings release\b",
        )
        if re.search(pattern, preview)
    )
    financial_signals = sum(
        1
        for pattern in (
            r"(?i)\bbalance sheets?\b",
            r"(?i)\bstatements?\s+of\s+financial position\b",
            r"(?i)\bstatements?\s+of\s+(?:income|operations|cash flows?)\b",
            r"(?i)\bnet income\b",
            r"(?i)\bearnings per share\b",
            r"(?i)\btotal assets\b",
            r"(?i)\btotal liabilities\b",
            r"(?i)\bweighted average number of\b",
        )
        if re.search(pattern, preview)
    )
    return quarter_signals >= 1 and financial_signals >= 2


def _score_primary_candidate(candidate: SixKCandidate) -> int:
    score = 0
    norm_type = _normalize_token(candidate.doc_type)
    desc_upper = candidate.description.upper()
    file_upper = candidate.filename.upper()
    preview = _candidate_preview(candidate.lines)

    if norm_type in {"EX991", "991"}:
        score += 50
    elif norm_type.startswith("EX99"):
        score += 30
    elif norm_type == "6K":
        score += 10

    if "99-1" in file_upper or "99_1" in file_upper:
        score += 35
    if "PRESS RELEASE" in desc_upper or "EARNINGS RELEASE" in desc_upper:
        score += 30
    if "REPORT TO SHAREHOLDERS" in desc_upper:
        score += 25
    if re.search(r"(?i)\b(?:quarterly|interim|annual)\s+results\b", preview):
        score += 20
    if re.search(r"(?i)\binterim condensed consolidated financial statements\b", preview):
        score += 20
    if re.search(r"(?i)\bstatements?\s+of\s+(?:income|operations|financial position|cash flows?)\b", preview):
        score += 15

    if "PRESENTATION" in desc_upper or "SLIDE" in desc_upper or "WEBCAST" in desc_upper or "TRANSCRIPT" in desc_upper:
        score -= 60
    if re.search(r"(?i)\b(?:presentation|slide deck|webcast|conference call transcript)\b", preview):
        score -= 50

    score += min(len(candidate.lines) // 80, 10)
    if candidate.periodic:
        score += 25
    return score


def _score_share_candidate(candidate: SixKCandidate) -> int:
    score = 0
    preview = _candidate_preview(candidate.lines, limit=2500)
    if re.search(r"(?i)\bselected share data\b", preview):
        score += 80
    if re.search(r"(?i)\bcommon shares outstanding\b", preview):
        score += 60
    if re.search(r"(?i)\bweighted average number of common shares\b", preview):
        score += 50
    if re.search(r"(?i)\bweighted average\b[^\n]{0,80}\bshares?\b", preview):
        score += 25
    if re.search(r"(?i)\bearnings per share\b", preview):
        score += 15
    if re.search(r"(?i)\breport to shareholders\b", preview):
        score += 20
    if re.search(r"(?i)\binterim condensed consolidated financial statements\b", preview):
        score += 15
    score += min(len(candidate.lines) // 120, 8)
    if candidate.periodic:
        score += 10
    return score


def _build_candidates(
    raw: str,
    period_end_date: str | None,
    *,
    extract_sections: Callable[[str], list[dict[str, str]]],
    strip_html_to_lines: Callable[[str], list[str]],
    is_periodic_callback: Callable[[list[str], str | None], bool],
) -> list[SixKCandidate]:
    candidates: list[SixKCandidate] = []
    sections = extract_sections(raw)
    if sections:
        for index, section in enumerate(sections):
            lines = strip_html_to_lines(section["text"])
            periodic = _looks_like_periodic_results(lines, period_end_date, is_periodic_callback)
            provisional = SixKCandidate(
                source_id=f"section:{index}",
                doc_type=section["type"],
                filename=section["filename"],
                description=section["description"],
                raw_text=section["text"],
                lines=lines,
                periodic=periodic,
                primary_score=0,
                share_score=0,
            )
            candidates.append(
                SixKCandidate(
                    source_id=provisional.source_id,
                    doc_type=provisional.doc_type,
                    filename=provisional.filename,
                    description=provisional.description,
                    raw_text=provisional.raw_text,
                    lines=provisional.lines,
                    periodic=provisional.periodic,
                    primary_score=_score_primary_candidate(provisional),
                    share_score=_score_share_candidate(provisional),
                )
            )

    full_lines = strip_html_to_lines(raw)
    full_candidate = SixKCandidate(
        source_id="full-raw",
        doc_type="6-K",
        filename="",
        description="full filing",
        raw_text=raw,
        lines=full_lines,
        periodic=_looks_like_periodic_results(full_lines, period_end_date, is_periodic_callback),
        primary_score=0,
        share_score=0,
    )
    candidates.append(
        SixKCandidate(
            source_id=full_candidate.source_id,
            doc_type=full_candidate.doc_type,
            filename=full_candidate.filename,
            description=full_candidate.description,
            raw_text=full_candidate.raw_text,
            lines=full_candidate.lines,
            periodic=full_candidate.periodic,
            primary_score=_score_primary_candidate(full_candidate),
            share_score=_score_share_candidate(full_candidate),
        )
    )
    return candidates


def build_6k_selection(
    raw: str,
    period_end_date: str | None,
    *,
    extract_sections: Callable[[str], list[dict[str, str]]],
    strip_html_to_lines: Callable[[str], list[str]],
    is_periodic_callback: Callable[[list[str], str | None], bool],
) -> SixKSelection | None:
    candidates = _build_candidates(
        raw,
        period_end_date,
        extract_sections=extract_sections,
        strip_html_to_lines=strip_html_to_lines,
        is_periodic_callback=is_periodic_callback,
    )
    periodic_candidates = [candidate for candidate in candidates if candidate.periodic]
    if not periodic_candidates:
        return None

    primary_pool = [
        candidate
        for candidate in periodic_candidates
        if candidate.source_id != "full-raw" and not _is_machine_readable_candidate(candidate)
    ]
    release_like_pool = [candidate for candidate in primary_pool if _is_release_like_candidate(candidate)]
    primary = max(
        release_like_pool or primary_pool or periodic_candidates,
        key=lambda candidate: (candidate.primary_score, candidate.share_score, len(candidate.lines)),
    )
    share_pool = [
        candidate
        for candidate in periodic_candidates
        if candidate.share_score > 0 and not _is_machine_readable_candidate(candidate)
    ]
    share_candidate = max(
        share_pool or periodic_candidates,
        key=lambda candidate: (candidate.share_score, candidate.primary_score, len(candidate.lines)),
    )
    return SixKSelection(
        primary_raw=primary.raw_text,
        primary_lines=primary.lines,
        share_lines=share_candidate.lines,
        primary_source_id=primary.source_id,
        share_source_id=share_candidate.source_id,
    )
