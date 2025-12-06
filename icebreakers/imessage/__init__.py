"""Helpers for in-chat Two Truths and a Lie via SMS/iMessage."""

import random
import re
from typing import List


def parse_statements(text: str) -> List[str]:
    """
    Split user input into up to three statements.
    Supports numbered lines (1./2./3.) and groups text until the next number.
    Falls back to common separators like newlines, '/', '|', ';'.
    """
    lines = [ln.strip() for ln in text.splitlines()]

    statements: List[str] = []
    current: List[str] = []

    def normalize(stmt: str) -> str:
        return re.sub(r"\s+", " ", stmt).strip()

    def flush_current():
        if current:
            joined = " ".join(current).strip()
            if joined:
                statements.append(normalize(joined))

    for ln in lines:
        if not ln:
            continue
        num_match = re.match(r"^\\s*(\\d+)[\\).:-]?\\s*(.*)$", ln)
        if num_match:
            # Start a new statement
            flush_current()
            remainder = num_match.group(2).strip()
            current = [remainder] if remainder else []
            continue
        current.append(ln)

    flush_current()

    if len(statements) >= 3:
        return statements[:3]

    # Fallback: simple split on separators
    parts = re.split(r"[\\n\\r/|;,]+", text)
    cleaned = [normalize(p) for p in parts if p and p.strip()]
    if len(cleaned) >= 3:
        return cleaned[:3]

    # Last resort: take the first three non-empty lines
    line_fallback = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return line_fallback[:3]


def format_partner_share(statements: List[str]) -> str:
    """Format a partner's statements for sharing, shuffled to hide the lie position."""
    def normalize(stmt: str) -> str:
        return re.sub(r"\s+", " ", stmt).strip()

    base = [normalize(s) for s in statements if s and s.strip()]
    if len(base) > 1:
        # Try to shuffle to a different order; fall back to any random order.
        for _ in range(3):
            candidate = random.sample(base, len(base))
            if candidate != base:
                base = candidate
                break
        else:
            random.shuffle(base)
    shuffled = base

    lines = [f"{idx+1}. {stmt}" for idx, stmt in enumerate(shuffled)]
    body = "\n".join(lines)
    return (
        "🎲 Your partner sent two truths and a lie (order shuffled):\n"
        f"{body}\n\n"
        "Guess together which one is the lie."
    )

