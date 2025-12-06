"""Helpers for in-chat Two Truths and a Lie via SMS/iMessage."""

import re
from typing import List


def parse_statements(text: str) -> List[str]:
    """
    Split user input into up to three statements.
    Accepts newlines or common separators like '/', '|', ';'.
    """
    parts = re.split(r"[\\n\\r/|;]+", text)
    cleaned = [p.strip() for p in parts if p and p.strip()]
    return cleaned[:3]


def format_partner_share(statements: List[str]) -> str:
    """Format a partner's statements for sharing."""
    lines = [f"{idx+1}. {stmt}" for idx, stmt in enumerate(statements)]
    body = "\n".join(lines)
    return (
        "🎲 Your partner's two truths and a lie (lie is last):\n"
        f"{body}\n\n"
        "Ask them which is which or guess together."
    )

