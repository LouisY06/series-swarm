"""Helpers for in-chat Shared Bucket List."""

from typing import Dict, List
import re


CATEGORIES = ["travel", "skill", "food", "adventure", "creative"]


def parse_bucketlist(text: str) -> Dict[str, str]:
    """
    Parse up to 5 non-empty lines into a bucket list dict.
    Pads missing lines with 'skip'.
    """
    lines: List[str] = [ln.strip() for ln in text.splitlines() if ln.strip()]
    items: Dict[str, str] = {}
    for idx, cat in enumerate(CATEGORIES):
        items[cat] = lines[idx] if idx < len(lines) else "skip"
    return items


def format_captured(items: Dict[str, str]) -> str:
    """Format a single user's captured items."""
    return (
        "Got this bucket list for you:\n"
        f"Travel: {items.get('travel', '')}\n"
        f"Skill: {items.get('skill', '')}\n"
        f"Food: {items.get('food', '')}\n"
        f"Adventure: {items.get('adventure', '')}\n"
        f"Creative: {items.get('creative', '')}"
    )


def format_shared_bucketlist(me: Dict[str, str], partner: Dict[str, str]) -> str:
    """
    Simple merge: interleave and de-duplicate while preserving order.
    """
    merged: List[str] = []
    seen = set()
    ordered = [me.get(c, "") for c in CATEGORIES] + [partner.get(c, "") for c in CATEGORIES]
    for item in ordered:
        norm = item.strip().lower()
        if norm and norm not in seen and norm != "skip":
            merged.append(item.strip())
            seen.add(norm)

    body = "\n".join(f"- {m}" for m in merged) if merged else "- (no items)"
    return (
        "🌍 Your shared bucket list:\n"
        f"{body}\n\n"
        "Ask each other which one you'd do first."
    )

