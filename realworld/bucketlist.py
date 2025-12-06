"""Real-world bucket list recommendations."""

from typing import Dict, List
import os
import requests
import urllib.parse

from ai_utils import AIUtils


def normalize_bucketlist_themes(me_items: Dict[str, str], partner_items: Dict[str, str]) -> List[Dict[str, str]]:
    """
    Use AI to turn raw bucket list fields into a small set of 'search themes'
    like:
    - {label: "Japan winter trip", query: "skiing snowboarding shrines", type: "travel"}
    - {label: "Japanese food night", query: "ramen omakase Tokyo", type: "food"}
    - {label: "Art session", query: "painting drawing art class", type: "creative"}
    """
    ai = AIUtils()
    themes = ai.generate_bucketlist_themes(me_items, partner_items)
    return themes


def search_real_places(theme: Dict[str, str], city: str) -> List[Dict[str, str]]:
    """
    Use Google Places Text Search to find venues matching theme+city.
    Returns list of {name, address, url, type}.
    """
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key or not city:
        return []

    query = f"{theme.get('query', '')} {city}".strip()
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": api_key}

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    results = []
    for item in data.get("results", [])[:3]:  # take top 3 per theme
        place_id = item.get("place_id")
        results.append({
            "name": item.get("name", ""),
            "address": item.get("formatted_address", ""),
            "url": f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else "",
            "type": theme.get("type", ""),
        })
    return results


def build_maps_search_url(name: str, city: str) -> str:
    query = urllib.parse.quote_plus(f"{name} {city}".strip())
    return f"https://www.google.com/maps/search/?api=1&query={query}"


def get_real_world_recommendations(me_items: Dict[str, str], partner_items: Dict[str, str], city: str) -> List[Dict[str, str]]:
    """
    Full pipeline: themes -> place search -> flattened list.
    """
    if not city:
        return []

    all_places: List[Dict[str, str]] = []

    # Try Google Places first if key is present
    if os.getenv("GOOGLE_MAPS_API_KEY"):
        themes = normalize_bucketlist_themes(me_items, partner_items)
        for theme in themes:
            all_places.extend(search_real_places(theme, city))
        if all_places:
            return all_places[:6]

    # Fallback: use LLM place ideas and build Maps search links
    ai = AIUtils()
    ideas = ai.generate_bucketlist_place_ideas(me_items, partner_items, city)
    for idea in ideas:
        name = idea.get("name", "")
        if not name:
            continue
        all_places.append({
            "name": name,
            "address": idea.get("note", ""),
            "url": build_maps_search_url(name, city),
            "type": idea.get("type", "experience"),
        })

    return all_places[:6]


def get_top_bucketlist_spots(me_items: Dict[str, str], partner_items: Dict[str, str], city: str) -> List[Dict[str, str]]:
    """
    Return up to 3 'top spots' with label, name, address, url, rating, reason, type.
    """
    if not city:
        return []

    ai = AIUtils()
    themes = ai.generate_bucketlist_themes(me_items, partner_items)
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key or not themes:
        return []

    def search_top_place(theme: Dict[str, str]) -> Dict[str, str]:
        query = f"{theme.get('query', '')} {city}".strip()
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {"query": query, "key": api_key}
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return {}

        results = data.get("results", [])
        if not results:
            return {}

        best = max(
            results,
            key=lambda r: (r.get("rating", 0), r.get("user_ratings_total", 0))
        )

        name = best.get("name", "")
        if not name:
            return {}
        address = best.get("formatted_address", "")
        rating = best.get("rating", 0)
        place_id = best.get("place_id")
        place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else ""

        reason = ai.explain_why_place_fits_bucketlist(theme, name, address, rating)

        return {
            "label": theme.get("label", "Idea"),
            "name": name,
            "address": address,
            "url": place_url,
            "rating": rating,
            "reason": reason,
            "type": theme.get("type", ""),
        }

    tops: List[Dict[str, str]] = []
    for theme in themes:
        place = search_top_place(theme)
        if place:
            tops.append(place)
    return tops[:3]

