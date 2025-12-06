"""Real-world bucket list recommendations."""

from typing import Dict, List, Optional, Tuple
import os
import requests
import urllib.parse

from ai_utils import AIUtils

# Cache city geocodes to avoid repeated calls
_GEOCODE_CACHE: Dict[str, Tuple[float, float]] = {}

# Places API v1 endpoints
PLACES_TEXT_ENDPOINT = "https://places.googleapis.com/v1/places:searchText"

# Field mask keeps payloads small and ensures required fields are present
FIELD_MASK = (
    "places.displayName,places.formattedAddress,"
    "places.rating,places.userRatingCount,places.googleMapsUri,places.location"
)


def geocode_city(city: str) -> Optional[Tuple[float, float]]:
    """Resolve a city name to (lat, lng) using Places Text Search v1 and cache it."""
    city_key = city.strip().lower()
    if not city_key:
        return None
    if city_key in _GEOCODE_CACHE:
        return _GEOCODE_CACHE[city_key]

    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        return None

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.location",
    }
    body = {
        "textQuery": f"{city} city",
        "maxResultCount": 1,
    }

    try:
        resp = requests.post(PLACES_TEXT_ENDPOINT, headers=headers, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        places = data.get("places", [])
        if not places:
            return None
        loc = places[0].get("location") or {}
        lat, lng = loc.get("latitude"), loc.get("longitude")
        if lat is None or lng is None:
            return None
        _GEOCODE_CACHE[city_key] = (lat, lng)
        return (lat, lng)
    except Exception:
        return None


def search_places_in_city(theme: Dict[str, str], city: str, max_results: int = 3) -> List[Dict[str, str]]:
    """
    Use Places Text Search v1 with city-biased query and optional locationBias.
    Returns list of {name, address, url, rating, type}.
    """
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key or not city:
        return []

    query = f"{theme.get('query', '')} in {city}".strip()
    latlng = geocode_city(city)

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    body = {
        "textQuery": query,
        "maxResultCount": max_results,
    }
    if latlng:
        lat, lng = latlng
        body["locationBias"] = {
            "circle": {
                "center": {"latitude": lat, "longitude": lng},
                "radius": 30000.0,  # 30km radius; adjust if needed
            }
        }

    try:
        resp = requests.post(PLACES_TEXT_ENDPOINT, headers=headers, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    results: List[Dict[str, str]] = []
    for item in data.get("places", [])[:max_results]:
        results.append(
            {
                "name": (item.get("displayName") or {}).get("text", ""),
                "address": item.get("formattedAddress", ""),
                "url": item.get("googleMapsUri", ""),
                "rating": item.get("rating", 0),
                "user_ratings_total": item.get("userRatingCount", 0),
                "type": theme.get("type", ""),
            }
        )
    return results


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
    """Backward-compat wrapper to keep existing callers working."""
    results = search_places_in_city(theme, city, max_results=3)
    # Align keys with previous structure
    aligned = []
    for r in results:
        aligned.append(
            {
                "name": r.get("name", ""),
                "address": r.get("address", ""),
                "url": r.get("url", ""),
                "type": r.get("type", ""),
            }
        )
    return aligned


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
        results = search_places_in_city(theme, city, max_results=3)
        if not results:
            return {}

        best = max(
            results,
            key=lambda r: (
                r.get("rating", 0) or 0,
                r.get("user_ratings_total", 0) or 0,
            ),
        )

        name = best.get("name", "")
        if not name:
            return {}
        address = best.get("address", "")
        rating = best.get("rating", 0)
        place_url = best.get("url", "")

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

