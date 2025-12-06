"""Lightweight web icebreaker host (Two Truths and a Lie).

Run separately from the SMS agent. Provides:
- GET /game?session=<id>&who=<me|partner>: simple HTML UI
- POST /api/game/submit: store statements; TODO: notify via SeriesAPI

Note: This is an in-memory demo server for hackathon use. Persist/secure as needed.
"""

import logging
import os
from typing import Dict

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

try:
    from core.series_api import SeriesAPI
except Exception:  # pragma: no cover
    SeriesAPI = None  # type: ignore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI()


@app.get("/", response_class=JSONResponse)
async def root():
    """Provide quick links for available demo routes."""
    return {
        "message": "Use /game or /bucketlist with ?session=<id>&who=<me|partner>.",
        "examples": {
            "game": "/game?session=demo&who=me",
            "bucketlist": "/bucketlist?session=demo&who=me",
            "docs": "/docs",
        },
    }

# In-memory session store: session_id -> { "statements": { "me": str, "partner": str } }
sessions: Dict[str, Dict] = {}
# In-memory bucket list store: session_id -> { "me": {...}, "partner": {...} }
bucket_sessions: Dict[str, Dict] = {}


class SubmitPayload(BaseModel):
    session: str
    who: str  # "me" or "partner"
    text: str


class BucketPayload(BaseModel):
    session: str
    who: str  # "me" or "partner"
    travel: str
    skill: str
    food: str
    adventure: str
    creative: str


def get_series_api():
    api_key = os.getenv("SERIES_API_KEY")
    if api_key and SeriesAPI:
        return SeriesAPI(api_key=api_key)
    return None


@app.get("/game", response_class=HTMLResponse)
async def game_page(session: str, who: str):
    """Serve a minimal HTML UI for Two Truths and a Lie."""
    html = f"""
    <!DOCTYPE html>
    <html>
      <head><title>Icebreaker Game</title></head>
      <body>
        <h1>Two Truths and a Lie</h1>
        <p>You are: {who}</p>
        <p>Enter three statements about yourself. Two true, one false.</p>
        <form onsubmit="submitForm(event)">
          <textarea id="statements" rows="5" cols="40"
            placeholder="statement 1 / statement 2 / statement 3"></textarea><br/>
          <button type="submit">Submit</button>
        </form>
        <div id="status"></div>
        <script>
          async function submitForm(e) {{
            e.preventDefault();
            const text = document.getElementById('statements').value;
            const resp = await fetch('/api/game/submit', {{
              method: 'POST',
              headers: {{ 'Content-Type': 'application/json' }},
              body: JSON.stringify({{ session: '{session}', who: '{who}', text }})
            }});
            const data = await resp.json();
            document.getElementById('status').innerText = data.message;
          }}
        </script>
      </body>
    </html>
    """
    return HTMLResponse(html)


@app.post("/api/game/submit")
async def submit_statements(payload: SubmitPayload):
    """Store statements; once both sides submit, inform users (TODO: SMS notify)."""
    session_id = payload.session
    who = payload.who
    text = payload.text.strip()

    if session_id not in sessions:
        sessions[session_id] = {"statements": {}}

    sessions[session_id]["statements"][who] = text

    done = len(sessions[session_id]["statements"]) == 2
    if done:
        message = "Got both sets of statements! Check your texts for a summary."

        # Optional: notify both users via Series API if you can map session->(users, chat_ids).
        # This stub leaves the notification to the main agent, which already has chat_ids.
        api = get_series_api()
        if api:
            logger.info("Both submissions received for session %s; integrate with SeriesAPI as needed.", session_id)
    else:
        message = "Got your statements. Waiting for your partner."

    return JSONResponse({"ok": True, "message": message, "done": done})


@app.get("/bucketlist", response_class=HTMLResponse)
async def bucketlist_page(session: str, who: str):
    """Serve a minimal HTML UI for Shared Bucket List Builder."""
    html = f"""
    <!DOCTYPE html>
    <html>
      <head><title>Shared Bucket List</title></head>
      <body>
        <h1>Shared Bucket List Builder</h1>
        <p>You are: {who}</p>
        <p>Fill in one item for each category.</p>
        <form onsubmit="submitForm(event)">
          <label>Travel:</label><br/>
          <input id="travel" /><br/><br/>
          <label>Skill to learn:</label><br/>
          <input id="skill" /><br/><br/>
          <label>Food experience:</label><br/>
          <input id="food" /><br/><br/>
          <label>Adventure:</label><br/>
          <input id="adventure" /><br/><br/>
          <label>Creative experience:</label><br/>
          <input id="creative" /><br/><br/>
          <button type="submit">Submit</button>
        </form>
        <div id="status"></div>
        <script>
          async function submitForm(e) {{
            e.preventDefault();
            const payload = {{
              session: "{session}",
              who: "{who}",
              travel: document.getElementById('travel').value,
              skill: document.getElementById('skill').value,
              food: document.getElementById('food').value,
              adventure: document.getElementById('adventure').value,
              creative: document.getElementById('creative').value,
            }};
            const resp = await fetch('/api/bucketlist/submit', {{
              method: 'POST',
              headers: {{ 'Content-Type': 'application/json' }},
              body: JSON.stringify(payload),
            }});
            const data = await resp.json();
            document.getElementById('status').innerText = data.message;
          }}
        </script>
      </body>
    </html>
    """
    return HTMLResponse(html)


def _merge_bucketlists(submissions: Dict[str, Dict[str, str]]) -> list:
    """Merge two users' bucket list inputs into a shared list."""
    me = submissions.get("me", {})
    partner = submissions.get("partner", {})
    items = []
    for key in ["travel", "skill", "food", "adventure", "creative"]:
        if me.get(key):
            items.append(me[key])
        if partner.get(key):
            items.append(partner[key])
    # Deduplicate while preserving order
    seen = set()
    merged = []
    for it in items:
        norm = it.strip().lower()
        if norm and norm not in seen:
            merged.append(it.strip())
            seen.add(norm)
    return merged


@app.post("/api/bucketlist/submit")
async def submit_bucketlist(payload: BucketPayload):
    """Store bucket list entries; when both submit, send summary via SeriesAPI (stub)."""
    session_id = payload.session
    if session_id not in bucket_sessions:
        bucket_sessions[session_id] = {}

    bucket_sessions[session_id][payload.who] = {
        "travel": payload.travel.strip(),
        "skill": payload.skill.strip(),
        "food": payload.food.strip(),
        "adventure": payload.adventure.strip(),
        "creative": payload.creative.strip(),
    }

    done = len(bucket_sessions[session_id]) == 2
    if done:
        shared = _merge_bucketlists(bucket_sessions[session_id])
        summary = (
            "Your shared bucket list:\n"
            + "\n".join(f"- {item}" for item in shared)
            + "\n\nAsk each other which one you'd do first."
        )

        api = get_series_api()
        # To actually notify, this server must resolve session_id -> users -> chat_ids.
        # That mapping lives in the SMS agent. Integrate via a shared store or an agent HTTP endpoint.
        if api:
            logger.info("Bucketlist complete for session %s; integrate SeriesAPI notification with chat_ids.", session_id)

        message = "Got both lists! Check your texts for your shared bucket list."
    else:
        message = "Saved your picks. Waiting for your partner to submit."

    return JSONResponse({"ok": True, "message": message, "done": done})


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

