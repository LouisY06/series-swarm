"""State machine and data structures for Social Character Sheets."""

from enum import Enum
from collections import defaultdict, deque
import time
from typing import Dict, List, Tuple, Optional


class Status(str, Enum):
    """User status in the system."""
    IDLE = "idle"
    ONBOARDING_NAME = "onboarding_name"
    ONBOARDING_EMAIL = "onboarding_email"
    ONBOARDING_INTRO = "onboarding_intro"
    ONBOARDING = "onboarding"  # Legacy, maps to ONBOARDING_INTRO
    READY = "ready"
    SEARCHING = "searching"
    PREVIEW = "preview"
    CHATTING = "chatting"


class StateManager:
    """Manages all application state."""
    
    def __init__(self):
        """Initialize state structures."""
        # User level state
        self.user_status: Dict[str, Status] = {}
        self.user_intro: Dict[str, str] = {}
        self.current_partner: Dict[str, str] = {}
        self.user_chat_ids: Dict[str, int] = {}
        self.last_user_message_ts: Dict[str, float] = defaultdict(float)
        self.last_messages: Dict[str, List[str]] = defaultdict(list)
        
        # Matching state
        self.waiting_queue: deque = deque()
        self.pending_accept: Dict[str, Dict[str, any]] = {}  # user_id -> {"partner": str, "accepted": bool}
        
        # Conversation and profile state
        self.pair_messages: Dict[Tuple[str, str], List[Dict[str, str]]] = defaultdict(list)
        self.pair_profiles: Dict[Tuple[str, str], Dict[str, Dict[str, any]]] = defaultdict(dict)
        self.pair_last_message_ts: Dict[Tuple[str, str], float] = defaultdict(float)
        
        # Icebreaker state per pair
        self.icebreakers: Dict[Tuple[str, str], Dict[str, any]] = {}
        # Web icebreaker sessions mapping
        self.icebreaker_sessions: Dict[Tuple[str, str], str] = {}
        self.icebreaker_session_users: Dict[str, Tuple[str, str]] = {}
    
    def get_status(self, user_id: str) -> Status:
        """Get user status, defaulting to IDLE for new users."""
        return self.user_status.get(user_id, Status.IDLE)
    
    def set_status(self, user_id: str, status: Status):
        """Set user status."""
        self.user_status[user_id] = status
    
    def get_intro(self, user_id: str) -> Optional[str]:
        """Get user intro."""
        return self.user_intro.get(user_id)
    
    def set_intro(self, user_id: str, intro: str):
        """Set user intro."""
        self.user_intro[user_id] = intro
    
    def get_partner(self, user_id: str) -> Optional[str]:
        """Get current partner."""
        return self.current_partner.get(user_id)
    
    def set_partner(self, user_id: str, partner_id: str):
        """Set current partner (bidirectional)."""
        self.current_partner[user_id] = partner_id
        self.current_partner[partner_id] = user_id
    
    def clear_partner(self, user_id: str):
        """Clear partner relationship (bidirectional)."""
        partner = self.current_partner.get(user_id)
        if partner:
            self.current_partner.pop(user_id, None)
            self.current_partner.pop(partner, None)
    
    def store_chat_id(self, user_id: str, chat_id: int):
        """Store chat_id for user."""
        self.user_chat_ids[user_id] = chat_id
    
    def get_chat_id(self, user_id: str) -> Optional[int]:
        """Get chat_id for user."""
        return self.user_chat_ids.get(user_id)
    
    def pair_id_for(self, a: str, b: str) -> Tuple[str, str]:
        """Get canonical pair ID (sorted tuple)."""
        return tuple(sorted([a, b]))
    
    def record_message(self, sender: str, partner: str, text: str):
        """Record a message in the conversation."""
        pid = self.pair_id_for(sender, partner)
        self.pair_messages[pid].append({
            "from": sender,
            "text": text
        })
        self.pair_last_message_ts[pid] = time.time()
        self.last_user_message_ts[sender] = time.time()
        self.last_messages[sender].append(text)
        # Keep only the last 5 messages per user
        if len(self.last_messages[sender]) > 5:
            self.last_messages[sender] = self.last_messages[sender][-5:]
    
    def get_message_count(self, user_a: str, user_b: str) -> int:
        """Get total message count for a pair."""
        pid = self.pair_id_for(user_a, user_b)
        return len(self.pair_messages[pid])
    
    def get_conversation(self, user_a: str, user_b: str) -> List[Dict[str, str]]:
        """Get full conversation history."""
        pid = self.pair_id_for(user_a, user_b)
        return self.pair_messages[pid]
    
    def get_profile(self, viewer_id: str, other_id: str) -> Dict[str, any]:
        """Get profile that viewer_id has of other_id."""
        pid = self.pair_id_for(viewer_id, other_id)
        return self.pair_profiles[pid].get(viewer_id, {
            "level": 0,
            "resume": "",
            "last_message_count": 0
        })
    
    def set_profile(self, viewer_id: str, other_id: str, profile: Dict[str, any]):
        """Set profile that viewer_id has of other_id."""
        pid = self.pair_id_for(viewer_id, other_id)
        self.pair_profiles[pid][viewer_id] = profile

    def get_last_message_ts_for_pair(self, a: str, b: str) -> float:
        """Get last message timestamp for the pair."""
        pid = self.pair_id_for(a, b)
        return self.pair_last_message_ts.get(pid, 0)

    def get_last_message_ts_for_user(self, user_id: str) -> float:
        """Get last message timestamp for a user."""
        return self.last_user_message_ts.get(user_id, 0)

    def get_last_messages_for_user(self, user_id: str) -> List[str]:
        """Get recent messages for a user (up to last 5)."""
        return self.last_messages.get(user_id, [])
    
    def add_to_queue(self, user_id: str):
        """Add user to waiting queue."""
        if user_id not in self.waiting_queue:
            self.waiting_queue.append(user_id)
    
    def pop_from_queue(self) -> Optional[str]:
        """Pop next user from waiting queue."""
        if self.waiting_queue:
            return self.waiting_queue.popleft()
        return None
    
    def set_pending_accept(self, user_id: str, partner_id: str):
        """Set pending accept state."""
        self.pending_accept[user_id] = {
            "partner": partner_id,
            "accepted": False
        }
    
    def accept_match(self, user_id: str) -> Tuple[bool, Optional[str]]:
        """Mark user as accepting match. Returns (both_accepted, partner_id)."""
        if user_id not in self.pending_accept:
            return False, None
        
        self.pending_accept[user_id]["accepted"] = True
        partner = self.pending_accept[user_id]["partner"]
        
        # Check if partner also accepted
        if partner in self.pending_accept and self.pending_accept[partner].get("accepted"):
            return True, partner
        
        return False, partner
    
    def clear_pending_accept(self, user_id: str):
        """Clear pending accept state."""
        self.pending_accept.pop(user_id, None)

    # Icebreaker helpers
    def _get_ib(self, a: str, b: str) -> Dict[str, any]:
        pid = self.pair_id_for(a, b)
        if pid not in self.icebreakers:
            self.icebreakers[pid] = {
                "active": False,
                "statements": {},
                "lie_index": {},
                "guesses": {}
            }
        return self.icebreakers[pid]

    def start_icebreaker(self, a: str, b: str):
        ib = self._get_ib(a, b)
        ib["active"] = True
        ib["statements"] = {}
        ib["lie_index"] = {}
        ib["guesses"] = {}

    def set_icebreaker_statements(self, user: str, partner: str, statements: list, lie_index: int):
        ib = self._get_ib(user, partner)
        ib["active"] = True
        ib["statements"][user] = statements
        ib["lie_index"][user] = lie_index

    def set_icebreaker_guess(self, user: str, partner: str, guess_index: int):
        ib = self._get_ib(user, partner)
        ib["guesses"][user] = guess_index

    def get_icebreaker(self, a: str, b: str) -> Dict[str, any]:
        return self._get_ib(a, b)

    def clear_icebreaker(self, a: str, b: str):
        pid = self.pair_id_for(a, b)
        if pid in self.icebreakers:
            self.icebreakers.pop(pid, None)

    def icebreaker_active(self, a: str, b: str) -> bool:
        return self._get_ib(a, b).get("active", False)

    # Web icebreaker session helpers
    def set_icebreaker_session(self, user_a: str, user_b: str, session_id: str):
        pid = self.pair_id_for(user_a, user_b)
        self.icebreaker_sessions[pid] = session_id
        self.icebreaker_session_users[session_id] = (user_a, user_b)

    def get_icebreaker_session(self, user_a: str, user_b: str) -> Optional[str]:
        pid = self.pair_id_for(user_a, user_b)
        return self.icebreaker_sessions.get(pid)

    def clear_icebreaker_session(self, user_a: str, user_b: str):
        pid = self.pair_id_for(user_a, user_b)
        sess = self.icebreaker_sessions.pop(pid, None)
        if sess:
            self.icebreaker_session_users.pop(sess, None)

    def get_users_for_session(self, session_id: str) -> Optional[Tuple[str, str]]:
        return self.icebreaker_session_users.get(session_id)

