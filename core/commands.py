"""Command handler for SeriesSwarm."""

import logging
import re
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def looks_like_bad_name(arg: str) -> bool:
    t = arg.strip()
    if not t:
        return True
    if t.startswith('/'):
        return True
    if '@' in t:
        return True
    if any(ch.isdigit() for ch in t):
        return True
    if len(t) > 60:
        return True
    return False


def normalize_name_input(raw: str) -> str:
    """
    Normalize free-form name text into a clean name string.

    Examples:
      "I'm Alana"      -> "Alana"
      "My name is alana kwan!!!" -> "Alana Kwan"
    """
    if not raw:
        return ""

    t = raw.strip()

    # Tokenize and strip trivial punctuation per token
    tokens = t.split()
    lower_tokens = [tok.lower().strip(",.!?") for tok in tokens]

    # Heuristics to skip leading phrases like "I'm", "I am", "My name is"
    idx = 0
    if len(tokens) >= 2:
        # "I'm Alana" / "Im Alana" / "I’m Alana"
        if lower_tokens[0] in ("i", "im", "i'm", "i’m", "iam"):
            if len(tokens) >= 3 and lower_tokens[1] == "am":
                # "I am Alana"
                idx = 2
            else:
                idx = 1
        # "My name is Alana"
        elif len(tokens) >= 3 and lower_tokens[0] == "my" and lower_tokens[1] == "name" and lower_tokens[2] == "is":
            idx = 3
        # "Name is Alana"
        elif len(tokens) >= 2 and lower_tokens[0] == "name" and lower_tokens[1] in ("is", ":"):
            idx = 2
        # "This is Alana"
        elif len(tokens) >= 2 and lower_tokens[0] == "this" and lower_tokens[1] == "is":
            idx = 2
        # "It's Alana" / "Its Alana"
        elif len(tokens) >= 2 and lower_tokens[0] in ("it's", "its"):
            idx = 2

    # Take remaining tokens as the name; if we stripped everything, fall back to original
    name_tokens = tokens[idx:] or tokens

    # Join and keep only letters, spaces, hyphens, apostrophes
    name = " ".join(name_tokens)
    name = re.sub(r"[^A-Za-z'\- ]+", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    # Title-case each word ("alana kwan" -> "Alana Kwan")
    name = " ".join(part.capitalize() for part in name.split())

    return name


class CommandHandler:
    """Handles user commands."""

    COMMANDS = {
        'match': ['/match'],
        'end': ['/end', '/next', '/skip'],
        'reveal': ['/reveal'],
        'setname': ['/setname'],
        'setemail': ['/setemail'],
        'setphone': ['/setphone'],
        'profile': ['/profile'],
        'help': ['/help'],
        'we': ['/we'],
        'mood': ['/mood'],
        'topics': ['/topics'],
        'card': ['/card', 'card'],
        'override': ['/override'],
        'setcity': ['/setcity'],
        'topspots': ['/topspots'],
        'cityyes': ['/cityyes'],
        'cityno': ['/cityno'],
        'icebreaker': ['/icebreaker'],
        'icebreakers': ['/icebreakers'],
        'bucketlist': ['/bucketlist'],
        'spotify': ['/spotify'],
        'spotifylink': ['/spotifylink'],
        'quiz': ['/quiz'],
    }

    @staticmethod
    def parse_command(text: str) -> Optional[Tuple[str, Optional[str]]]:
        """
        Parse command from text.
        Returns (command_name, argument) or None.
        """
        if not text:
            return None

        text = text.strip()
        text_lower = text.lower()

        for cmd_name, patterns in CommandHandler.COMMANDS.items():
            for pattern in patterns:
                if text_lower == pattern.lower():
                    return (cmd_name, None)
                if text_lower.startswith(pattern.lower() + ' '):
                    arg = text[len(pattern):].strip()
                    return (cmd_name, arg) if arg else (cmd_name, None)

        # If it looks like a command but didn't match, mark as unknown
        if text_lower.startswith('/'):
            return ('unknown', None)

        return None

    @staticmethod
    def handle_setname(switchboard, user_phone: str, arg: str) -> str:
        """Handle /setname command."""
        if not arg:
            return "Usage: /setname <your name>"

        # Normalize free-form input into a reasonable name string
        candidate = normalize_name_input(arg)

        if not candidate or len(candidate) < 2:
            return (
                "That doesn't look like a name. "
                "Try `/setname Firstname Lastname` or just `/setname Firstname`."
            )

        switchboard.store_user_profile(user_phone, {'name': candidate})
        return f"Name set to: {candidate}"

    @staticmethod
    def handle_setemail(switchboard, user_phone: str, arg: str) -> str:
        """Handle /setemail command."""
        if not arg:
            return "Usage: /setemail <your email>"
        if '@' not in arg:
            return "Invalid email format"
        switchboard.store_user_profile(user_phone, {'email': arg})
        return f"Email set to: {arg}"

    @staticmethod
    def handle_setphone(switchboard, user_phone: str, arg: str) -> str:
        """Handle /setphone command."""
        if not arg:
            return "Usage: /setphone <phone number>"
        switchboard.store_user_profile(user_phone, {'phone': arg})
        return f"Phone set to: {arg}"

    @staticmethod
    def handle_profile(switchboard, user_phone: str) -> str:
        """Handle /profile command."""
        profile = switchboard.get_user_profile(user_phone)
        if not profile or len(profile) <= 1:
            return "Profile empty. Use /setname, /setemail to add info."
        lines = ["Your Profile:"]
        for key, value in profile.items():
            lines.append(f"  {key.capitalize()}: {value}")
        return "\n".join(lines)

    @staticmethod
    def handle_help() -> str:
        """Handle /help command."""
        return """Commands:

/match - Find someone to chat with
/end - Disconnect from current chat
/reveal - Share contact info (both must agree)
/setname <name> - Set your name
/setemail <email> - Set your email
/profile - View your profile
/we - Shared profile of your conversation
/mood - Current tone from recent chat
/topics - Key topics from recent chat
/card - Mini profile of your partner
/setcity <city> - Set your city for local recs
/icebreaker - Start Two Truths and a Lie
/icebreakers - In-chat Two Truths and a Lie (truth, truth, lie)
/spotify - Link your Spotify account
/quiz - Play a lyrics guessing game with your partner
/help - Show this message"""

    @staticmethod
    def execute_command(switchboard, user_phone: str, command: str, arg: Optional[str]) -> Optional[str]:
        """Execute a command and return response."""
        if command == 'setname':
            return CommandHandler.handle_setname(switchboard, user_phone, arg or '')
        elif command == 'setemail':
            return CommandHandler.handle_setemail(switchboard, user_phone, arg or '')
        elif command == 'setphone':
            return CommandHandler.handle_setphone(switchboard, user_phone, arg or '')
        elif command == 'profile':
            return CommandHandler.handle_profile(switchboard, user_phone)
        elif command == 'help':
            return CommandHandler.handle_help()
        return None

