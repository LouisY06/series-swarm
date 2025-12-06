"""Command handler for SeriesSwarm."""

import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


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
        'icebreaker': ['/icebreaker'],
        'icebreakers': ['/icebreakers'],
        'bucketlist': ['/bucketlist'],
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

        return None

    @staticmethod
    def handle_setname(switchboard, user_phone: str, arg: str) -> str:
        """Handle /setname command."""
        if not arg:
            return "Usage: /setname <your name>"
        switchboard.store_user_profile(user_phone, {'name': arg})
        return f"Name set to: {arg}"

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
/icebreaker - Start Two Truths and a Lie
/icebreakers - In-chat Two Truths and a Lie (truth, truth, lie)
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

