"""Command parsing and handling for switchboard system."""

import logging
import re
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)


class CommandHandler:
    """Handles user commands for the switchboard system."""

    # Command patterns
    COMMANDS = {
        'next': ['/next', 'next', 'skip', '/skip'],
        'reveal': ['/reveal', 'reveal', 'match', '/match'],
        'setname': ['/setname'],
        'setemail': ['/setemail'],
        'setphone': ['/setphone'],
        'profile': ['/profile', 'profile'],
        'help': ['/help', 'help', '/?', '?'],
    }

    @staticmethod
    def parse_command(text: str) -> Optional[Tuple[str, Optional[str]]]:
        """
        Parse a command from user text.

        Args:
            text: User's message text

        Returns:
            Tuple of (command_name, argument) or None if not a command
        """
        if not text:
            return None

        text = text.strip()

        # Check each command pattern
        for cmd_name, patterns in CommandHandler.COMMANDS.items():
            for pattern in patterns:
                # Exact match
                if text.lower() == pattern.lower():
                    return (cmd_name, None)

                # Command with argument (e.g., /setname John)
                if text.lower().startswith(pattern.lower() + ' '):
                    arg = text[len(pattern):].strip()
                    if arg:
                        return (cmd_name, arg)

        return None

    @staticmethod
    def is_command(text: str) -> bool:
        """Check if text is a command."""
        return CommandHandler.parse_command(text) is not None

    @staticmethod
    def handle_setname(switchboard, user_phone: str, arg: str) -> str:
        """Handle /setname command."""
        if not arg:
            return "Usage: /setname <your name>"

        switchboard.store_user_profile(user_phone, {'name': arg})
        return f"✅ Name set to: {arg}"

    @staticmethod
    def handle_setemail(switchboard, user_phone: str, arg: str) -> str:
        """Handle /setemail command."""
        if not arg:
            return "Usage: /setemail <your email>"

        # Basic email validation
        if '@' not in arg:
            return "❌ Invalid email format. Usage: /setemail your@email.com"

        switchboard.store_user_profile(user_phone, {'email': arg})
        return f"✅ Email set to: {arg}"

    @staticmethod
    def handle_setphone(switchboard, user_phone: str, arg: str) -> str:
        """Handle /setphone command."""
        if not arg:
            return "Usage: /setphone <phone number>"

        # Basic phone validation (E.164 format)
        if not arg.startswith('+'):
            return "❌ Phone must be in E.164 format (e.g., +1234567890)"

        switchboard.store_user_profile(user_phone, {'phone': arg})
        return f"✅ Phone set to: {arg}"

    @staticmethod
    def handle_profile(switchboard, user_phone: str) -> str:
        """Handle /profile command."""
        profile = switchboard.get_user_profile(user_phone)
        if not profile or len(profile) <= 1:  # Only has phone
            return "📋 Your profile is empty. Use /setname, /setemail to add info."
        
        lines = ["📋 Your Profile:"]
        for key, value in profile.items():
            if key == 'phone' and value == user_phone:
                continue  # Skip default phone
            lines.append(f"  {key.capitalize()}: {value}")
        
        return "\n".join(lines)

    @staticmethod
    def handle_help() -> str:
        """Handle /help command."""
        return """📖 Available Commands:

/next or /skip - Disconnect and find new match
/reveal or /match - Share contact info (both users must agree)
/setname <name> - Set your name
/setemail <email> - Set your email
/setphone <phone> - Set your phone (E.164 format)
/profile - View your profile
/help - Show this help message

💡 Tip: Set your profile before matching to share contact info easily!"""

    @staticmethod
    def execute_command(
        switchboard,
        user_phone: str,
        command: str,
        arg: Optional[str]
    ) -> Optional[str]:
        """
        Execute a command and return response message.

        Args:
            switchboard: Switchboard instance
            user_phone: User's phone number
            command: Command name
            arg: Command argument (if any)

        Returns:
            Response message or None if command doesn't need a response
        """
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
        else:
            # Commands that don't return messages (handled in main loop)
            return None

