"""Agent modules for SeriesSwarm."""

from .bouncer import generate_ticket
from .secretary import generate_calendar_invite
from .connector import generate_vcard
from .anchor import generate_audio_briefing

__all__ = [
    'generate_ticket',
    'generate_calendar_invite',
    'generate_vcard',
    'generate_audio_briefing',
]

