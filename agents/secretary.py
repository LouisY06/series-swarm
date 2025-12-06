"""Agent B: Calendar Invite Generator (Secretary)."""

import logging
from io import BytesIO
from typing import Dict, Any
from datetime import datetime, timedelta
from icalendar import Calendar, Event

logger = logging.getLogger(__name__)


def generate_calendar_invite(event_data: Dict[str, Any]) -> BytesIO:
    """
    Generate a calendar invite (.ics file) for an event.

    Args:
        event_data: Dictionary containing event information:
            - title: Event title
            - description: Optional event description
            - start_date: Event start date/time (ISO format or datetime)
            - end_date: Event end date/time (ISO format or datetime)
            - location: Event location
            - organizer: Optional organizer email
            - attendee: Optional attendee email

    Returns:
        BytesIO object containing the .ics calendar file
    """
    try:
        cal = Calendar()
        cal.add('prodid', '-//SeriesSwarm//Calendar Invite//EN')
        cal.add('version', '2.0')

        event = Event()
        event.add('summary', event_data.get('title', 'Event'))
        event.add('description', event_data.get('description', ''))

        # Parse start and end dates
        start_date = event_data.get('start_date')
        end_date = event_data.get('end_date')

        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        elif not isinstance(start_date, datetime):
            # Default to now + 1 hour if not provided
            start_date = datetime.now() + timedelta(hours=1)

        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        elif not isinstance(end_date, datetime):
            # Default to start + 1 hour if not provided
            end_date = start_date + timedelta(hours=1)

        event.add('dtstart', start_date)
        event.add('dtend', end_date)
        event.add('dtstamp', datetime.now())

        location = event_data.get('location', '')
        if location:
            event.add('location', location)

        organizer = event_data.get('organizer')
        if organizer:
            event.add('organizer', organizer)

        attendee = event_data.get('attendee')
        if attendee:
            event.add('attendee', attendee)

        # Add UID for calendar systems
        event.add('uid', f'series-swarm-{datetime.now().timestamp()}@series-swarm.local')

        cal.add_component(event)

        # Convert to BytesIO
        ics_bytes = cal.to_ical()
        calendar_io = BytesIO(ics_bytes)
        calendar_io.seek(0)

        logger.info(f"Generated calendar invite for event: {event_data.get('title', 'Event')}")
        return calendar_io

    except Exception as e:
        logger.error(f"Error generating calendar invite: {e}")
        raise

