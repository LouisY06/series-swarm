"""Agent B: Calendar Invite Generator (Secretary)."""

import logging
import os
import json
from io import BytesIO
from typing import Dict, Any
from datetime import datetime, timedelta
from icalendar import Calendar, Event
from openai import OpenAI

logger = logging.getLogger(__name__)


def _parse_event_from_text(text: str, openai_api_key: str) -> Dict[str, Any]:
    """
    Use OpenAI to parse natural language text and extract event details.

    Args:
        text: Natural language text describing an event
        openai_api_key: OpenAI API key

    Returns:
        Dictionary with extracted event information
    """
    client = OpenAI(api_key=openai_api_key)

    prompt = f"""Parse the following text message and extract event information. Return a JSON object with the following fields:
- title: Event title (required)
- description: Event description if mentioned
- start_date: Start date/time in ISO format (YYYY-MM-DDTHH:MM:SS) - infer from context if not explicit
- end_date: End date/time in ISO format (YYYY-MM-DDTHH:MM:SS) - default to 1 hour after start if not specified
- location: Event location/venue if mentioned
- organizer: Organizer email if mentioned
- attendee: Attendee email if mentioned

Text message: {text}

Return ONLY valid JSON, no other text."""

    try:
        response = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[
                {'role': 'system', 'content': 'You are a precise event parser. Extract event details from text and return only valid JSON.'},
                {'role': 'user', 'content': prompt}
            ],
            temperature=0.1,
            response_format={'type': 'json_object'}
        )

        result_text = response.choices[0].message.content.strip()
        parsed_data = json.loads(result_text)
        
        logger.info(f"Parsed event from text: {parsed_data.get('title', 'Unknown')}")
        return parsed_data

    except Exception as e:
        logger.error(f"Error parsing event from text: {e}")
        # Return minimal default structure if parsing fails
        return {
            'title': 'Event',
            'description': text,
            'start_date': (datetime.now() + timedelta(hours=1)).isoformat(),
            'end_date': (datetime.now() + timedelta(hours=2)).isoformat(),
        }


def generate_calendar_invite(event_data: Dict[str, Any]) -> BytesIO:
    """
    Generate a calendar invite (.ics file) for an event.

    Args:
        event_data: Dictionary containing event information:
            - text: Natural language text describing the event (will be parsed with AI)
            - OR structured fields:
            - title: Event title
            - description: Optional event description
            - start_date: Event start date/time (ISO format or datetime)
            - end_date: Event end date/time (ISO format or datetime)
            - location: Event location
            - organizer: Optional organizer email
            - attendee: Optional attendee email
            - openai_api_key: Optional OpenAI API key (falls back to OPENAI_API_KEY env var)

    Returns:
        BytesIO object containing the .ics calendar file
    """
    try:
        # Check if we have raw text that needs parsing
        text = event_data.get('text', '') or event_data.get('content', '')
        has_structured_data = any(key in event_data for key in ['title', 'start_date', 'date', 'time'])
        
        # If we have text but no structured data, parse it with AI
        if text and not has_structured_data:
            openai_api_key = event_data.get('openai_api_key') or os.getenv('OPENAI_API_KEY')
            if not openai_api_key:
                raise ValueError("OpenAI API key is required to parse text messages")
            
            logger.info("Parsing event from natural language text...")
            parsed_data = _parse_event_from_text(text, openai_api_key)
            # Merge parsed data with any additional fields from event_data
            event_data = {**parsed_data, **{k: v for k, v in event_data.items() if k not in ['text']}}
        
        # If we have both text and structured data, prefer structured but use text for description if missing
        elif text and has_structured_data and not event_data.get('description'):
            event_data['description'] = text
        cal = Calendar()
        cal.add('prodid', '-//SeriesSwarm//Calendar Invite//EN')
        cal.add('version', '2.0')

        event = Event()
        event.add('summary', event_data.get('title', 'Event'))
        event.add('description', event_data.get('description', event_data.get('text', '')))

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

