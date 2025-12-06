"""Test sending a response via Series API."""

import os
import json
from dotenv import load_dotenv
from core.series_api import SeriesAPI
from agents.secretary import generate_calendar_invite

load_dotenv()

# Test with a real message structure
test_message = {
    "api_version": "v2",
    "created_at": "2025-12-05T18:47:52-06:00",
    "data": {
        "attachments": [],
        "chat_handles": [
            {
                "display_name": "You",
                "identifier": "+16463230991",
                "is_me": True
            },
            {
                "display_name": "+1 (667) 352-2441",
                "identifier": "+16673522441",
                "is_me": False
            }
        ],
        "chat_id": "1696888",
        "from_phone": "+16673522441",
        "id": "53304269",
        "is_read": False,
        "reaction_id": None,
        "sent_at": "2025-12-05 18:47:51 -0600",
        "service": "iMessage",
        "text": "add a meeting tomorrow at 2pm"
    },
    "event_id": "489f766f-46bf-4e08-b5fe-06f16baa6dec",
    "event_type": "message.received"
}

def test_response():
    """Test sending a response."""
    api_key = os.getenv('SERIES_API_KEY')
    sender_number = os.getenv('SENDER_NUMBER')
    
    if not api_key:
        print("ERROR: SERIES_API_KEY not set")
        return
    
    if not sender_number:
        print("ERROR: SENDER_NUMBER not set")
        return
    
    print("Testing Series API response...")
    print(f"Sender: {sender_number}")
    print(f"Message from: {test_message['data']['from_phone']}")
    print(f"Chat ID: {test_message['data']['chat_id']}")
    print()
    
    # Initialize API client
    api = SeriesAPI(api_key)
    
    # Extract chat_id and recipient
    chat_id = api.get_chat_id_from_message(test_message)
    recipient = api.get_recipient_phone(test_message)
    
    print(f"Extracted chat_id: {chat_id} (type: {type(chat_id)})")
    print(f"Extracted recipient: {recipient}")
    print()
    
    # Generate a test calendar invite
    print("Generating test calendar invite...")
    calendar_data = {
        'text': test_message['data']['text'],
        'openai_api_key': os.getenv('OPENAI_API_KEY')
    }
    ics_file = generate_calendar_invite(calendar_data)
    print(f"Generated ICS file: {len(ics_file.getvalue())} bytes")
    print()
    
    # Try to send
    print("Attempting to send via Series API...")
    if chat_id:
        result = api.send_message_with_attachment(
            send_from=sender_number,
            chat_id=chat_id,
            text="Here's your calendar invite",
            attachment=ics_file,
            filename="event.ics",
            mime_type="text/calendar"
        )
    elif recipient:
        result = api.send_message_with_attachment(
            send_from=sender_number,
            phone_numbers=[recipient],
            text="Here's your calendar invite",
            attachment=ics_file,
            filename="event.ics",
            mime_type="text/calendar"
        )
    else:
        print("ERROR: Could not determine chat_id or recipient")
        return
    
    if result:
        print("✅ SUCCESS! Message sent via Series API")
        print(f"Response: {json.dumps(result, indent=2)}")
    else:
        print("❌ FAILED to send message")

if __name__ == '__main__':
    test_response()

